###############################################################################
#                                                                             #
# Peekaboo Extended Email Attachment Behavior Observation Owl                 #
#                                                                             #
# queuing.py                                                                  #
###############################################################################
#                                                                             #
# Copyright (C) 2016-2019  science + computing ag                             #
#                                                                             #
# This program is free software: you can redistribute it and/or modify        #
# it under the terms of the GNU General Public License as published by        #
# the Free Software Foundation, either version 3 of the License, or (at       #
# your option) any later version.                                             #
#                                                                             #
# This program is distributed in the hope that it will be useful, but         #
# WITHOUT ANY WARRANTY; without even the implied warranty of                  #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU           #
# General Public License for more details.                                    #
#                                                                             #
# You should have received a copy of the GNU General Public License           #
# along with this program.  If not, see <http://www.gnu.org/licenses/>.       #
#                                                                             #
###############################################################################


import logging
from threading import Thread, Event, Lock
from queue import Queue, Empty
from time import sleep
from peekaboo.duplicates import DuplicateHandler, \
    DuplicateHandlerCheckException
from peekaboo.ruleset import Result, RuleResult
from peekaboo.ruleset.engine import RulesetEngine
from peekaboo.exceptions import PeekabooAnalysisDeferred, \
    PeekabooDatabaseError


logger = logging.getLogger(__name__)


class JobQueue:
    """ Peekaboo's queuing system. """
    def __init__(self, ruleset_config, db_con, duplicate_handler,
                 worker_count=4, queue_timeout=300, shutdown_timeout=60):
        """ Initialise job queue by creating n Peekaboo worker threads to
        process samples.

        @param db_con: Database connection object for cluster instance
                       coordination, i.e. saving sample info.
        @param worker_count: The amount of worker threads to create. Defaults to 4.
        @param queue_timeout: How long to block before considering queueing failed.
        """
        self.db_con = db_con
        self.duplicate_handler = duplicate_handler
        self.jobs = Queue()
        self.workers = []
        self.worker_count = worker_count
        self.queue_timeout = queue_timeout
        self.shutdown_timeout = shutdown_timeout

        for i in range(0, self.worker_count):
            logger.debug("Create Worker %d" % i)
            w = Worker(i, self, ruleset_config, db_con)
            self.workers.append(w)
            w.start()

        logger.info('Created %d Workers.' % self.worker_count)

    def submit(self, sample, submitter):
        """
        Adds a Sample object to the job queue.
        If the queue is full, we block for 300 seconds and then throw an exception.

        @param sample: The Sample object to add to the queue.
        @param submitter: The name of the class / module that wants to submit the sample.
        @raises Full: if the queue is full.
        """
        try:
            is_duplicate = self.duplicate_handler.is_duplicate(sample):
        except DuplicateHandlerCheckError:
            # is_duplicate logged an error
            return False

        if is_duplicate:
            logger.debug("Sample from %s not submitted to job queue: %s",
                         submitter, sample)
            return True

        self.jobs.put(sample, True, self.queue_timeout)
        logger.debug("Sample from %s submitted to job queue: %s",
                     submitter, sample)
        return True

    def done(self, sample):
        """ Perform cleanup actions after sample processing is done:
        1. Submit held duplicates and
        2. notify request handler thread that sample processing is done.

        @param sample: The Sample object to post-process. """
        # submit all samples which have accumulated in the backlog, do not
        # check for duplication because we want them all to bubble through in
        # parallel now that hopefully the analysis result of their predecessor
        # is cached
        for sample in self.duplicate_handler.get_duplicates(sample_hash):
            logger.debug("Submitting duplicate from backlog: %s" % sample)
            self.jobs.put(s, True, self.queue_timeout)

        # now that this sample is really done and cleared from the queue, tell
        # its connection handler about it
        sample.mark_done()

    def dequeue(self):
        """ Remove a sample from the queue. Used by the workers to get their
        work. Blocks indefinitely until some work is available. If we want to
        wake the workers for some other reason, we send them a None item as
        ping. """
        return self.jobs.get(True)

    def shut_down(self, timeout = None):
        if not timeout:
            timeout = self.shutdown_timeout

        logger.info("Shutting down. Giving workers %d seconds to stop" % timeout)

        # tell all workers to shut down
        for worker in self.workers:
            worker.shut_down()

        # put a ping for each worker on the queue. Since they already all know
        # that they're supposed to shut down, each of them will only remove
        # one item from the queue and then exit, leaving the others for their
        # colleagues. For this reason this loop can't be folded into the above!
        for worker in self.workers:
            self.jobs.put(None)

        # wait for workers to end
        interval = 1
        for attempt in range(1, timeout // interval + 1):
            still_running = []
            for worker in self.workers:
                if worker.running:
                    still_running.append(worker)

            self.workers = still_running
            if len(self.workers) == 0:
                break

            sleep(interval)
            logger.debug('%d: %d workers still running', attempt,
                         len(self.workers))

        if len(self.workers) > 0:
            logger.error("Some workers refused to stop.")

class Worker(Thread):
    """ A Worker thread to process a sample. """
    def __init__(self, wid, job_queue, ruleset_config, db_con):
        # whether we should run
        self.shutdown_requested = Event()
        self.shutdown_requested.clear()
        # whether we are actually running
        self.running_flag = Event()
        self.running_flag.clear()
        self.worker_id = wid
        self.job_queue = job_queue
        self.ruleset_config = ruleset_config
        self.db_con = db_con
        Thread.__init__(self, name="Worker-%d" % wid)

    def run(self):
        self.running_flag.set()
        while not self.shutdown_requested.is_set():
            logger.debug('Worker %d: Ready', self.worker_id)

            try:
                # wait blocking for next job (thread safe) with timeout
                sample = self.job_queue.dequeue()
            except Empty:
                continue

            if sample is None:
                # we just got pinged
                continue

            logger.info('Worker %d: Processing sample %s',
                        self.worker_id, sample)

            # The following used to be one big try/except block catching any
            # exception. This got complicated because in the case of
            # CuckooReportPending we use exceptions for control flow as well
            # (which might be questionable in itself). Instead of catching,
            # logging and ignoring errors here if workers start to die again
            # because of uncaught exceptions we should improve error handling
            # in the subroutines causing it.

            if not sample.init():
                logger.error('Sample initialization failed')
                sample.add_rule_result(
                    RuleResult(
                        "Worker", result=Result.failed,
                        reason=_("Sample initialization failed"),
                        further_analysis=False))
                self.job_queue.done(sample.sha256sum)
                continue

            engine = RulesetEngine(self.ruleset_config, self.db_con)
            try:
                engine.run(sample)
            except PeekabooAnalysisDeferred:
                logger.debug("Report for sample %s still pending", sample)
                continue

            if sample.result >= Result.failed:
                sample.dump_processing_info()

            if sample.result != Result.failed:
                logger.debug('Saving results to database')
                try:
                    self.db_con.analysis_save(sample)
                except PeekabooDatabaseError as dberr:
                    logger.error('Failed to save analysis result to '
                                 'database: %s', dberr)
                    # no showstopper, we can limp on without caching in DB
            else:
                logger.debug('Not saving results of failed analysis')

            sample.cleanup()
            self.job_queue.done(sample)

        logger.info('Worker %d: Stopped' % self.worker_id)
        self.running_flag.clear()

    def shut_down(self):
        self.shutdown_requested.set()

    @property
    def running(self):
        return self.running_flag.is_set()
