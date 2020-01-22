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


logger = logging.getLogger(__name__)


class DuplicateHandlerCheckError(Exception):
    pass


class DuplicateHandler(object):
    """ A duplicate handler that manages samples which are already in analysis
    locally or on another instance in a cluster. """
    def __init__(self, db_con, cluster_check_interval=5):
        """ Initialise the duplicate handler .

        @param db_con: Connection to the database for cluster in-flight locking
        @param interval: How long to sleep inbetween locking retries.
        """
        self.db_con = db_con

        # keep a backlog of samples with hashes identical to samples currently
        # in analysis locally to avoid analysing multiple identical samples
        # simultaneously. Once one analysis has finished, we can submit the
        # others and the ruleset will notice that we already know the result.
        self.local = {}
        self.duplock = Lock()

        # keep a similar backlog of samples currently being processed by
        # other instances so we can regularly try to resubmit them and re-use
        # the other instances' cached results from the database
        self.cluster = {}
        self.cluster_check_interval = cluster_check_interval

        self.shutdown_requested = Event()
        self.shutdown_requested.clear()

        self.handler = None
        if interval:
            logger.debug("Starting cluster duplicate handler thread with "
                         "check interval %d.", interval)
            self.handler = Thread(target=self.handle_cluster_duplicates,
                                 name="ClusterDuplicateHandler")
            self.handler.start();
        else:
            logger.debug("Disabling cluster duplicate handler thread.")

    def is_duplicate(self, sample):
        """ Checks if a sample is a duplicate either locally or within the
        cluster.

        @param sample: The Sample object to check for duplication.
        @returns boolean: True if the sample was identified as a duplicate and
                          should not be processed further.
        @raises DuplicateHandlerCheckError: if duplicate check failed
        """
        sample_hash = sample.sha256sum
        sample_str = "%s" % sample
        duplicate = None
        cluster_duplicate = None
        resubmit = None
        # we have to lock this down because apart from callbacks from the job
        # queue workers we're also called from the ThreadingUnixStreamServer
        with self.duplock:
            # check if a sample with same hash is currently in flight
            duplicates = self.duplicates.get(sample_hash)
            if duplicates is not None:
                # we are regularly resubmitting samples, e.g. after we've
                # noticed that cuckoo is finished analysing them. This
                # obviously isn't a duplicate but continued processing of the
                # same sample.
                if duplicates["master"] == sample:
                    resubmit = sample_str
                else:
                    # record the to-be-submitted sample as duplicate and do nothing
                    duplicate = sample_str
                    duplicates["duplicates"].append(sample)
            else:
                # are we the first of potentially multiple instances working on
                # this sample?
                try:
                    locked = self.db_con.mark_sample_in_flight(sample)
                except PeekabooDatabaseError as dberr:
                    logger.error(dberr)
                    raise DuplicateHandlerCheckError()

                if locked:
                    # initialise a per-duplicate backlog for this sample which
                    # also serves as in-flight marker and submit to queue
                    self.duplicates[sample_hash] = {
                            "master": sample,
                            "duplicates": [] }
                else:
                    # another instance is working on this
                    cluster_duplicates = self.cluster_duplicates.get(sample_hash)
                    if cluster_duplicates is not None:
                        cluster_duplicates.append(sample)
                    else:
                        self.cluster_duplicates[sample_hash] = [sample]

                    cluster_duplicate = sample_str

        if duplicate:
            logger.debug("Sample is duplicate and waiting for running "
                         "analysis to finish: %s" % duplicate)
            return True
        elif cluster_duplicate:
            logger.debug("Sample is concurrently being processed by another "
                         "instance and held: %s" % cluster_duplicate)
            return True
        elif resubmit:
            logger.debug("Sample is resubmit of deferred analysis: %s" %
                         resubmit)
            return False

        logger.debug("Sample is new: %s", sample_str)
        return False

    def submit_cluster_duplicates(self):
        if not self.cluster_duplicates.keys():
            return True

        submitted_cluster_duplicates = []

        with self.duplock:
            # try to submit *all* samples which have been marked as being
            # processed by another instance concurrently
            for sample_hash, sample_duplicates in self.cluster_duplicates.items():
                # try to mark as in-flight
                try:
                    locked = self.db_con.mark_sample_in_flight(
                        sample_duplicates[0])
                except PeekabooDatabaseError as dberr:
                    logger.error(dberr)
                    return False

                if locked:
                    sample_str = "%s" % sample_duplicates[0]
                    if self.duplicates.get(sample_hash) is not None:
                        logger.error("Possible backlog corruption for sample "
                                "%s! Please file a bug report. Trying to "
                                "continue..." % sample_str)
                        continue

                    # submit one of the held-back samples as a new master
                    # analysis in case the analysis on the other instance
                    # failed and we have no result in the database yet. If all
                    # is well, this master should finish analysis very quickly
                    # using the stored result, causing all the duplicates to be
                    # submitted and finish quickly as well.
                    sample = sample_duplicates.pop()
                    self.duplicates[sample_hash] = {
                            'master': sample,
                            'duplicates': sample_duplicates }
                    submitted_cluster_duplicates.append(sample_str)
                    self.jobs.put(sample, True, self.queue_timeout)
                    del self.cluster_duplicates[sample_hash]

        if len(submitted_cluster_duplicates) > 0:
            logger.debug("Submitted cluster duplicates (and potentially "
                    "their duplicates) from backlog: %s" %
                    submitted_cluster_duplicates)

        return True

    def clear_stale_in_flight_samples(self):
        try:
            cleared = self.db_con.clear_stale_in_flight_samples()
        except PeekabooDatabaseError as dberr:
            logger.error(dberr)
            cleared = False

        return cleared

    def submit_duplicates(self, sample_hash):
        """ Check if any samples have been held from processing as duplicates
        and submit them now. Clear the original sample whose duplicates have
        been submitted from the in-flight list.

        @param sample_hash: Hash of sample to check for duplicates
        """
        with self.duplock:
            # remove from duplicate list atomically, still protect with lock to
            # prevent race with db operation below
            dupreg = self.duplicates.pop(sample_hash)

            # duplicates which have been submitted from the backlog still
            # report done but do not get registered as potentially having
            # duplicates because we expect the ruleset to identify them as
            # already known and process them quickly now that the first
            # instance has gone through full analysis. Therefore we can ignore
            # them here.
            if dupreg is None:
                return None

            sample = dupreg['master']
            try:
                self.db_con.clear_sample_in_flight(sample)
            except PeekabooDatabaseError as dberr:
                logger.error(dberr)

        logger.debug("Cleared sample %s from in-flight list" % sample)

        duplicates = dupreg['duplicates']
        if duplicates:
            logger.debug("Duplicates ready for submit: %s" % duplicates)

    def handle_cluster_duplicates(self):
        logger.debug("Duplicate handler started.")

        while not self.shutdown_requested.wait(self.interval):
            logger.debug("Checking for samples in processing by other "
                         "instances to submit")
            # TODO: Error handling: How do we cause Peekaboo to exit with an
            # error from here? For now just keep trying and hope (database)
            # failure is transient.
            self.clear_stale_in_flight_samples()
            self.submit_cluster_duplicates()

        logger.debug("Cluster duplicate handler shut down.")

    def shut_down(self):
        self.shutdown_requested.set()
        self.handler.join()
