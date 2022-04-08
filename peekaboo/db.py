###############################################################################
#                                                                             #
# Peekaboo Extended Email Attachment Behavior Observation Owl                 #
#                                                                             #
# db.py                                                                       #
###############################################################################
#                                                                             #
# Copyright (C) 2016-2022 science + computing ag                              #
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

""" A class wrapping database operations needed by Peekaboo.  """

import asyncio
import datetime
import json
import logging
import time
import urllib
import uuid

import aiocouch
import aiohttp
import schema
import tenacity

from .ruleset import Result
from .sample import JobState
from .exceptions import PeekabooDatabaseError

logger = logging.getLogger(__name__)


CASE_STATUS_TO_JOB_STATE = {
    "Open": JobState.ACCEPTED,
    "Resolved": JobState.FINISHED,
    "Deleted": JobState.FINISHED,
    "Duplicate": JobState.FINISHED,
}


def retry_if_connection_refused(retry_state):
    exception = retry_state.outcome.exception()
    return (isinstance(exception, aiohttp.ClientConnectionError) and
            isinstance(exception.os_error, ConnectionRefusedError))


def log_retry(retry_state):
    """ Log a warning message on retries of requests. """
    exception = retry_state.outcome.exception()
    wait = retry_state.next_action.sleep
    logger.log(logging.WARNING, '%s. Retrying in %.2f seconds.',
               exception, wait)


class PeekabooDatabase:
    """ Peekaboo's database. """
    def __init__(self, url, db_prefix, user, password, instance_id=0,
                 stale_in_flight_threshold=15*60,
                 thehive_url="http://localhost:9000", api_token="", retries=5,
                 backoff=0.5):
        """
        Initialize the Peekaboo database handler.

        @param url: An RFC 1738 URL that points to the couchdb instance.
        @param db_prefix: Prefix for database names.
        @param user: Name of the user to connect as.
        @param password: Passwort to use for connection.
        @param instance_id: A positive, unique ID differentiating this Peekaboo
                            instance from any other instance using the same
                            database for concurrency coordination. Value of 0
                            means that we're alone and have no other instances
                            to worry about.
        @param stale_in_flight_threshold: Number of seconds after which a in
        flight marker is considered stale and deleted or ignored.
        @param thehive_url: Where to reach the TheHive REST API
        @type thehive_url: string
        @param api_token: API token to use for authentication
        @type api_token: string
        @param retries: Number of retries on API requests
        @type retries: int
        @param backoff: Backoff factor for urllib3
        @type backoff: float
        """
        # remember for diagnostics
        self.url = url
        self.db_prefix = db_prefix
        self.instance_id = instance_id
        self.stale_in_flight_threshold = stale_in_flight_threshold
        self.retries = 5
        self.connect_backoff_base = 2

        self.analyses_name = f'{db_prefix}-analyses'
        self.in_flight_name = f'{db_prefix}-in-flight-samples'

        # retry connection errors slowly so we don't flood an already congested
        # network and because the database might also just be restarting
        # FIXME: Limit to 'connection refused'
        self.connect_retrier = tenacity.AsyncRetrying(
                stop=tenacity.stop_after_attempt(self.retries),
                wait=tenacity.wait_exponential(
                    multiplier=self.connect_backoff_base),
                retry=retry_if_connection_refused,
                before_sleep=log_retry)

        # retry conflicts immediately (responsibility of the user to use
        # changed values to avoid conflict)
        self.conflict_retrier = tenacity.AsyncRetrying(
                stop=tenacity.stop_after_attempt(self.retries),
                retry=tenacity.retry_if_exception_type(
                    aiocouch.ConflictError),
                before_sleep=log_retry)

        logger.info('Creating CouchDB client for %s/%s',
                     url, db_prefix)
        self.client = aiocouch.CouchDB(url, user, password)
        # FIXME: Only Admins can create databases :(
        self.analyses_db = aiocouch.Database(self.client, self.analyses_name)
        self.in_flight_db = aiocouch.Database(self.client, self.in_flight_name)

        self.thehive_retrier = tenacity.AsyncRetrying(
                stop=tenacity.stop_after_attempt(retries),
                wait=tenacity.wait_exponential(multiplier=backoff),
                retry=tenacity.retry_if_exception_type(aiohttp.ClientError),
                before_sleep=log_retry)

        api_token = 'cct78qgRcdW2s4my50w0ZkRq7iAyLFZ8'
        headers = {'Authorization': f'Bearer {api_token}'}
        self.session = aiohttp.ClientSession(
            raise_for_status=True, headers=headers)
        self.thehive_url = thehive_url
        self.case_schema = schema.Schema({
                'caseId': int,
                'status': schema.Or('Open', 'Resolved', 'Deleted', 'Duplicate'),
                'startDate': int,
                'customFields': schema.Schema({
                        schema.Or(
                                #'peekaboo-analysis-state',
                                'peekaboo-analysis-sha256sum',
                                'peekaboo-analysis-result',
                                'peekaboo-analysis-file-extension'): {
                            'string': str,
                            'order': schema.Or(None, int),
                        },
                        'peekaboo-analysis-result-numeric': {
                            'integer': int,
                            'order': schema.Or(None, int),
                        },
                        schema.Optional('peekaboo-analysis-reason'): {
                            'string': str,
                            'order': schema.Or(None, int),
                        },
                        schema.Optional('peekaboo-analysis-report'): {
                            'string': str,
                            'order': schema.Or(None, int),
                        },
                        schema.Optional('peekaboo-analysis-cuckoo'): {
                            'string': str,
                            'order': schema.Or(None, int),
                        },
                    }, ignore_extra_keys=True),
            }, ignore_extra_keys=True)

        self.journal_schema = schema.Schema([{
                'number': int,
                'status': schema.Or('Open', 'Resolved', 'Deleted', 'Duplicate'),
                'startDate': int,
                'customFields': [
                    schema.Schema({
                            'name': schema.Or(
                                #'peekaboo-analysis-state',
                                'peekaboo-analysis-sha256sum',
                                'peekaboo-analysis-result',
                                'peekaboo-analysis-file-extension'),
                            'type': 'string',
                            'value': str,
                        }, ignore_extra_keys=True),
                    schema.Schema({
                            'name': 'peekaboo-analysis-result-numeric',
                            'type': 'integer',
                            'value': int,
                        }, ignore_extra_keys=True),
                    schema.Optional({
                            'name': schema.Or(
                                'peekaboo-analysis-reason',
                                'peekaboo-analysis-report',
                                'peekaboo-analysis-cuckoo'),
                            'type': 'string',
                            'value': str,
                        }, ignore_extra_keys=True),
                ]
            }], ignore_extra_keys=True)

    async def start(self):
        """ Start the database. """

    async def analysis_add(self, sample):
        """
        Add an analysis task to the analysis journal in the database.

        @param sample: The sample object for this analysis task.
        @returns: ID of the newly created analysis task (also updated
                  in the sample)
        """
        request_url = urllib.parse.urljoin(self.thehive_url, '/api/case')
        analysis = {
            'title': 'Peekaboo Analysis',
            'description': 'Peekaboo Analysis',
            'tags': ['peekaboo-analysis'],
            # case id and analysis time are auto-generated upon creation
            'customFields': {
                #'peekaboo-analysis-state': sample.state.name,
                'peekaboo-analysis-sha256sum': await sample.sha256sum,
                'peekaboo-analysis-file-extension': sample.file_extension,
                'peekaboo-analysis-result': sample.result.name,
                # purely for ordering reasons in find queries, particularly
                # worst result
                'peekaboo-analysis-result-numeric': sample.result.value
            },
        }

        try:
            async for attempt_connect in self.thehive_retrier:
                with attempt_connect:
                    async with self.session.post(
                            request_url, json=analysis) as response:
                        response = await response.json()
                        case = self.case_schema.validate(response)
                        case_id = case['caseId']
                        sample.update_id(case_id)
                        return case_id
        except (ValueError, schema.SchemaError) as error:
            raise PeekabooDatabaseError(
                'Invalid JSON in response when creating analysis case: '
                f'{error}') from error
        except tenacity.RetryError as error:
            # retries expired
            raise PeekabooDatabaseError(
                'Failed to add analysis case to the TheHive: '
                f'{error}') from error

    async def analysis_update(self, sample):
        """
        Update an analysis task in the analysis journal in the database.

        @param sample: The sample object for this analysis task.
        """
        request_url = urllib.parse.urljoin(
            self.thehive_url, f'/api/case/{sample.id}')

        update = {
            #'customFields.peekaboo-analysis-state.string':
            #    sample.state.name,
            #'customFields.peekaboo-analysis-sha256sum.string':
            #    await sample.sha256sum,
            #'customFields.peekaboo-analysis-file-extension.string':
            #    sample.file_extension,
            'customFields.peekaboo-analysis-result.string':
                sample.result.name,
            # purely for ordering reasons in find queries, particularly
            # worst result
            'customFields.peekaboo-analysis-result-numeric.integer':
                sample.result.value,
            'customFields.peekaboo-analysis-reason.string': sample.reason,
            'customFields.peekaboo-analysis-report.string':
                json.dumps(sample.peekaboo_report),
        }

        # map job state to case status
        if sample.state == JobState.FINISHED:
            update['status'] = 'Resolved'
            update['summary'] = 'Analysis concluded'
            update['impactStatus'] = 'NotApplicable'
            update['resolutionStatus'] = 'Indeterminate'

            if sample.result == Result.bad:
                update['resolutionStatus'] = 'TruePositive'
                update['impactStatus'] = 'NoImpact'

        if sample.cuckoo_report is not None:
            update['customFields.peekaboo-analysis-cuckoo.string'
                ] = json.dumps(sample.cuckoo_report.dump)
        #if sample.cortex_report is not None:
        #    update['cortex'] = sample.cortex_report
        #if sample.filetools_report is not None:
        #    update['filetools'] = sample.filetools_report
        #if sample.oletools_report is not None:
        #    update['oletools'] = sample.oletools_report
        #if sample.knowntools_report is not None:
        #    update['knowntools'] = sample.knowntools_report

        # attach the sample in case it is malware or analysis failed
        if sample.result >= Result.failed:
            attachment_url = urllib.parse.urljoin(
                self.thehive_url, f'/api/v0/case/{sample.id}/artifact')

            params = {
                "dataType": "file",
                "sighted": True,
                "message": "sample",
            }

            payload = aiohttp.FormData()
            payload.add_field('_json', json.dumps(params))
            # do not use client-supplied content type here to avoid
            # attampts of confusing CouchDB. Instead we simply save
            # some bytes here and metadata such as the content type
            # claimed by the client is part of the report.
            payload.add_field(
                'attachment', sample.content,
                content_type="application/octet-stream",
                filename=sample.filename)

            # make reusable across retries
            attachment = payload()

        try:
            async for attempt_connect in self.thehive_retrier:
                with attempt_connect:
                    async with self.session.patch(
                            request_url, json=update) as response:
                        await response.json()

            if sample.result >= Result.failed:
                async for attempt_connect in self.thehive_retrier:
                    with attempt_connect:
                        async with self.session.post(
                                attachment_url, data=attachment) as response:
                            await response.json()
        except (ValueError, schema.SchemaError) as error:
            raise PeekabooDatabaseError(
                'Invalid JSON in response when updating analysis case: '
                f'{error}') from error
        except tenacity.RetryError as error:
            raise PeekabooDatabaseError(
                f'{sample.id}: Failed to update analysis case in the '
                f'TheHive: {error}') from error

    async def analysis_journal_query(self, sample, query_update={}):
        """ Find entries in the analysis journal based on a base query
        referencing properties of a supplied sample that can be updated with
        additional criteria.

        @param query_update: dict with additional parameters to be merged into
                             the base query.

        @return: A dict containing the attributes of the requested sample as
                 stored in the journal and converted from database
                 representation back into our internal schema (i.e. Enums and
                 datetime objects).
        """
        request_url = urllib.parse.urljoin(
            self.thehive_url, '/api/v1/query?name=cases')

        query = [
            {
                '_name': 'listCase',
            },
            {
                '_name': 'filter',
                '_and': [
                    {
                        '_field': 'tags',
                        '_value': 'peekaboo-analysis',
                    }, {
                        '_not': {
                            '_field': 'number',
                            '_value': sample.id,
                        },
                    }, {
                        '_not': {
                            '_field': 'customFields.peekaboo-analysis-result',
                            '_value': Result.failed.name,
                        },
                    }, {
                        '_field': 'status',
                        '_value': 'Resolved',
                    }, {
                        '_field': 'customFields.peekaboo-analysis-sha256sum',
                        '_value': await sample.sha256sum,
                    }, {
                        '_field': 'customFields.peekaboo-analysis-file-extension',
                        '_value': sample.file_extension,
                    }
                ],
            }
        ]

        query.extend(query_update)

        try:
            async for attempt_connect in self.thehive_retrier:
                with attempt_connect:
                    async with self.session.post(
                            request_url, json={'query': query}) as response:
                        response = await response.json()
                        cases = self.journal_schema.validate(response)
        except (ValueError, schema.SchemaError) as error:
            raise PeekabooDatabaseError(
                'Invalid JSON in response when fetching analysis journal: '
                f'{error}') from error
        except tenacity.RetryError as error:
            raise PeekabooDatabaseError(
                'Failed to fetch analysis journal from the database: '
                f'{error}') from error

        if not cases:
            return None

        case = cases[0]
        analysis = dict(
            id=case['number'],
            analysis_time=time.gmtime(case['startDate']),
            state=CASE_STATUS_TO_JOB_STATE[case['status']])

        for field in case['customFields']:
            # this is sane because of schema validation done before
            prop = field['name'].replace('peekaboo-analysis-', '')
            value = field['value']

            if prop in ['report', 'cuckoo']:
                try:
                    analysis[prop] = json.loads(value)
                except ValueError as error:
                    raise PeekabooDatabaseError(
                        'Invalid report JSON in case when fetching journal: '
                        f'{error}') from error

                continue

            if prop == 'result':
                analysis[prop] = Result[value]
                continue

            analysis[prop] = value

        return analysis

    async def analysis_journal_get_first(self, sample):
        """
        Fetch the first analysis result stored in the database about a given
        sample object.

        @param sample: The sample object of which the information shall be
                       fetched from the database.
        @return: A dict containing the attributes of the requested sample as
                 stored in the journal.
        """
        return await self.analysis_journal_query(sample, [
            {
                '_name': 'sort',
                '_fields': [{
                    'startDate': 'asc',
                }],
            }, {
                '_name': 'page',
                'from': 0,
                'to': 1,
            }])

    async def analysis_journal_get_last(self, sample):
        """
        Fetch the worst analysis result stored in the database about a given
        sample object.

        @param sample: The sample object of which the information shall be
                       fetched from the database.
        @return: A dict containing id, result, reason and report of the
                 requested sample.
        """
        return await self.analysis_journal_query(sample, [{
                '_name': 'sort',
                '_fields': [{
                    'startDate': 'desc',
                }],
            }, {
                '_name': 'page',
                'from': 0,
                'to': 1,
            }])

    async def analysis_journal_get_worst(self, sample):
        """
        Fetch the worst analysis result stored in the database about a given
        sample object.

        @param sample: The sample object of which the information shall be
                       fetched from the database.
        @return: A dict containing id, result, reason and report of the
                 requested sample.
        """
        return await self.analysis_journal_query(sample, [{
                '_name': 'sort',
                '_fields': [{
                    'customFields.peekaboo-analysis-result-numeric': 'desc',
                }],
            }, {
                '_name': 'page',
                'from': 0,
                'to': 1,
            }])

    async def analysis_retrieve(self, job_id):
        """
        Fetch information stored in the database about a given sample object.

        @param job_id: ID of the analysis to retrieve
        @type job_id: uuid.UUID
        @return: reason and result for the given analysis task
        """
        request_url = urllib.parse.urljoin(
            self.thehive_url, f'/api/case/{job_id}')

        try:
            async for attempt_connect in self.thehive_retrier:
                with attempt_connect:
                    async with self.session.get(
                            request_url) as response:
                        response = await response.json()
                        case = self.case_schema.validate(response)
        except (ValueError, schema.SchemaError) as error:
            raise PeekabooDatabaseError(
                'Invalid JSON in response when retrieving analysisc: '
                f'{error}') from error
        except tenacity.RetryError as error:
            raise PeekabooDatabaseError(
                f'{sample.id}: Failed to retrieve analysis from the '
                f'database: {error}') from error

        analysis = dict(
            id=case['caseId'],
            state=CASE_STATUS_TO_JOB_STATE[case['status']],
            sha256sum=case['customFields'][
                'peekaboo-analysis-sha256sum']['string'],
            file_extension=case['customFields'][
                'peekaboo-analysis-file-extension']['string'],
            analysis_time=time.gmtime(case['startDate']),
            result=Result[case['customFields'][
                'peekaboo-analysis-result']['string']],
            reason=case['customFields'].get(
                'peekaboo-analysis-reason', {}).get('string'))

        try:
            report = case['customFields'].get(
                'peekaboo-analysis-report', {}).get('string')
            if report is not None:
                analysis['report'] = json.loads(report)

            cuckoo_report = case['customFields'].get(
                'peekaboo-analysis-cuckoo', {}).get('string')
            if cuckoo_report is not None:
                analysis['cuckoo_report'] = json.loads(cuckoo_report)
        except ValueError as error:
            raise PeekabooDatabaseError(
                'Invalid report JSON in case when retrieving analysis: '
                f'{error}') from error

        return analysis

    async def mark_sample_in_flight(self, sample, instance_id=None, start_time=None):
        """
        Mark a sample as in flight, i.e. being worked on by an instance.

        This is meant as a best-effort lock to improve efficiency by avoiding
        duplicated analyses for the same sample. Race conditions are likely and
        will in our case only lead to duplicated analyses.

        Using only the sha256sum as in-flight marker is an oversimplification
        that will actually hurt throughput if the same file content is
        presented multiple times with different accompanying meta-data such as
        file extension or content type that necessitate individual analyses.

        @param sample: The sample to mark as in flight.
        @param instance_id: (optionally) The ID of the instance that is
                            handling this sample. Default: Us.
        @param start_time: Override the time the marker was placed for
                           debugging purposes.
        """
        # an instance id of 0 denotes that we're alone and don't need to track
        # in-flight samples in the database
        if self.instance_id == 0:
            return True

        # use our own instance id if none is given
        if instance_id is None:
            instance_id = self.instance_id

        if start_time is None:
            start_time = datetime.datetime.now(datetime.timezone.utc)

        in_flight_marker_id = await sample.sha256sum
        in_flight_marker = dict(
            instance_id=instance_id,
            start_time=start_time.isoformat())

        try:
            async for attempt_connect in self.connect_retrier:
                with attempt_connect:
                    try:
                        marker = aiocouch.Document(
                            self.in_flight_db, in_flight_marker_id,
                            data=in_flight_marker)
                        await marker.save()
                        logger.debug('%s: Marked sample in flight', sample.id)
                        return True
                    except ConflictError:
                        logger.debug('%s: Sample is already in flight on another '
                                    'instance', sample.id)
                        return False
        except tenacity.RetryError as error:
            raise PeekabooDatabaseError(
                f'{sample.id}: Unable to mark sample as in flight: '
                f'{error}') from error

        return False

    async def clear_sample_in_flight(self, sample, instance_id=None):
        """
        Clear the mark that a sample is being processed by an instance.

        @param sample: The sample to clear from in-flight list.
        @param instance_id: (optionally) The ID of the instance that is
                            handling this sample. Default: Us.
        """
        # an instance id of 0 denotes that we're alone and don't need to track
        # in-flight samples in the database
        if self.instance_id == 0:
            return

        # use our own instance id if none is given
        if instance_id is None:
            instance_id = self.instance_id

        in_flight_marker_id = await sample.sha256sum
        cleared = 0
        try:
            async for attempt_connect in self.connect_retrier:
                with attempt_connect:
                    try:
                        marker = await self.in_flight_db[in_flight_marker_id]

                        marker_instance_id = marker['instance_id']
                        if marker_instance_id != instance_id:
                            raise PeekabooDatabaseError(
                                f'{sample.id} Unexpected inconsistency: '
                                f'Instance {marker_instance_id} '
                                'has meanwhile started processing our '
                                'in-flight sample.') from error

                        # race condition with another instance having deleted
                        # and re-created our marker - couchdb cannot delete by
                        # criteria

                        # deletion only starts a new document revision -
                        # accumulation?
                        await marker.delete()
                        logger.debug(
                            '%s: Removed sample in-flight marker', sample.id)
                    except aiocouch.NotFoundError:
                        raise PeekabooDatabaseError(
                            f'{sample.id}: Unexpected inconsistency: Sample '
                            'not recorded as in-flight upon clearing '
                            'flag.') from error
        except tenacity.RetryError as error:
            raise PeekabooDatabaseError(
                f'{sample.id}: Unable to clear in-flight status of sample: '
                f'{error}') from error

    async def clear_in_flight_samples(self, instance_id=None):
        """
        Clear all in-flight markers left over by previous runs or other
        instances by removing them from the lock table.

        @param instance_id: Clear our own (None), another instance's (positive
                            integer) or all instances' (negative integer) locks.
                            Since an instance_id of 0 disables in-flight sample
                            tracking, no instance will ever set a marker with
                            that ID so that specifying 0 here will amount to a
                            no-op or rather clean-up of invalid entries.
        """
        # an instance id of 0 denotes that we're alone and don't need to track
        # in-flight samples
        if self.instance_id == 0:
            return

        # use our own instance id if none is given
        if instance_id is None:
            instance_id = self.instance_id

        if instance_id < 0:
            # delete all locks
            query = dict()
            logger.debug('Clearing database of all in-flight samples.')
        else:
            # delete only the locks of a specific instance
            query = dict(instance_id=instance_id)
            logger.debug('Clearing database of all in-flight samples of '
                         'instance %d.', instance_id)

        try:
            async for attempt_connect in self.connect_retrier:
                with attempt_connect:
                    # race condition with other instances creating more markers
                    async for marker in self.in_flight_db.find(query):
                        try:
                            await marker.delete()
                        except aiocouch.NotFoundError:
                            # race condition with another instance having
                            # deleted its marker on its own
                            pass
        except tenacity.RetryError as error:
            raise PeekabooDatabaseError(
                'Unable to clear the database of in-flight samples: '
                f'{error}') from error

    async def clear_stale_in_flight_samples(self):
        """
        Clear all in-flight markers that are too old and therefore stale. This
        detects instances which are locked up, crashed or shut down.
        """
        # an instance id of 0 denotes that we're alone and don't need to track
        # in-flight samples in the database
        if self.instance_id == 0:
            return True

        logger.debug(
            'Clearing database of all stale in-flight samples '
            '(%d seconds)', self.stale_in_flight_threshold)

        # delete only the locks of a specific instance
        utcnow = datetime.datetime.now(datetime.timezone.utc)
        threshold = datetime.timedelta(seconds=self.stale_in_flight_threshold)
        query = dict(start_time={'$lte': (utcnow - threshold).isoformat()})

        try:
            async for attempt_connect in self.connect_retrier:
                with attempt_connect:
                    # race condition with other instances creating more markers
                    async for stale in self.in_flight_db.find(query):
                        logger.debug(
                            'Stale in-flight marker to clear: %s', stale)
                        try:
                            await stale.delete()
                        except aiocouch.NotFoundError:
                            # race condition with another instance having
                            # deleted its marker on its own
                            pass

        except tenacity.RetryError as error:
            raise PeekabooDatabaseError(
                'Unable to clear the database of stale in-flight samples: '
                f'{error}') from error

    def shut_down(self):
        """ Trigger shutdown of database components. """

    async def close_down(self):
        """ Finally close down all resources and wait for it to finish. """
        await self.client.close()
        await self.session.close()
