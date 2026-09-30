from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from peewee import SqliteDatabase
from redis.exceptions import ResponseError

from agent.job import Job, JobModel, StepModel, job

DISK_FULL = "MISCONF Errors writing to the AOF file: No space left on device"


class Archiver:
    def __init__(self):
        self.job_record = Job()

    @job("Archive Site", timeout=60)
    def archive_site(self):
        pass


@patch("agent.job.get_agent_job_id", return_value="42")
@patch("agent.job.get_current_job", return_value=None)
@patch("agent.job.connection")
class TestJobEnqueue(unittest.TestCase):
    def setUp(self):
        self.database = SqliteDatabase(":memory:")
        self.models = [JobModel, StepModel]
        self.database.bind(self.models)
        self.database.connect()
        self.database.create_tables(self.models)

    def tearDown(self):
        self.database.drop_tables(self.models)
        self.database.close()

    def test_job_is_marked_failure_with_the_redis_error_when_enqueue_fails(self, *_):
        with patch("agent.job.queue") as queue:
            queue.return_value.enqueue_call.side_effect = ResponseError(DISK_FULL)
            with self.assertRaises(ResponseError):
                Archiver().archive_site()

        record = JobModel.get(JobModel.agent_job_id == "42")
        self.assertEqual(record.status, "Failure")
        self.assertIn(DISK_FULL, json.loads(record.data)["traceback"])

    def test_job_stays_pending_when_enqueue_succeeds(self, *_):
        with patch("agent.job.queue"):
            Archiver().archive_site()

        self.assertEqual(JobModel.get(JobModel.agent_job_id == "42").status, "Pending")
