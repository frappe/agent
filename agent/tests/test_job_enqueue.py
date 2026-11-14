from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from flask import Flask
from peewee import SqliteDatabase
from redis.exceptions import ResponseError

from agent.job import Job, JobModel, StepModel, job

MODELS = (JobModel, StepModel)


class Worker:
    def __init__(self):
        self.job_record = Job()

    @job("Test Job")
    def run(self):
        return "done"


class TestJobEnqueue(unittest.TestCase):
    """The job decorator saves a job record, then puts the job on an RQ queue.

    Press finds a job by the agent_job_id of its record and then treats the job
    as delivered. The record must exist only when the job is on the queue.
    """

    def setUp(self):
        self.db = SqliteDatabase(":memory:")
        self.db.bind(MODELS, bind_refs=False, bind_backrefs=False)
        self.db.connect()
        self.db.create_tables(MODELS)

        self.app = Flask(__name__)
        self.request = self.app.test_request_context(headers={"X-Agent-Job-Id": "press-job-1"})
        self.request.push()

        self.queue = MagicMock()
        for target, value in (
            ("agent.job.connection", MagicMock()),
            ("agent.job.get_current_job", MagicMock(return_value=None)),
            ("agent.job.queue", MagicMock(return_value=self.queue)),
            ("agent.job.RQJob.exists", MagicMock(return_value=False)),
        ):
            patcher = patch(target, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def tearDown(self):
        self.request.pop()
        self.db.drop_tables(MODELS)
        self.db.close()

    def records(self):
        return list(JobModel.select().where(JobModel.agent_job_id == "press-job-1"))

    def test_keeps_the_record_of_an_enqueued_job(self):
        job_id = Worker().run()

        records = self.records()
        self.assertEqual([r.id for r in records], [job_id])
        self.assertEqual(records[0].status, "Pending")
        self.assertEqual(self.queue.enqueue_call.call_args.kwargs["job_id"], str(job_id))

    def test_removes_the_record_when_the_enqueue_fails(self):
        self.queue.enqueue_call.side_effect = ResponseError(
            "MISCONF Errors writing to the AOF file: No space left on device"
        )

        with self.assertRaises(ResponseError):
            Worker().run()

        self.assertEqual(self.records(), [])

    def test_press_can_deliver_the_job_again_after_a_failed_enqueue(self):
        self.queue.enqueue_call.side_effect = [ResponseError("MISCONF"), None]

        with self.assertRaises(ResponseError):
            Worker().run()
        job_id = Worker().run()

        self.assertEqual([r.id for r in self.records()], [job_id])

    def test_keeps_the_record_when_redis_queued_the_job_but_the_reply_was_lost(self):
        self.queue.enqueue_call.side_effect = ConnectionError("reply lost")

        with patch("agent.job.RQJob.exists", return_value=True):
            job_id = Worker().run()

        self.assertEqual([r.id for r in self.records()], [job_id])

    def test_raises_the_enqueue_error_when_the_record_cannot_be_removed(self):
        self.queue.enqueue_call.side_effect = ResponseError("MISCONF")
        delete = patch.object(JobModel, "delete_instance", side_effect=OSError("disk full"))

        with delete, self.assertLogs("agent.job", "ERROR"), self.assertRaises(ResponseError):
            Worker().run()


if __name__ == "__main__":
    unittest.main()
