from __future__ import annotations

import datetime
import json
import os
import traceback
from typing import TYPE_CHECKING

import wrapt
from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    ForeignKeyField,
    Model,
    SqliteDatabase,
    TextField,
    TimeField,
)
from redis import Redis
from rq import Queue, get_current_job
from rq.command import send_stop_job_command
from rq.job import Job as RQJob

from agent.callbacks import callback

if TYPE_CHECKING:
    from agent.base import Base


if os.environ.get("SENTRY_DSN"):
    try:
        import sentry_sdk

        sentry_sdk.init(dsn=os.environ["SENTRY_DSN"])
    except ImportError:
        pass

DEFAULT_TIMEOUT = 4 * 3600
agent_database = SqliteDatabase(
    "jobs.sqlite3",
    timeout=15,
    pragmas={
        "journal_mode": "wal",
        "synchronous": "normal",
        "mmap_size": 2**32 - 1,
        "page_size": 8192,
    },
)


def connection():
    from agent.server import Server

    port = Server().config["redis_port"]
    return Redis(port=port)


def queue(name):
    return Queue(name, connection=connection())


@wrapt.decorator
def save(wrapped, instance: Action, args, kwargs):
    wrapped(*args, **kwargs)
    instance.model.save()


class Action:
    if TYPE_CHECKING:
        model: Model | None

    def success(self, data):
        self.model.status = "Success"
        self.model.data = json.dumps(data, default=str)
        self.end()

    def failure(self, data):
        self.model.data = json.dumps(data, default=str)
        self.model.status = "Failure"
        self.end()

    @save
    def end(self):
        self.model.end = datetime.datetime.now()
        self.model.duration = self.model.end - self.model.start


class Step(Action):
    if TYPE_CHECKING:
        model: StepModel | None

    @save
    def start(self, name, job):
        self.model = StepModel()
        self.model.name = name
        self.model.job = job
        self.model.start = datetime.datetime.now()
        self.model.status = "Running"


class Job(Action):
    if TYPE_CHECKING:
        model: JobModel | None

    def __init__(self, id=None):
        super().__init__()
        if id:
            self.model = JobModel.get(JobModel.id == id)
            self.redis = connection()
            self.job = RQJob.fetch(str(self.model.id), connection=self.redis)

    @save
    def start(self):
        self.model.start = datetime.datetime.now()
        self.model.status = "Running"

    @save
    def enqueue(self, name, function, args, kwargs, agent_job_id=None):
        self.model = JobModel()
        self.model.name = name
        self.model.status = "Pending"
        self.model.enqueue = datetime.datetime.now()
        self.model.data = json.dumps(
            {
                "function": function.__func__.__name__,
                "args": args,
                "kwargs": kwargs,
            },
            default=str,
            sort_keys=True,
            indent=4,
        )
        self.model.agent_job_id = agent_job_id

    @save
    def cancel(self):
        self.job.cancel()
        self.model.status = "Failure"

    @save
    def stop(self):
        send_stop_job_command(self.redis, self.job.get_id())
        self.job.refresh()
        self.model.data = json.dumps(self.job.to_dict(), default=str)
        self.model.status = "Failure"
        self.end()

    def cancel_or_stop(self):
        if self.job.is_started:
            self.stop()
        else:
            self.cancel()


def step(name):
    @wrapt.decorator
    def wrapper(wrapped, instance: Base, args, kwargs):
        from agent.base import AgentException

        instance.step_record.start(name, instance.job_record.model.id)
        try:
            result = wrapped(*args, **kwargs)
        except AgentException as e:
            instance.step_record.failure(e.data)
            raise e
        except Exception as e:
            instance.step_record.failure({"traceback": "".join(traceback.format_exc())})
            raise e
        else:
            instance.step_record.success(result)
        finally:
            instance.step_record = None
        return result

    return wrapper


def job(name: str, priority="default", timeout=None, on_success=None, on_failure=None):
    @wrapt.decorator
    def wrapper(wrapped, instance: Base, args, kwargs):
        from flask import has_request_context, request

        from agent.base import AgentException
        from agent.server import Server

        if get_current_job(connection=connection()):
            instance.job_record.start()
            try:
                result = wrapped(*args, **kwargs)
            except AgentException as e:
                instance.job_record.failure(e.data)
                raise e
            except Exception as e:
                instance.job_record.failure({"traceback": "".join(traceback.format_exc())})
                raise e
            else:
                instance.job_record.success(result)
            return result
        agent_job_id = get_agent_job_id()
        agent_job_timeout = None
        if has_request_context() and request and request.is_json:
            agent_job_timeout = request.json.get("agent_job_timeout", None)
        instance.job_record.enqueue(name, wrapped, args, kwargs, agent_job_id)
        final_timeout = (
            agent_job_timeout or timeout or Server().config.get("job_timeout", None) or DEFAULT_TIMEOUT
        )
        if not 0 <= final_timeout <= 24 * 3600:
            final_timeout = DEFAULT_TIMEOUT
        queue(priority).enqueue_call(
            wrapped,
            args=args,
            kwargs=kwargs,
            timeout=final_timeout,
            result_ttl=24 * 3600,
            job_id=str(instance.job_record.model.id),
            on_success=on_success or callback,
            on_failure=on_failure or callback,
        )
        return instance.job_record.model.id

    return wrapper


def get_agent_job_id():
    from flask import request

    return request.headers.get("X-Agent-Job-Id")


class JobModel(Model):
    name = CharField()
    status = CharField(
        choices=[
            (0, "Pending"),
            (1, "Running"),
            (2, "Success"),
            (3, "Failure"),
        ]
    )
    agent_job_id = CharField(null=True)
    data = TextField(null=True, default="{}")

    enqueue = DateTimeField(default=datetime.datetime.now)

    start = DateTimeField(null=True)
    end = DateTimeField(null=True)
    duration = TimeField(null=True)

    class Meta:
        database = agent_database


class StepModel(Model):
    name = CharField()
    job = ForeignKeyField(JobModel, backref="steps", lazy_load=False)
    status = CharField(choices=[(1, "Running"), (2, "Success"), (3, "Failure")])
    data = TextField(null=True, default="{}")

    start = DateTimeField()
    end = DateTimeField(null=True)
    duration = TimeField(null=True)

    class Meta:
        database = agent_database


class PatchLogModel(Model):
    name = AutoField()
    patch = TextField()

    class Meta:
        database = agent_database
