import datetime
import json
import traceback

import wrapt
from peewee import (
    CharField,
    DateTimeField,
    ForeignKeyField,
    Model,
    SqliteDatabase,
    TextField,
    TimeField,
    AutoField,
)
from redis import Redis
from rq import Queue, get_current_job

from agent.base import AgentException

agent_database = SqliteDatabase("jobs.sqlite3")


def connection():
    from agent.server import Server

    port = Server().config["redis_port"]
    return Redis(port=port)


def queue(name):
    return Queue(name, connection=connection())


@wrapt.decorator
def save(wrapped, instance, args, kwargs):
    wrapped(*args, **kwargs)
    instance.model.save()


class Action:
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
    @save
    def start(self, name, job):
        self.model = StepModel()
        self.model.name = name
        self.model.job = job
        self.model.start = datetime.datetime.now()
        self.model.status = "Running"


class Job(Action):
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


def step(name):
    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        instance.step_record.start(name, instance.job_record.model.id)
        try:
            result = wrapped(*args, **kwargs)
        except AgentException as e:
            instance.step_record.failure(e.data)
            raise e
        except Exception as e:
            instance.step_record.failure(
                {"traceback": "".join(traceback.format_exc())}
            )
            raise e
        else:
            instance.step_record.success(result)
        finally:
            instance.step_record = None
        return result

    return wrapper


def job(name, priority="default"):
    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        if get_current_job(connection=connection()):
            instance.job_record.start()
            try:
                result = wrapped(*args, **kwargs)
            except AgentException as e:
                instance.job_record.failure(e.data)
                raise e
            except Exception as e:
                instance.job_record.failure(
                    {"traceback": "".join(traceback.format_exc())}
                )
                raise e
            else:
                instance.job_record.success(result)
            return result
        else:
            agent_job_id = get_agent_job_id()
            instance.job_record.enqueue(
                name, wrapped, args, kwargs, agent_job_id
            )
            queue(priority).enqueue_call(
                wrapped,
                args=args,
                kwargs=kwargs,
                timeout=4 * 3600,
                result_ttl=24 * 3600,
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
    status = CharField(
        choices=[(1, "Running"), (2, "Success"), (3, "Failure")]
    )
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
