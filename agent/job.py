from __future__ import annotations

import datetime
import json
import logging
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

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 4 * 3600
MAX_TIMEOUT = 24 * 3600

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


def _requested_job_timeout():
    """`agent_job_timeout` from the request body, when the body is a JSON object.

    The body is not always an object: /proxy/wildcards POSTs a JSON *array*
    (press sends `get_wildcard_domains()` straight through), so `.get()` on the
    parsed body raises AttributeError there and 500s the endpoint before the job
    can be enqueued - which is what broke "Add Wildcard Hosts to Proxy", and with
    it wildcard TLS renewals, last time this landed. `get_json(silent=True)` also
    keeps a body that doesn't parse from aborting the request with a 400.
    """
    from flask import has_request_context, request

    if not (has_request_context() and request):
        return None
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        return payload.get("agent_job_timeout")
    return None


def _configured_job_timeout():
    """`job_timeout` from the agent's config.json.

    Handed to resolve_job_timeout as a callable so it is read only when the
    request didn't carry a timeout of its own. `Base.set_config` rewrites
    config.json by renaming it aside first, and `Base.config` reads it without
    taking the lock, so every read on this path is a chance to hit
    FileNotFoundError - and constructing Server() re-reads the file three more
    times in set_config_attributes().
    """
    from agent.server import Server

    return Server().config.get("job_timeout")


def resolve_job_timeout(*candidates) -> int:
    """Return the first candidate usable as an RQ job timeout, else DEFAULT_TIMEOUT.

    Candidates are tried in order of precedence and may be any JSON type: they
    come from a request body (press sends `agent_job_timeout`) and from
    config.json. A value that isn't a positive integer within MAX_TIMEOUT is
    skipped rather than raising, because this runs in the request handler that
    enqueues the job - a bad timeout must not turn every enqueue into a 500.

    A candidate may also be a zero-argument callable, evaluated only once every
    earlier candidate has been rejected, and never allowed to raise. Resolving a
    timeout is not worth failing an enqueue over: this runs for every agent job,
    so anything that can throw here takes down every job type at once.

    Skipping (instead of falling straight back to the default) means a garbled
    request param still leaves an operator's configured `job_timeout` in play.
    Zero is not usable: RQ would kill the work horse 60s in.
    """
    for candidate in candidates:
        if callable(candidate):
            try:
                candidate = candidate()
            except Exception:
                logger.warning("Could not resolve a job timeout from %r", candidate, exc_info=True)
                continue
        if not candidate:
            continue
        try:
            timeout = int(candidate)
        except (TypeError, ValueError):
            logger.warning("Ignoring non-numeric job timeout %r", candidate)
            continue
        if not 0 < timeout <= MAX_TIMEOUT:
            logger.warning("Ignoring job timeout %r, outside 1-%s seconds", candidate, MAX_TIMEOUT)
            continue
        return timeout
    return DEFAULT_TIMEOUT


def job(name: str, priority="default", timeout=None, on_success=None, on_failure=None):
    @wrapt.decorator
    def wrapper(wrapped, instance: Base, args, kwargs):
        from agent.base import AgentException

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
        agent_job_timeout = _requested_job_timeout()
        instance.job_record.enqueue(name, wrapped, args, kwargs, agent_job_id)
        final_timeout = resolve_job_timeout(agent_job_timeout, timeout, _configured_job_timeout)
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
