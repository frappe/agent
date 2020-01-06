import json
from peewee import (
    SqliteDatabase,
    Model,
    CharField,
    DateTimeField,
    TimeField,
    TextField,
    ForeignKeyField,
)

agent_database = SqliteDatabase("jobs.sqlite3")


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


def migrate():
    agent_database.create_tables([JobModel, StepModel])
