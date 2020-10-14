import datetime

from peewee import (
    CharField,
    DateTimeField,
    IntegerField,
    Model,
    SqliteDatabase,
    TextField
)

usage_database = SqliteDatabase("usage.sqlite3")

class UsageModel(Model):
    site = TextField()
    timestamp = DateTimeField(default=datetime.datetime.now)
    time_zone = TextField(null=True)
    database = IntegerField(null=True, default=0)
    public = IntegerField(null=True, default=0)
    private = IntegerField(null=True, default=0)
    backups = IntegerField(null=True, default=0)

    class Meta:
        database = usage_database


if __name__ == "__main__":
    from agent.server import Server

    databases = {}
    server = Server()

    for bench in server.benches.values():
        for site in bench.sites.values():
            UsageModel.insert(**{
                "site": site.name,
                "time_zone": site.timezone,
                **site.get_usage()
            }).execute()
