from __future__ import annotations


def execute():
    """add a new fc_agent_job_id field to JobModel"""
    from peewee import CharField
    from playhouse.migrate import SqliteMigrator, migrate

    from agent.job import agent_database as database

    migrator = SqliteMigrator(database)
    try:
        migrate(migrator.add_column("JobModel", "agent_job_id", CharField(null=True)))
    except Exception as e:
        print(e)
