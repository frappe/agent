from __future__ import annotations


def execute():
    """add index on id field of JobModel"""
    from agent.job import agent_database as database

    database.execute_sql("CREATE INDEX IF NOT EXISTS idx_jobmodel_id ON jobmodel (id)")
