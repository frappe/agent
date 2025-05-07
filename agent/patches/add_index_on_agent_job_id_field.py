from __future__ import annotations


def execute():
    """add index on agent_job_id field of JobModel"""
    from agent.job import agent_database as database

    database.execute_sql("CREATE INDEX IF NOT EXISTS idx_jobmodel_agent_job_id ON jobmodel (agent_job_id)")
