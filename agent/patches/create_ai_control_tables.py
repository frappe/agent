from __future__ import annotations


def execute():
    from agent.ai_control.models import AI_CONTROL_TABLES
    from agent.job import agent_database

    agent_database.create_tables(AI_CONTROL_TABLES, safe=True)
