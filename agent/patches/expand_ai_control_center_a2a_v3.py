"""Create the local A2A control-plane tables introduced in AI Control Center v3."""

from agent.ai_control.models import AI_CONTROL_TABLES
from agent.job import agent_database


def execute():
    agent_database.create_tables(AI_CONTROL_TABLES, safe=True)
