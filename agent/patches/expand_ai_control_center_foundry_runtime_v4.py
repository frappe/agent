"""Create Foundry runtime tool-call and human-approval tables."""
from agent.ai_control.models import AI_CONTROL_TABLES
from agent.job import agent_database


def execute():
    agent_database.create_tables(AI_CONTROL_TABLES, safe=True)
