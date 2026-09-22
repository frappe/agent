from __future__ import annotations

import datetime
import json
import unittest

from agent.ai_control.execution import FoundryAgentRuntime, FoundryExecutionError, managed_tool_name
from agent.ai_control.models import AI_CONTROL_TABLES
from agent.ai_control.store import (
    create_binding,
    decide_approval_request,
    get_approval_request,
    upsert_production_tool,
)
from agent.job import agent_database


class FakeFoundryClient:
    def __init__(self, tool_name: str):
        self.tool_name = tool_name
        self.submissions = []

    def invoke_agent(self, agent_name, input_items, *, conversation_id=None):
        return {
            "agent_name": agent_name,
            "conversation_id": conversation_id or "conv-test",
            "response_id": "resp-1",
            "output_text": "",
            "status": "completed",
            "output": [
                {
                    "type": "function_call",
                    "name": self.tool_name,
                    "call_id": "call-1",
                    "arguments": json.dumps(
                        {
                            "method": "GET",
                            "path_params": {},
                            "query": {},
                            "body": None,
                        }
                    ),
                }
            ],
        }

    def submit_tool_outputs(self, agent_name, conversation_id, outputs):
        self.submissions.append(outputs)
        return {
            "agent_name": agent_name,
            "conversation_id": conversation_id,
            "response_id": "resp-2",
            "output_text": "DONE",
            "status": "completed",
            "output": [],
        }


class FoundryExecutionRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        agent_database.init(":memory:")
        agent_database.connect()
        agent_database.create_tables(AI_CONTROL_TABLES, safe=True)

    @classmethod
    def tearDownClass(cls):
        agent_database.drop_tables(AI_CONTROL_TABLES, safe=True)
        agent_database.close()

    def setUp(self):
        for model in reversed(AI_CONTROL_TABLES):
            model.delete().execute()

    @staticmethod
    def _tool(*, risk_level: str, approval_policy: str):
        return upsert_production_tool(
            {
                "name": f"test-{risk_level}-{approval_policy}",
                "display_name": "Test Tool",
                "category": "test",
                "source": "Test",
                "route": "/test",
                "methods": ["GET"],
                "status": "Ready",
                "risk_level": risk_level,
                "approval_policy": approval_policy,
                "config": {"endpoint": "test_endpoint"},
            }
        )

    @staticmethod
    def _bind(agent_name: str, tool_id: int):
        return create_binding(
            {
                "source_type": "agent",
                "source_ref": agent_name,
                "target_type": "production_tool",
                "target_ref": str(tool_id),
                "status": "Active",
            }
        )

    def test_read_auto_tool_executes_and_returns_output_to_foundry(self):
        tool = self._tool(risk_level="read", approval_policy="auto")
        self._bind("agent-read", tool.id)
        client = FakeFoundryClient(managed_tool_name(tool.as_dict()))
        calls = []

        def execute(local_tool, arguments):
            calls.append((local_tool, arguments))
            return {"ok": True, "value": "observed"}

        runtime = FoundryAgentRuntime(client=client, tool_executor=execute)
        result = runtime.start("agent-read", "inspect")

        self.assertEqual(result["run"]["status"], "Success")
        self.assertEqual(result["run"]["result"]["output_text"], "DONE")
        self.assertEqual(len(result["tool_calls"]), 1)
        self.assertEqual(result["tool_calls"][0]["status"], "Success")
        self.assertEqual(result["approvals"], [])
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(client.submissions), 1)
        self.assertEqual(client.submissions[0][0]["type"], "function_call_output")

    def test_sensitive_tool_pauses_for_approval_then_resumes_same_run(self):
        tool = self._tool(risk_level="review", approval_policy="confirm")
        self._bind("agent-write", tool.id)
        client = FakeFoundryClient(managed_tool_name(tool.as_dict()))
        calls = []

        def execute(local_tool, arguments):
            calls.append((local_tool, arguments))
            return {"ok": True, "changed": True}

        runtime = FoundryAgentRuntime(client=client, tool_executor=execute)
        pending = runtime.start("agent-write", "change")

        self.assertEqual(pending["run"]["status"], "AwaitingApproval")
        self.assertEqual(len(pending["approvals"]), 1)
        self.assertEqual(pending["approvals"][0]["status"], "Pending")
        self.assertEqual(calls, [])
        self.assertEqual(client.submissions, [])

        approval_id = pending["approvals"][0]["id"]
        decide_approval_request(approval_id, "approved", decided_by="test")
        completed = runtime.resume(pending["run"]["id"])

        self.assertEqual(completed["run"]["status"], "Success")
        self.assertEqual(completed["run"]["result"]["output_text"], "DONE")
        self.assertEqual(completed["tool_calls"][0]["status"], "Success")
        self.assertEqual(completed["approvals"][0]["status"], "Approved")
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(client.submissions), 1)

    def test_expired_approval_fails_closed_without_executing_tool(self):
        tool = self._tool(risk_level="high", approval_policy="manual")
        self._bind("agent-expiry", tool.id)
        client = FakeFoundryClient(managed_tool_name(tool.as_dict()))
        calls = []

        def execute(local_tool, arguments):
            calls.append((local_tool, arguments))
            return {"ok": True}

        runtime = FoundryAgentRuntime(client=client, tool_executor=execute)
        pending = runtime.start("agent-expiry", "dangerous change")
        approval_id = pending["approvals"][0]["id"]
        approval = get_approval_request(approval_id)
        approval.expires_at = datetime.datetime.now() - datetime.timedelta(seconds=1)
        approval.save()

        with self.assertRaises(ValueError):
            decide_approval_request(approval_id, "approved", decided_by="test")

        approval = get_approval_request(approval_id)
        self.assertEqual(approval.status, "Expired")
        self.assertEqual(calls, [])
        self.assertEqual(client.submissions, [])

    def test_unbound_tool_cannot_be_executed_directly(self):
        tool = self._tool(risk_level="read", approval_policy="auto")
        runtime = FoundryAgentRuntime(
            client=FakeFoundryClient(managed_tool_name(tool.as_dict())),
            tool_executor=lambda local_tool, arguments: {"ok": True},
        )

        with self.assertRaises(FoundryExecutionError):
            runtime.execute_tool("agent-unbound", tool.id, {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
