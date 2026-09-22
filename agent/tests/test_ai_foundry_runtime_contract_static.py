from __future__ import annotations

import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AI = ROOT / "agent" / "ai_control"


class FoundryRuntimeContractTests(unittest.TestCase):
    def test_modified_ai_modules_parse(self):
        for name in (
            "foundry.py",
            "execution.py",
            "protocols.py",
            "models.py",
            "store.py",
            "routes.py",
            "mcp_server.py",
        ):
            ast.parse((AI / name).read_text(encoding="utf-8"), filename=name)

    def test_foundry_uses_persisted_agent_responses_and_function_tools(self):
        source = (AI / "foundry.py").read_text(encoding="utf-8")
        self.assertIn("get_openai_client(agent_name=agent_name)", source)
        self.assertIn("openai.responses.create(", source)
        self.assertIn("FunctionTool(", source)
        self.assertIn("project.agents.create_version(", source)
        self.assertIn("def submit_tool_outputs(", source)
        self.assertIn("def sync_agent_function_tools(", source)

    def test_tool_calls_and_approvals_are_persistent(self):
        source = (AI / "models.py").read_text(encoding="utf-8")
        table_list = source[source.index("AI_CONTROL_TABLES ="):]
        for model in ("AIToolCallModel", "AIApprovalRequestModel"):
            self.assertIn(f"class {model}", source)
            self.assertIn(model, table_list)

    def test_runtime_has_human_gate_and_audit(self):
        source = (AI / "execution.py").read_text(encoding="utf-8")
        for symbol in (
            "tool_requires_approval",
            "create_tool_call",
            "create_approval_request",
            "AwaitingApproval",
            "submit_tool_outputs",
            "def resume(",
        ):
            self.assertIn(symbol, source)
        self.assertIn('risk == "read" and policy == "auto"', source)
        self.assertIn("FOUNDRY_APPROVAL_TTL_SECONDS", source)

    def test_routes_expose_complete_control_surface(self):
        source = (AI / "routes.py").read_text(encoding="utf-8")
        for route in (
            "/api/foundry/agents/<string:agent_name>/chat",
            "/api/foundry/agents/<string:agent_name>/invoke",
            "/api/foundry/agents/<string:agent_name>/tools",
            "/api/foundry/agents/<string:agent_name>/tools/sync",
            "/api/foundry/tools/<string:tool_ref>/execute",
            "/api/approvals",
            "/api/approvals/<int:approval_id>/decision",
            "/api/runs/<int:execution_id>",
        ):
            self.assertIn(route, source)

    def test_mcp_and_ai_tool_testers_are_implemented(self):
        protocols = (AI / "protocols.py").read_text(encoding="utf-8")
        requirements = (ROOT / "requirements-ai.txt").read_text(encoding="utf-8")
        self.assertIn("from mcp import Client", protocols)
        self.assertIn("streamable_http_client", protocols)
        self.assertIn("client.list_tools()", protocols)
        self.assertIn("client.call_tool(", protocols)
        self.assertIn("FoundryClient()", protocols)
        self.assertNotIn("MCP runtime client is not enabled", protocols)
        self.assertNotIn("AI Tool execution requires a linked Foundry tool/toolbox", protocols)
        self.assertIn("mcp>=2,<3", requirements)

    def test_credentials_are_references_not_database_secrets(self):
        protocols = (AI / "protocols.py").read_text(encoding="utf-8")
        models = (AI / "models.py").read_text(encoding="utf-8")
        self.assertIn("os.environ.get(ref)", protocols)
        self.assertIn("secret_ref = CharField", models)
        self.assertNotIn("secret_value = CharField", models)

    def test_v4_migration_is_registered(self):
        patches = (ROOT / "agent" / "patches.txt").read_text(encoding="utf-8")
        self.assertIn("agent.patches.expand_ai_control_center_foundry_runtime_v4", patches)
        migration = ROOT / "agent" / "patches" / "expand_ai_control_center_foundry_runtime_v4.py"
        self.assertTrue(migration.exists())
        ast.parse(migration.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
