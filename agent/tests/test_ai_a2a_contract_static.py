from __future__ import annotations

import unittest
from pathlib import Path

AGENT_ROOT = Path(__file__).resolve().parents[2]
AI = AGENT_ROOT / "agent" / "ai_control"


class A2AContractStaticTests(unittest.TestCase):
    def test_a2a_models_are_persistent_tables(self):
        source = (AI / "models.py").read_text(encoding="utf-8")
        for name in ("A2AParticipantModel", "A2AContextModel", "A2ATaskModel"):
            self.assertIn(f"class {name}", source)
            table_list = source[source.index("AI_CONTROL_TABLES =") :]
            self.assertIn(name, table_list)

    def test_agent_routes_use_a2a_not_generic_copilot_gateway(self):
        source = (AI / "routes.py").read_text(encoding="utf-8")
        self.assertNotIn("register_channel_routes", source)
        self.assertNotIn('"channels"', source)
        for route in (
            "/api/a2a/status",
            "/api/a2a/sync",
            "/api/a2a/participants",
            "/api/a2a/message",
            "/api/a2a/tasks",
        ):
            self.assertIn(route, source)

    def test_orchestrator_uses_foundry_responses_and_dynamic_participant_tools(self):
        runtime = (AI / "a2a_runtime.py").read_text(encoding="utf-8")
        foundry = (AI / "foundry.py").read_text(encoding="utf-8")
        self.assertIn("participant_tools(participants", runtime)
        self.assertIn("responses_with_tools", runtime)
        self.assertIn("previous_response_id", runtime)
        self.assertIn("function_call_output", runtime)
        self.assertIn("openai.responses.create", foundry)
        self.assertNotIn('"az-model-sol"', runtime)
        self.assertIn("A2A_ORCHESTRATOR_MODEL", runtime)

    def test_orchestration_has_no_small_implicit_iteration_cap(self):
        runtime = (AI / "a2a_runtime.py").read_text(encoding="utf-8")
        self.assertNotIn("max_iterations = 5", runtime)
        self.assertIn("A2A_ORCHESTRATOR_MAX_STEPS", runtime)
        self.assertIn("if not raw:\n        return None", runtime)

    def test_mcp_does_not_bypass_a2a_to_call_copilot_tools(self):
        source = (AI / "mcp_server.py").read_text(encoding="utf-8")
        self.assertNotIn("CopilotChannelClient", source)
        self.assertNotIn("call_copilot_tool", source)
        self.assertIn("run_a2a_task", source)

    def test_a2a_sidecar_is_v1_and_local_by_default(self):
        source = (AI / "a2a_server.py").read_text(encoding="utf-8")
        requirements = (AGENT_ROOT / "requirements-a2a.txt").read_text(encoding="utf-8")
        self.assertIn("A2A_RUNTIME_HOST", source)
        self.assertIn("127.0.0.1", source)
        self.assertIn('protocol_version="1.0"', source)
        self.assertIn("create_agent_card_routes", source)
        self.assertIn("create_jsonrpc_routes", source)
        self.assertIn("DefaultRequestHandler", source)
        self.assertIn("a2a-sdk[http-server]>=1.1.2,<2", requirements)

    def test_ui_exposes_a2a_network_not_chatgpt_channel_page(self):
        js = (AGENT_ROOT / "agent" / "static" / "ai_control" / "app.js").read_text(encoding="utf-8")
        html = (AGENT_ROOT / "agent" / "templates" / "ai_control" / "index.html").read_text(encoding="utf-8")
        self.assertIn("async function a2aPage()", js)
        self.assertIn("شبكة A2A", html)
        self.assertNotIn("قنوات ChatGPT", html)
        self.assertNotIn("channelsPage", js)

    def test_a2a_integration_tester_uses_standard_agent_card_discovery(self):
        source = (AI / "protocols.py").read_text(encoding="utf-8")
        self.assertIn("/.well-known/agent-card.json", source)
        self.assertIn("supportedInterfaces", source)
        self.assertNotIn("A2A runtime client is not enabled in this patch", source)

    def test_a2a_migration_and_console_entry_exist(self):
        patches = (AGENT_ROOT / "agent" / "patches.txt").read_text(encoding="utf-8")
        setup = (AGENT_ROOT / "setup.py").read_text(encoding="utf-8")
        self.assertIn("agent.patches.expand_ai_control_center_a2a_v3", patches)
        self.assertIn("agent-ai-a2a = agent.ai_control.a2a_server:main", setup)
        self.assertIn("agent-ai-a2a-check = agent.ai_control.a2a_cli:main", setup)
        self.assertIn('"ai-a2a"', setup)


if __name__ == "__main__":
    unittest.main(verbosity=2)
