"""Optional MCP transport for Agent AI Control Center.

MCP is an external/additional transport. It exposes Agent-owned Foundry/A2A
capabilities and does not bypass the A2A runtime to call Copilot tools directly.
"""
from __future__ import annotations

import os
from typing import Any

from mcp.server import MCPServer

from agent.ai_control.execution import FoundryAgentRuntime
from agent.ai_control.a2a_runtime import (
    send_message as send_a2a_message,
    status as a2a_status_payload,
    sync_participants as sync_a2a_participants,
    tasks as list_a2a_tasks,
)
from agent.ai_control.store import list_a2a_participants, list_assets, list_integrations, list_bound_production_tools
from agent.ai_control.training import send_training_message as send_training_message_runtime

mcp = MCPServer(
    "Alazab AI Control",
    description="Foundry AI control plane with GPT-5.6 Sol A2A orchestration.",
)


@mcp.tool()
def a2a_status() -> dict[str, Any]:
    """Return the local A2A network and GPT-5.6 Sol orchestrator status."""
    return a2a_status_payload()


@mcp.tool()
def sync_a2a_network() -> dict[str, Any]:
    """Synchronize trusted Foundry agents and the signed Copilot participant."""
    return sync_a2a_participants()


@mcp.tool()
def list_a2a_network_participants(participant_type: str | None = None) -> list[dict[str, Any]]:
    """List trusted participants available to GPT-5.6 Sol A2A orchestration."""
    return list_a2a_participants(participant_type=participant_type)


@mcp.tool()
def run_a2a_task(message: str, context_id: str | None = None) -> dict[str, Any]:
    """Route one task through GPT-5.6 Sol to the appropriate A2A participants."""
    return send_a2a_message(message, source="mcp", context_id=context_id)


@mcp.tool()
def recent_a2a_tasks(limit: int = 50) -> list[dict[str, Any]]:
    """Return recent orchestration and delegation task records."""
    return list_a2a_tasks(limit=limit)


@mcp.tool()
def list_ai_assets(asset_type: str | None = None, project: str | None = None) -> dict[str, Any]:
    """List Foundry assets and Agent-managed AI integrations."""
    return {
        "assets": list_assets(asset_type, project),
        "integrations": list_integrations(asset_type if asset_type in {"mcp", "a2a", "api", "webhook", "ai_tool"} else None),
    }


@mcp.tool()
def list_foundry_bound_tools(agent_name: str) -> dict[str, Any]:
    """List Agent-owned production tools explicitly bound to a Foundry agent."""
    return {
        "agent_name": agent_name,
        "tools": list_bound_production_tools(agent_name),
    }


@mcp.tool()
def sync_foundry_bound_tools(agent_name: str) -> dict[str, Any]:
    """Publish the current Agent tool bindings as Foundry function tools."""
    return FoundryAgentRuntime().sync_tools(agent_name)


@mcp.tool()
def send_training_message(session_id: int, message: str) -> dict[str, Any]:
    """Continue an existing Foundry Agent training conversation."""
    return send_training_message_runtime(session_id, message)


def main():
    host = os.environ.get("AI_CHANNEL_MCP_HOST", "127.0.0.1")
    port = int(os.environ.get("AI_CHANNEL_MCP_PORT", "8765"))
    mcp.run(
        transport="streamable-http",
        host=host,
        port=port,
        streamable_http_path="/mcp",
        stateless_http=True,
        json_response=True,
    )


if __name__ == "__main__":
    main()
