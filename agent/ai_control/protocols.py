from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import requests

from agent.ai_control.models import AIIntegrationModel
from agent.ai_control.foundry import FoundryClient


class ProtocolTestError(RuntimeError):
    pass


def _config(integration: AIIntegrationModel) -> dict[str, Any]:
    try:
        value = json.loads(integration.config or "{}")
    except json.JSONDecodeError as exc:
        raise ProtocolTestError(f"Integration config is not valid JSON: {exc}") from exc
    return value if isinstance(value, dict) else {}


def _secret(integration: AIIntegrationModel) -> str | None:
    ref = str(integration.secret_ref or "").strip()
    if not ref:
        return None
    value = os.environ.get(ref)
    if value is None:
        raise ProtocolTestError(f"Credential environment variable '{ref}' is not configured")
    return value


def _headers(integration: AIIntegrationModel) -> dict[str, str]:
    config = _config(integration)
    headers = {"Accept": "application/json"}
    configured = config.get("headers") or {}
    if isinstance(configured, dict):
        headers.update({str(k): str(v) for k, v in configured.items()})

    auth_type = str(integration.auth_type or "none").strip().lower()
    secret = _secret(integration) if integration.secret_ref else None
    if auth_type in {"none", ""}:
        return headers
    if not secret:
        raise ProtocolTestError(f"Authentication type '{auth_type}' requires secret_ref")
    if auth_type in {"bearer", "oauth2"}:
        headers["Authorization"] = f"Bearer {secret}"
    elif auth_type == "basic":
        token = base64.b64encode(secret.encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {token}"
    elif auth_type in {"api_key", "header"}:
        header_name = str(config.get("auth_header") or "X-API-Key")
        headers[header_name] = secret
    else:
        raise ProtocolTestError(f"Unsupported authentication type: {auth_type}")
    return headers


def _run_async(factory):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(factory())).result()


def test_api(integration: AIIntegrationModel) -> dict[str, Any]:
    if not integration.endpoint:
        raise ProtocolTestError("API endpoint is required")
    started = time.monotonic()
    response = requests.get(integration.endpoint, headers=_headers(integration), timeout=20)
    return {
        "ok": response.ok,
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type"),
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "preview": response.text[:5000],
    }


def test_webhook(integration: AIIntegrationModel) -> dict[str, Any]:
    if not integration.endpoint:
        raise ProtocolTestError("Webhook target endpoint is required")
    config = json.loads(integration.config or "{}")
    payload = config.get("test_payload") or {"source": "agent-ai-control", "event": "test"}
    started = time.monotonic()
    response = requests.post(
        integration.endpoint,
        json=payload,
        headers={**_headers(integration), "Content-Type": "application/json"},
        timeout=20,
    )
    return {
        "ok": response.ok,
        "status_code": response.status_code,
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "preview": response.text[:5000],
    }


def test_mcp(integration: AIIntegrationModel) -> dict[str, Any]:
    """Connect with the official MCP Python SDK and verify tool discovery/call."""
    if not integration.endpoint:
        raise ProtocolTestError("MCP endpoint is required")

    async def probe():
        try:
            from mcp import Client
            from mcp.client.streamable_http import streamable_http_client
            import httpx2
        except ImportError as exc:
            raise ProtocolTestError("MCP SDK v2 is not installed; install agent[ai]") from exc

        headers = _headers(integration)
        config = _config(integration)
        timeout = float(config.get("timeout_seconds") or 30)
        started = time.monotonic()
        async with httpx2.AsyncClient(headers=headers, timeout=timeout) as http_client:
            transport = streamable_http_client(integration.endpoint, http_client=http_client)
            async with Client(transport) as client:
                listed = await client.list_tools()
                raw_tools = getattr(listed, "tools", listed) or []
                tools = []
                for tool in raw_tools:
                    if hasattr(tool, "model_dump"):
                        tools.append(tool.model_dump())
                    elif hasattr(tool, "to_dict"):
                        tools.append(tool.to_dict())
                    else:
                        tools.append({"name": getattr(tool, "name", str(tool))})

                result = {
                    "ok": True,
                    "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                    "protocol_version": str(getattr(client, "protocol_version", "") or ""),
                    "server_info": (
                        getattr(client.server_info, "model_dump", lambda: {})()
                        if getattr(client, "server_info", None)
                        else None
                    ),
                    "tool_count": len(tools),
                    "tools": tools,
                }

                test_tool = str(config.get("test_tool") or "").strip()
                if test_tool:
                    call = await client.call_tool(test_tool, config.get("test_arguments") or {})
                    if hasattr(call, "model_dump"):
                        result["test_call"] = call.model_dump()
                    else:
                        result["test_call"] = {"result": str(call)}
                return result

    return _run_async(probe)




def test_a2a(integration: AIIntegrationModel) -> dict[str, Any]:
    """Validate standard A2A Agent Card discovery without auto-trusting the peer.

    This is a discovery/compatibility check only. Routing an external A2A peer still
    requires explicit participant registration and trust in the A2A control plane.
    """
    if not integration.endpoint:
        raise ProtocolTestError("A2A endpoint is required")
    base = integration.endpoint.strip().rstrip("/")
    if base.endswith("/.well-known/agent-card.json"):
        card_url = base
    else:
        card_url = f"{base}/.well-known/agent-card.json"
    started = time.monotonic()
    response = requests.get(card_url, headers=_headers(integration), timeout=20)
    elapsed_ms = round((time.monotonic() - started) * 1000, 3)
    if not response.ok:
        return {
            "ok": False,
            "status_code": response.status_code,
            "elapsed_ms": elapsed_ms,
            "agent_card_url": card_url,
            "preview": response.text[:5000],
        }
    try:
        card = response.json()
    except ValueError as exc:
        raise ProtocolTestError(f"A2A Agent Card is not valid JSON: {exc}") from exc
    name = card.get("name") if isinstance(card, dict) else None
    interfaces = []
    if isinstance(card, dict):
        interfaces = card.get("supportedInterfaces") or card.get("supported_interfaces") or []
    if not name or not isinstance(interfaces, list) or not interfaces:
        raise ProtocolTestError("A2A Agent Card is missing name or supported interfaces")
    return {
        "ok": True,
        "status_code": response.status_code,
        "elapsed_ms": elapsed_ms,
        "agent_card_url": card_url,
        "agent": name,
        "supported_interfaces": interfaces,
        "discovery_only": True,
    }


def test_ai_tool(integration: AIIntegrationModel) -> dict[str, Any]:
    """Verify a Foundry agent/tool binding and optionally execute a harmless test prompt."""
    config = _config(integration)
    agent_name = str(config.get("agent_name") or integration.project or "").strip()
    if not agent_name:
        raise ProtocolTestError("AI Tool integration requires config.agent_name or project")
    client = FoundryClient()
    tools = client.list_agent_tools(agent_name)
    expected = str(config.get("tool_name") or "").strip()
    if expected:
        names = []
        for tool in tools:
            name = tool.get("name")
            if not name and isinstance(tool.get("function"), dict):
                name = tool["function"].get("name")
            if name:
                names.append(str(name))
        if expected not in names:
            return {
                "ok": False,
                "agent_name": agent_name,
                "expected_tool": expected,
                "available_tools": names,
            }

    result = {
        "ok": True,
        "agent_name": agent_name,
        "tool_count": len(tools),
        "tools": tools,
    }
    prompt = str(config.get("test_prompt") or "").strip()
    if prompt:
        result["response"] = client.invoke_agent(agent_name, prompt)
    return result




def test_integration(integration: AIIntegrationModel) -> dict[str, Any]:
    handlers = {
        "api": test_api,
        "webhook": test_webhook,
        "mcp": test_mcp,
        "a2a": test_a2a,
        "ai_tool": test_ai_tool,
    }
    handler = handlers.get(integration.integration_type)
    if not handler:
        raise ProtocolTestError(f"No tester for {integration.integration_type}")
    return handler(integration)
