from __future__ import annotations

import datetime
import json
import os
import re
import time
import uuid
from typing import TYPE_CHECKING, Any

from flask import current_app

from agent.ai_control.foundry import FoundryClient
from agent.ai_control.store import (
    create_approval_request,
    create_tool_call,
    finish_execution,
    get_approval_request,
    get_execution,
    get_production_tool,
    get_production_tool_by_ref,
    get_tool_call,
    list_approval_requests,
    list_bound_production_tools,
    list_tool_calls,
    record_execution,
    update_execution,
    update_tool_call,
)


if TYPE_CHECKING:
    from collections.abc import Callable

MANAGED_TOOL_PREFIX = "agent_tool_"


class FoundryExecutionError(RuntimeError):
    pass


def managed_tool_name(tool: dict[str, Any]) -> str:
    tool_id = int(tool["id"])
    raw = re.sub(r"[^a-zA-Z0-9_]+", "_", str(tool.get("name") or tool.get("display_name") or "tool"))
    raw = raw.strip("_").lower()[:40] or "tool"
    return f"{MANAGED_TOOL_PREFIX}{tool_id}_{raw}"


def managed_tool_id(function_name: str) -> int | None:
    match = re.match(rf"^{re.escape(MANAGED_TOOL_PREFIX)}(\d+)_", str(function_name or ""))
    return int(match.group(1)) if match else None


def production_tool_contract(tool: dict[str, Any]) -> dict[str, Any]:
    methods = [str(x).upper() for x in (tool.get("methods") or [])]
    if not methods:
        methods = ["GET"]
    route = str(tool.get("route") or "")
    risk = str(tool.get("risk_level") or "review")
    description = (
        f"Agent operation: {tool.get('display_name') or tool.get('name')}. "
        f"Route: {route}. Risk: {risk}. "
        "Provide path parameters, query parameters, and JSON body explicitly."
    )
    return {
        "name": managed_tool_name(tool),
        "description": description,
        "parameters": {
            "type": "object",
            "properties": {
                "method": {"type": "string", "enum": methods},
                "path_params": {"type": "object"},
                "query": {"type": "object"},
                "body": {},
            },
            "required": ["method"],
            "additionalProperties": False,
        },
        # Generic Agent routes can carry arbitrary JSON bodies. Keep strict off
        # until an OpenAPI/schema contract is available for that route.
        "strict": False,
    }


def bound_tool_contracts(agent_name: str) -> list[dict[str, Any]]:
    return [production_tool_contract(tool) for tool in list_bound_production_tools(agent_name)]


def tool_requires_approval(tool: dict[str, Any]) -> bool:
    risk = str(tool.get("risk_level") or "review").strip().lower()
    policy = str(tool.get("approval_policy") or "manual").strip().lower()
    return not (risk == "read" and policy == "auto")


def _approval_expiry() -> datetime.datetime:
    # Foundry function-call runs have a short lifetime. Keep the local approval
    # inside that window so a late approval never pretends it can resume a stale run.
    seconds = int(os.environ.get("FOUNDRY_APPROVAL_TTL_SECONDS", "480"))
    seconds = max(30, min(seconds, 540))
    return datetime.datetime.now() + datetime.timedelta(seconds=seconds)


def _json_value(response) -> Any:
    value = response.get_json(silent=True)
    if value is not None:
        return value
    text = response.get_data(as_text=True)
    limit = max(1000, int(os.environ.get("AI_TOOL_RESULT_MAX_CHARS", "100000")))
    return {"text": text[:limit], "truncated": len(text) > limit}


def execute_production_tool(tool: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
    """Execute one explicitly bound Agent HTTP route inside the running Flask app."""
    config = tool.get("config") or {}
    endpoint = str(config.get("endpoint") or "").strip()
    if not endpoint:
        raise FoundryExecutionError(f"Production tool '{tool.get('name')}' has no Flask endpoint")

    allowed_methods = [str(x).upper() for x in (tool.get("methods") or [])]
    method = str(arguments.get("method") or "").strip().upper()
    if not method:
        if len(allowed_methods) == 1:
            method = allowed_methods[0]
        else:
            raise FoundryExecutionError("method is required for a multi-method tool")
    if allowed_methods and method not in allowed_methods:
        raise FoundryExecutionError(
            f"Method {method} is not allowed; expected one of {', '.join(allowed_methods)}"
        )

    path_params = arguments.get("path_params") or {}
    query = arguments.get("query") or {}
    body = arguments.get("body")
    if not isinstance(path_params, dict) or not isinstance(query, dict):
        raise FoundryExecutionError("path_params and query must be objects")

    adapter = current_app.url_map.bind("localhost")
    try:
        path = adapter.build(endpoint, values=path_params, method=method, force_external=False)
    except Exception as exc:
        raise FoundryExecutionError(f"Could not build Agent route '{endpoint}': {exc}") from exc

    view = current_app.view_functions.get(endpoint)
    if view is None:
        raise FoundryExecutionError(f"Agent endpoint '{endpoint}' is not registered")

    # This is an in-process control-plane invocation, not a second unauthenticated
    # HTTP request. Flask route decorators (bench/site validation, etc.) remain
    # wrapped around the view function, while the global HTTP access-token hook
    # is not re-entered. Authorization is the explicit Agent-tool binding plus
    # the risk/approval gate enforced by this runtime.
    started = time.monotonic()
    with current_app.test_request_context(
        path,
        method=method,
        query_string=query,
        json=body if body is not None else None,
        headers={"X-Alazab-AI-Control": "foundry-agent-runtime"},
    ):
        raw_response = view(**path_params)
        response = current_app.make_response(raw_response)
    return {
        "ok": 200 <= response.status_code < 400,
        "status_code": response.status_code,
        "method": method,
        "path": path,
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "result": _json_value(response),
    }


class FoundryAgentRuntime:
    """Durable local execution loop for persisted Foundry agents and Agent tools."""

    def __init__(
        self,
        client: FoundryClient | None = None,
        tool_executor: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None,
    ):
        self.client = client or FoundryClient()
        self.tool_executor = tool_executor or execute_production_tool

    def tool_inventory(self, agent_name: str) -> dict[str, Any]:
        return {
            "agent_name": agent_name,
            "foundry_tools": self.client.list_agent_tools(agent_name),
            "agent_tools": list_bound_production_tools(agent_name),
            "function_tools": bound_tool_contracts(agent_name),
        }

    def sync_tools(self, agent_name: str) -> dict[str, Any]:
        contracts = bound_tool_contracts(agent_name)
        return self.client.sync_agent_function_tools(agent_name, contracts)

    @staticmethod
    def _function_calls(response: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            item
            for item in (response.get("output") or [])
            if str(item.get("type") or "") == "function_call"
        ]

    @staticmethod
    def _arguments(item: dict[str, Any]) -> dict[str, Any]:
        raw = item.get("arguments") or "{}"
        if isinstance(raw, dict):
            return raw
        try:
            value = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise FoundryExecutionError(f"Tool arguments are not valid JSON: {exc}") from exc
        if not isinstance(value, dict):
            raise FoundryExecutionError("Tool arguments must decode to an object")
        return value

    @staticmethod
    def _output(call_id: str, result: Any) -> dict[str, Any]:
        return {
            "type": "function_call_output",
            "call_id": call_id,
            "output": json.dumps(result, ensure_ascii=False, default=str),
        }

    def _execute_call(self, row, tool: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
        update_tool_call(row, status="Running")
        try:
            result = self.tool_executor(tool, arguments)
            status = "Success" if result.get("ok", result.get("success", True)) else "Failure"
            update_tool_call(row, status=status, result=result, ended=True)
            return result
        except Exception as exc:
            result = {"ok": False, "error": str(exc), "type": type(exc).__name__}
            update_tool_call(row, status="Failure", result=result, error=str(exc), ended=True)
            return result

    def _advance(  # noqa: C901
        self,
        execution,
        response: dict[str, Any],
        *,
        requested_by: str | None,
        step: int,
    ):
        max_steps = max(1, min(int(os.environ.get("FOUNDRY_AGENT_MAX_STEPS", "20")), 100))
        while step < max_steps:
            calls = self._function_calls(response)
            if not calls:
                result = {
                    "phase": "completed",
                    "agent_name": response.get("agent_name"),
                    "conversation_id": response.get("conversation_id"),
                    "response_id": response.get("response_id"),
                    "output_text": response.get("output_text") or "",
                    "response": response,
                    "steps": step + 1,
                }
                finish_execution(execution, "Success", result)
                return self.run_detail(execution.id)

            outputs: list[dict[str, Any]] = []
            pending_ids: list[int] = []
            for item in calls:
                function_name = str(item.get("name") or "")
                call_id = str(item.get("call_id") or item.get("id") or uuid.uuid4())
                arguments = self._arguments(item)
                tool_id = managed_tool_id(function_name)
                tool = None
                if tool_id is not None:
                    try:
                        tool = get_production_tool(tool_id).as_dict()
                    except Exception:
                        tool = None
                call_row = create_tool_call(
                    execution_id=execution.id,
                    agent_name=str(response.get("agent_name") or execution.resource_id or ""),
                    response_id=response.get("response_id"),
                    conversation_id=response.get("conversation_id"),
                    call_id=call_id,
                    tool_id=tool_id,
                    tool_name=function_name,
                    arguments=arguments,
                )

                if tool is None:
                    result = {"ok": False, "error": f"Unmanaged or missing function tool: {function_name}"}
                    update_tool_call(
                        call_row,
                        status="Failure",
                        result=result,
                        error=result["error"],
                        ended=True,
                    )
                    outputs.append(self._output(call_id, result))
                    continue

                if tool_requires_approval(tool):
                    approval = create_approval_request(
                        execution_id=execution.id,
                        tool_call_id=call_row.id,
                        agent_name=call_row.agent_name,
                        tool_name=function_name,
                        risk_level=str(tool.get("risk_level") or "review"),
                        approval_policy=str(tool.get("approval_policy") or "manual"),
                        request_data={"arguments": arguments, "tool": tool},
                        requested_by=requested_by,
                        expires_at=_approval_expiry(),
                    )
                    update_tool_call(call_row, status="AwaitingApproval", approval_id=approval.id)
                    pending_ids.append(call_row.id)
                else:
                    outputs.append(self._output(call_id, self._execute_call(call_row, tool, arguments)))

            if pending_ids:
                state = {
                    "phase": "awaiting_approval",
                    "agent_name": response.get("agent_name"),
                    "conversation_id": response.get("conversation_id"),
                    "response_id": response.get("response_id"),
                    "outputs": outputs,
                    "pending_tool_call_ids": pending_ids,
                    "steps": step + 1,
                }
                update_execution(execution, status="AwaitingApproval", result=state)
                return self.run_detail(execution.id)

            response = self.client.submit_tool_outputs(
                str(response.get("agent_name") or execution.resource_id),
                str(response.get("conversation_id") or ""),
                outputs,
            )
            step += 1

        result = {"phase": "failed", "error": f"Foundry agent exceeded {max_steps} execution steps"}
        finish_execution(execution, "Failure", result)
        return self.run_detail(execution.id)

    def start(self, agent_name: str, message: str, *, conversation_id=None, requested_by=None):
        agent_name = str(agent_name or "").strip()
        message = str(message or "").strip()
        if not agent_name or not message:
            raise ValueError("agent_name and message are required")
        execution = record_execution(
            "invoke",
            "foundry_agent_run",
            agent_name,
            {
                "agent_name": agent_name,
                "message": message,
                "conversation_id": conversation_id,
                "requested_by": requested_by,
            },
            status="Running",
        )
        try:
            response = self.client.invoke_agent(
                agent_name,
                message,
                conversation_id=conversation_id,
            )
            return self._advance(execution, response, requested_by=requested_by, step=0)
        except Exception as exc:
            finish_execution(execution, "Failure", {"phase": "failed", "error": str(exc)})
            raise

    def resume(self, execution_id: int):  # noqa: C901
        execution = get_execution(execution_id)
        if execution.resource_type != "foundry_agent_run":
            raise ValueError("execution is not a Foundry agent run")
        try:
            state = json.loads(execution.result_json or "{}")
        except json.JSONDecodeError:
            state = {}
        if execution.status != "AwaitingApproval" or state.get("phase") != "awaiting_approval":
            return self.run_detail(execution.id)

        outputs = list(state.get("outputs") or [])
        pending = []
        for tool_call_id in state.get("pending_tool_call_ids") or []:
            call_row = get_tool_call(int(tool_call_id))
            approval = get_approval_request(int(call_row.approval_id))
            if approval.status == "Pending":
                pending.append(call_row.id)
                continue
            if approval.status == "Approved":
                tool = get_production_tool(int(call_row.tool_id)).as_dict()
                arguments = json.loads(call_row.arguments_json or "{}")
                outputs.append(self._output(call_row.call_id, self._execute_call(call_row, tool, arguments)))
            elif approval.status == "Rejected":
                result = {
                    "ok": False,
                    "error": "Tool execution rejected by human approval gate",
                    "approval_id": approval.id,
                }
                update_tool_call(call_row, status="Rejected", result=result, ended=True)
                outputs.append(self._output(call_row.call_id, result))
            else:
                raise FoundryExecutionError(
                    f"Approval {approval.id} is {approval.status}; the Foundry run must be started again"
                )

        if pending:
            state["pending_tool_call_ids"] = pending
            state["outputs"] = outputs
            update_execution(execution, status="AwaitingApproval", result=state)
            return self.run_detail(execution.id)

        update_execution(
            execution,
            status="Running",
            result={**state, "phase": "running", "outputs": outputs},
        )
        try:
            response = self.client.submit_tool_outputs(
                str(state.get("agent_name") or execution.resource_id),
                str(state.get("conversation_id") or ""),
                outputs,
            )
            return self._advance(
                execution,
                response,
                requested_by=None,
                step=int(state.get("steps") or 0),
            )
        except Exception as exc:
            finish_execution(execution, "Failure", {"phase": "failed", "error": str(exc)})
            raise

    def execute_tool(
        self,
        agent_name: str,
        tool_ref: str | int,
        arguments: dict[str, Any],
        *,
        requested_by=None,
    ):
        tool = get_production_tool_by_ref(tool_ref).as_dict()
        execution = record_execution(
            "tool.execute",
            "production_tool",
            str(tool["id"]),
            {
                "agent_name": agent_name,
                "tool_id": tool["id"],
                "arguments": arguments,
                "requested_by": requested_by,
            },
            status="Running",
        )
        call_row = create_tool_call(
            execution_id=execution.id,
            agent_name=agent_name,
            response_id=None,
            conversation_id=None,
            call_id=f"direct-{uuid.uuid4()}",
            tool_id=int(tool["id"]),
            tool_name=managed_tool_name(tool),
            arguments=arguments,
        )
        if tool_requires_approval(tool):
            approval = create_approval_request(
                execution_id=execution.id,
                tool_call_id=call_row.id,
                agent_name=agent_name,
                tool_name=call_row.tool_name,
                risk_level=str(tool.get("risk_level") or "review"),
                approval_policy=str(tool.get("approval_policy") or "manual"),
                request_data={"arguments": arguments, "tool": tool},
                requested_by=requested_by,
                expires_at=None,
            )
            update_tool_call(call_row, status="AwaitingApproval", approval_id=approval.id)
            update_execution(
                execution,
                status="AwaitingApproval",
                result={
                    "phase": "awaiting_approval",
                    "tool_call_id": call_row.id,
                    "approval_id": approval.id,
                },
            )
            return self.run_detail(execution.id)

        result = self._execute_call(call_row, tool, arguments)
        finish_execution(execution, "Success" if result.get("ok") else "Failure", result)
        return self.run_detail(execution.id)

    def resume_direct(self, execution_id: int):
        execution = get_execution(execution_id)
        if execution.resource_type != "production_tool":
            raise ValueError("execution is not a direct production tool run")
        calls = list_tool_calls(execution_id=execution.id, limit=10)
        if not calls:
            raise FoundryExecutionError("tool call audit record is missing")
        call_row = get_tool_call(int(calls[0]["id"]))
        approval = get_approval_request(int(call_row.approval_id))
        if approval.status == "Pending":
            return self.run_detail(execution.id)
        if approval.status == "Rejected":
            result = {"ok": False, "error": "Tool execution rejected by human approval gate"}
            update_tool_call(call_row, status="Rejected", result=result, ended=True)
            finish_execution(execution, "Rejected", result)
            return self.run_detail(execution.id)
        if approval.status != "Approved":
            raise FoundryExecutionError(f"Approval is {approval.status}")
        tool = get_production_tool(int(call_row.tool_id)).as_dict()
        arguments = json.loads(call_row.arguments_json or "{}")
        result = self._execute_call(call_row, tool, arguments)
        finish_execution(execution, "Success" if result.get("ok") else "Failure", result)
        return self.run_detail(execution.id)

    def run_detail(self, execution_id: int) -> dict[str, Any]:
        execution = get_execution(execution_id)
        return {
            "run": execution.as_dict(),
            "tool_calls": list_tool_calls(execution_id=execution.id),
            "approvals": list_approval_requests(execution_id=execution.id),
        }
