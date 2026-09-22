"""Local Alazab A2A orchestration runtime.

Agent owns the Foundry control plane and A2A network. GPT-5.6 Sol performs
routing/delegation through Foundry Responses function calls. Copilot participates
only as the Frappe/Bench specialist; it is not a generic Agent gateway.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import re
import time
import uuid
from typing import Any

from peewee import DoesNotExist

from agent.ai_control.a2a_copilot import CopilotA2AClient
from agent.ai_control.foundry import FoundryClient
from agent.ai_control.models import AIConfigurationModel
from agent.ai_control.store import (
    append_a2a_task_trace,
    create_a2a_task,
    finish_execution,
    get_a2a_context,
    get_a2a_participant,
    get_a2a_task,
    list_a2a_participants,
    list_a2a_tasks,
    list_assets,
    record_execution,
    set_a2a_context,
    update_a2a_participant,
    update_a2a_task,
    upsert_a2a_participant,
)

ORCHESTRATOR_BASE_MODEL = "gpt-5.6-sol"


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"tool arguments are not valid JSON: {exc}") from exc
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("tool arguments must be a JSON object")


def _string_leaves(value: Any):
    if isinstance(value, dict):
        for item in value.values():
            yield from _string_leaves(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _string_leaves(item)
    elif isinstance(value, str):
        yield value


def _asset_is_gpt56_sol(asset: dict[str, Any]) -> bool:
    metadata = asset.get("metadata") or {}
    candidates = [asset.get("name"), *list(_string_leaves(metadata))]
    return any(str(x or "").strip().lower() == ORCHESTRATOR_BASE_MODEL for x in candidates)


def _explicit_models_from_active_configurations() -> list[tuple[str, str]]:
    keys = (
        "a2a_orchestrator_model",
        "orchestrator_model",
        "model_deployment",
        "deployment_name",
        "deployment",
        "model",
    )
    found: list[tuple[str, str]] = []
    query = AIConfigurationModel.select().where(AIConfigurationModel.status == "Active")
    for row in query:
        record = row.as_dict()
        data = record.get("config") or {}
        if not isinstance(data, dict):
            continue
        purpose = str(data.get("purpose") or data.get("role") or "").strip().lower()
        scope_ref = str(record.get("scope_ref") or "").strip().lower()
        if purpose not in {"a2a", "a2a_orchestrator"} and scope_ref not in {
            "a2a",
            "a2a-orchestrator",
            "a2a_orchestrator",
        }:
            continue
        for key in keys:
            value = str(data.get(key) or "").strip()
            if value:
                found.append((row.name, value))
                break
    return found


def resolve_orchestrator_model() -> dict[str, str]:
    """Resolve the exact Foundry deployment without a silent fallback."""
    env_model = (os.environ.get("A2A_ORCHESTRATOR_MODEL") or "").strip()
    if env_model:
        return {"deployment": env_model, "source": "A2A_ORCHESTRATOR_MODEL"}

    configured = _explicit_models_from_active_configurations()
    unique_configured = sorted({value for _, value in configured})
    if len(unique_configured) == 1:
        names = ", ".join(name for name, _ in configured)
        return {"deployment": unique_configured[0], "source": f"AIConfiguration:{names}"}
    if len(unique_configured) > 1:
        raise RuntimeError(
            "A2A orchestrator model is ambiguous across active AIConfigurations: "
            + ", ".join(f"{name}={value}" for name, value in configured)
        )

    matches = [asset for asset in list_assets("model") if _asset_is_gpt56_sol(asset)]
    deployment_names = sorted(
        {str(asset.get("name") or "").strip() for asset in matches if asset.get("name")}
    )
    if len(deployment_names) == 1:
        return {"deployment": deployment_names[0], "source": "Foundry synchronized model inventory"}
    if not deployment_names:
        raise RuntimeError(
            "No exact GPT-5.6 Sol deployment is configured or discoverable. "
            "Set A2A_ORCHESTRATOR_MODEL, create an active A2A-scoped AIConfiguration, or sync Foundry models."
        )
    raise RuntimeError(
        "Multiple synchronized deployments identify as gpt-5.6-sol; "
        "configure A2A_ORCHESTRATOR_MODEL explicitly: " + ", ".join(deployment_names)
    )


def _foundry_participant_card(asset: dict[str, Any]) -> dict[str, Any]:
    metadata = asset.get("metadata") or {}
    definition = metadata.get("definition") if isinstance(metadata.get("definition"), dict) else {}
    description = metadata.get("description") or definition.get("description")
    return {
        "name": asset["name"],
        "participant_type": "foundry_agent",
        "description": description or f"Foundry agent {asset['name']}",
        "project": asset.get("project"),
        "version": asset.get("version"),
        "status": asset.get("status"),
        "metadata": metadata,
    }


def sync_participants() -> dict[str, Any]:
    """Synchronize trusted local participants from Foundry inventory + Copilot."""
    saved: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for asset in list_assets("agent"):
        row = upsert_a2a_participant(
            {
                "name": asset["name"],
                "participant_type": "foundry_agent",
                "status": "Active"
                if str(asset.get("status") or "").lower() not in {"disabled", "deleted", "failed"}
                else "Unavailable",
                "source_ref": asset["name"],
                "agent_card": _foundry_participant_card(asset),
                "config": {"project": asset.get("project"), "foundry_asset_id": asset.get("id")},
            }
        )
        saved.append(row.as_dict())

    try:
        card = CopilotA2AClient().get_card()
        try:
            current = get_a2a_participant("copilot").as_dict()
            config = current.get("config") or {}
        except DoesNotExist:
            config = {}
        row = upsert_a2a_participant(
            {
                "name": "copilot",
                "participant_type": "copilot",
                "status": "Active",
                "source_ref": "copilot",
                "agent_card": card,
                "config": config,
                "last_seen_at": datetime.datetime.now(),
            }
        )
        saved.append(row.as_dict())
    except Exception as exc:
        errors.append({"participant": "copilot", "error": str(exc)})
        try:
            update_a2a_participant("copilot", {"status": "Unavailable"})
        except DoesNotExist:
            upsert_a2a_participant(
                {
                    "name": "copilot",
                    "participant_type": "copilot",
                    "status": "Unavailable",
                    "source_ref": "copilot",
                    "agent_card": {},
                    "config": {},
                }
            )

    return {"participants": saved, "errors": errors, "count": len(saved)}


def _safe_tool_name(name: str, used: set[str]) -> str:
    base = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower() or "participant"
    if base[0].isdigit():
        base = "p_" + base
    candidate = f"delegate_{base}"[:60]
    if candidate in used:
        suffix = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
        candidate = f"{candidate[:51]}_{suffix}"
    used.add(candidate)
    return candidate


def participant_tools(
    participants: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    tools: list[dict[str, Any]] = []
    mapping: dict[str, dict[str, Any]] = {}
    used: set[str] = set()
    for participant in participants:
        if str(participant.get("status") or "").lower() != "active":
            continue
        name = participant["name"]
        card = participant.get("agent_card") or {}
        description = str(card.get("description") or f"Delegate a task to A2A participant {name}")
        tool_name = _safe_tool_name(name, used)
        mapping[tool_name] = participant
        tools.append(
            {
                "type": "function",
                "name": tool_name,
                "description": f"Delegate work to {name}. {description}",
                "strict": True,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "message": {
                            "type": "string",
                            "description": "The complete task or question for this participant.",
                        }
                    },
                    "required": ["message"],
                    "additionalProperties": False,
                },
            }
        )
    return tools, mapping


def _copilot_configuration(participant: dict[str, Any]) -> str:
    config = participant.get("config") or {}
    value = str(
        config.get("copilot_configuration")
        or config.get("configuration")
        or os.environ.get("A2A_COPILOT_CONFIGURATION")
        or ""
    ).strip()
    if not value:
        raise RuntimeError(
            "Copilot A2A participant has no execution configuration. Set its config.copilot_configuration "
            "or A2A_COPILOT_CONFIGURATION."
        )
    return value


def _delegate_foundry(participant: dict[str, Any], message: str, context_id: str) -> dict[str, Any]:
    agent_name = str(participant.get("source_ref") or participant["name"])
    context = get_a2a_context(context_id, participant["name"])
    conversation_id = context.remote_context_id if context else None
    result = FoundryClient().chat_with_agent(agent_name, message, conversation_id)
    set_a2a_context(
        context_id,
        participant["name"],
        result.get("conversation_id"),
        {"participant_type": "foundry_agent", "agent_name": agent_name},
    )
    return result


def _delegate_copilot(
    participant: dict[str, Any], message: str, context_id: str, task_id: str
) -> dict[str, Any]:
    return CopilotA2AClient().send_message(
        message=message,
        context_id=context_id,
        task_id=task_id,
        source="gpt-5.6-sol-a2a-orchestrator",
        configuration=_copilot_configuration(participant),
    )


def delegate(
    participant: dict[str, Any], message: str, context_id: str, parent_task_id: str
) -> dict[str, Any]:
    delegation_id = str(uuid.uuid4())
    create_a2a_task(
        delegation_id,
        context_id,
        source="gpt-5.6-sol",
        target=participant["name"],
        input_data={"message": message, "parent_task_id": parent_task_id},
        state="TASK_STATE_WORKING",
    )
    try:
        if participant["participant_type"] == "foundry_agent":
            result = _delegate_foundry(participant, message, context_id)
        elif participant["participant_type"] == "copilot":
            result = _delegate_copilot(participant, message, context_id, delegation_id)
        elif participant["participant_type"] == "remote_a2a":
            raise RuntimeError(
                "remote_a2a participants require explicit trust/enablement and are not auto-routed yet"
            )
        else:
            raise RuntimeError(f"unsupported participant type: {participant['participant_type']}")
        update_a2a_task(delegation_id, state="TASK_STATE_COMPLETED", result=result, ended=True)
        return {"delegation_task_id": delegation_id, "participant": participant["name"], "result": result}
    except Exception as exc:
        update_a2a_task(
            delegation_id, state="TASK_STATE_FAILED", error=str(exc), result={"error": str(exc)}, ended=True
        )
        raise


def _function_calls(response: dict[str, Any]) -> list[dict[str, Any]]:
    calls = []
    for item in response.get("output") or []:
        if str(item.get("type") or "") == "function_call":
            calls.append(item)
    return calls


def _configured_max_steps() -> int | None:
    raw = (os.environ.get("A2A_ORCHESTRATOR_MAX_STEPS") or "").strip()
    if not raw:
        return None
    value = int(raw)
    if value < 1:
        raise ValueError("A2A_ORCHESTRATOR_MAX_STEPS must be a positive integer when configured")
    return value


def _orchestration_instructions() -> str:
    return (
        "You are GPT-5.6 Sol, the orchestration core of the Alazab A2A network. "
        "Route work only through the participant tools supplied to you. "
        "Each participant has a strict scope. Copilot is the Frappe/ERPNext/Bench specialist. "
        "Foundry agents handle their own declared specialties. Do not fabricate a participant result. "
        "Delegate whenever the task requires participant-owned data or execution, inspect returned outputs, "
        "delegate again when necessary, then produce one final answer grounded in those outputs."
    )


def _delegation_outputs(
    calls: list[dict[str, Any]],
    tool_map: dict[str, dict[str, Any]],
    context_id: str,
    root_task_id: str,
    step: int,
    trace: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    for call in calls:
        tool_name = str(call.get("name") or "")
        call_id = str(call.get("call_id") or "")
        if tool_name not in tool_map:
            raise RuntimeError(f"GPT requested unknown A2A participant tool: {tool_name}")
        args = _json_object(call.get("arguments") or "{}")
        delegated_message = str(args.get("message") or "").strip()
        if not delegated_message:
            raise ValueError(f"{tool_name} requires message")
        participant = tool_map[tool_name]
        delegated = delegate(participant, delegated_message, context_id, root_task_id)
        event = {
            "step": step,
            "type": "delegation",
            "tool": tool_name,
            "call_id": call_id,
            "participant": participant["name"],
            "delegation_task_id": delegated["delegation_task_id"],
            "request": delegated_message,
            "result": delegated["result"],
        }
        trace.append(event)
        append_a2a_task_trace(root_task_id, event)
        outputs.append(
            {
                "type": "function_call_output",
                "call_id": call_id,
                "output": _json(delegated),
            }
        )
    return outputs


def _run_orchestration_loop(
    *,
    model_info: dict[str, str],
    tools: list[dict[str, Any]],
    tool_map: dict[str, dict[str, Any]],
    context_id: str,
    root_task_id: str,
    execution,
    started: float,
    text: str,
    trace: list[dict[str, Any]],
) -> dict[str, Any]:
    max_steps = _configured_max_steps()
    step = 0
    previous_response_id: str | None = None
    next_input: Any = text
    seen_response_ids: set[str] = set()
    instructions = _orchestration_instructions()

    while True:
        step += 1
        if max_steps is not None and step > max_steps:
            raise RuntimeError(f"A2A orchestration reached configured A2A_ORCHESTRATOR_MAX_STEPS={max_steps}")

        response = FoundryClient().responses_with_tools(
            model=model_info["deployment"],
            input_items=next_input,
            tools=tools,
            instructions=instructions,
            previous_response_id=previous_response_id,
        )
        response_id = str(response.get("id") or "")
        if response_id and response_id in seen_response_ids:
            raise RuntimeError(
                f"Foundry Responses returned duplicate response id {response_id}; orchestration stopped"
            )
        if response_id:
            seen_response_ids.add(response_id)

        event = {
            "step": step,
            "type": "model_response",
            "response_id": response.get("id"),
            "model": response.get("model"),
            "usage": response.get("usage"),
            "output_text": response.get("output_text"),
        }
        trace.append(event)
        append_a2a_task_trace(root_task_id, event)

        calls = _function_calls(response)
        if not calls:
            result = {
                "task_id": root_task_id,
                "context_id": context_id,
                "orchestrator": model_info,
                "response_id": response.get("id"),
                "output_text": response.get("output_text") or "",
                "usage": response.get("usage"),
                "trace": trace,
            }
            update_a2a_task(
                root_task_id,
                state="TASK_STATE_COMPLETED",
                result=result,
                trace=trace,
                ended=True,
            )
            finish_execution(execution, "Success", result, (time.monotonic() - started) * 1000)
            return result

        next_input = _delegation_outputs(
            calls,
            tool_map,
            context_id,
            root_task_id,
            step,
            trace,
        )
        previous_response_id = response.get("id")
        if not previous_response_id:
            raise RuntimeError("Foundry function call response did not include response id")


def send_message(
    message: str,
    *,
    source: str = "user",
    context_id: str | None = None,
) -> dict[str, Any]:
    text = str(message or "").strip()
    if not text:
        raise ValueError("message is required")
    participants = list_a2a_participants(status="Active")
    if not participants:
        raise RuntimeError("A2A participant registry is empty. Synchronize participants first.")

    model_info = resolve_orchestrator_model()
    tools, tool_map = participant_tools(participants)
    if not tools:
        raise RuntimeError("No active A2A participants expose routable capabilities")

    context_id = str(context_id or uuid.uuid4())
    root_task_id = str(uuid.uuid4())
    create_a2a_task(
        root_task_id,
        context_id,
        source=source,
        target=None,
        input_data={"message": text, "orchestrator": model_info},
        state="TASK_STATE_WORKING",
    )
    execution = record_execution(
        "a2a_orchestrate",
        "a2a_task",
        root_task_id,
        {"context_id": context_id, "source": source, "model": model_info, "message": text},
    )
    started = time.monotonic()
    trace: list[dict[str, Any]] = []

    try:
        return _run_orchestration_loop(
            model_info=model_info,
            tools=tools,
            tool_map=tool_map,
            context_id=context_id,
            root_task_id=root_task_id,
            execution=execution,
            started=started,
            text=text,
            trace=trace,
        )
    except Exception as exc:
        failure = {
            "task_id": root_task_id,
            "context_id": context_id,
            "orchestrator": model_info,
            "error": str(exc),
            "trace": trace,
        }
        update_a2a_task(
            root_task_id,
            state="TASK_STATE_FAILED",
            result=failure,
            trace=trace,
            error=str(exc),
            ended=True,
        )
        finish_execution(execution, "Failure", failure, (time.monotonic() - started) * 1000)
        raise


def status() -> dict[str, Any]:
    participants = list_a2a_participants()
    try:
        model = resolve_orchestrator_model()
        model_error = None
    except Exception as exc:
        model = None
        model_error = str(exc)
    sidecar_host = (os.environ.get("A2A_RUNTIME_HOST") or "127.0.0.1").strip()
    sidecar_port = int(os.environ.get("A2A_RUNTIME_PORT") or "8766")
    public_url = (os.environ.get("A2A_PUBLIC_URL") or "").strip().rstrip("/") or None
    return {
        "runtime": "alazab-a2a",
        "protocol": "A2A 1.0",
        "standard_endpoint": {
            "host": sidecar_host,
            "port": sidecar_port,
            "public_url": public_url,
            "agent_card": f"{public_url}/.well-known/agent-card.json"
            if public_url
            else f"http://{sidecar_host}:{sidecar_port}/.well-known/agent-card.json",
        },
        "orchestrator": model,
        "orchestrator_error": model_error,
        "participants": {
            "total": len(participants),
            "active": sum(1 for x in participants if str(x.get("status") or "").lower() == "active"),
            "foundry_agent": sum(1 for x in participants if x.get("participant_type") == "foundry_agent"),
            "copilot": sum(1 for x in participants if x.get("participant_type") == "copilot"),
            "remote_a2a": sum(1 for x in participants if x.get("participant_type") == "remote_a2a"),
        },
        "copilot": next((x for x in participants if x.get("participant_type") == "copilot"), None),
        "configured_max_steps": _configured_max_steps(),
    }


def task(task_id: str) -> dict[str, Any]:
    return get_a2a_task(task_id).as_dict()


def tasks(limit: int = 100, context_id: str | None = None) -> list[dict[str, Any]]:
    return list_a2a_tasks(limit=limit, context_id=context_id)
