from __future__ import annotations

import datetime
import json
from typing import Any

from peewee import IntegrityError

from agent.ai_control.models import (
    A2AContextModel,
    A2AParticipantModel,
    A2ATaskModel,
    AIApprovalRequestModel,
    AIBindingModel,
    AIConfigurationModel,
    AIExecutionModel,
    AIIntegrationModel,
    AIKnowledgeSourceModel,
    AIProductionToolModel,
    AIToolCallModel,
    FoundryAssetModel,
)

SUPPORTED_INTEGRATION_TYPES = {"mcp", "a2a", "api", "webhook", "ai_tool"}
SUPPORTED_CONFIG_TYPES = {"runtime", "model", "behavior", "knowledge", "tools", "safety"}
SUPPORTED_SCOPES = {"global", "agent", "tool", "integration"}


def list_integrations(integration_type: str | None = None):
    query = AIIntegrationModel.select().order_by(AIIntegrationModel.integration_type, AIIntegrationModel.name)
    if integration_type:
        query = query.where(AIIntegrationModel.integration_type == integration_type)
    return [row.as_dict() for row in query]


def get_integration(integration_id: int):
    return AIIntegrationModel.get_by_id(integration_id)


def create_integration(payload: dict[str, Any]) -> AIIntegrationModel:
    integration_type = str(payload.get("type") or "").strip().lower()
    if integration_type not in SUPPORTED_INTEGRATION_TYPES:
        raise ValueError(f"Unsupported integration type: {integration_type}")
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("name is required")
    now = datetime.datetime.now()
    try:
        return AIIntegrationModel.create(
            name=name,
            integration_type=integration_type,
            status=payload.get("status") or "Draft",
            project=payload.get("project"),
            endpoint=payload.get("endpoint"),
            auth_type=payload.get("auth_type") or "none",
            secret_ref=payload.get("secret_ref"),
            config=json.dumps(payload.get("config") or {}, default=str),
            created_at=now,
            modified_at=now,
        )
    except IntegrityError as exc:
        raise ValueError(f"Integration '{name}' already exists") from exc


def update_integration(integration_id: int, payload: dict[str, Any]) -> AIIntegrationModel:
    row = get_integration(integration_id)
    if "name" in payload:
        row.name = str(payload["name"]).strip()
    if "type" in payload:
        integration_type = str(payload["type"]).strip().lower()
        if integration_type not in SUPPORTED_INTEGRATION_TYPES:
            raise ValueError(f"Unsupported integration type: {integration_type}")
        row.integration_type = integration_type
    for source, target in (
        ("status", "status"),
        ("project", "project"),
        ("endpoint", "endpoint"),
        ("auth_type", "auth_type"),
        ("secret_ref", "secret_ref"),
    ):
        if source in payload:
            setattr(row, target, payload[source])
    if "config" in payload:
        row.config = json.dumps(payload.get("config") or {}, default=str)
    row.modified_at = datetime.datetime.now()
    row.save()
    return row


def delete_integration(integration_id: int):
    row = get_integration(integration_id)
    data = row.as_dict()
    row.delete_instance()
    return data


def list_assets(asset_type: str | None = None, project: str | None = None):
    query = FoundryAssetModel.select().order_by(FoundryAssetModel.asset_type, FoundryAssetModel.name)
    if asset_type:
        query = query.where(FoundryAssetModel.asset_type == asset_type)
    if project:
        query = query.where(FoundryAssetModel.project == project)
    return [row.as_dict() for row in query]


def get_asset(asset_id: int):
    return FoundryAssetModel.get_by_id(asset_id)


def upsert_asset(asset: dict[str, Any]):
    key = {"project": asset["project"], "asset_type": asset["type"], "name": asset["name"]}
    values = {
        "external_id": asset.get("external_id"),
        "source": asset.get("source") or "Foundry",
        "status": asset.get("status"),
        "version": str(asset.get("version")) if asset.get("version") is not None else None,
        "metadata_json": json.dumps(asset.get("metadata") or {}, default=str),
        "last_synced_at": datetime.datetime.now(),
    }
    row, created = FoundryAssetModel.get_or_create(defaults=values, **key)
    if not created:
        for field, value in values.items():
            setattr(row, field, value)
        row.save()
    return row


def record_execution(action, resource_type, resource_id=None, request_data=None, status="Pending"):
    return AIExecutionModel.create(
        action=action,
        resource_type=resource_type,
        resource_id=str(resource_id) if resource_id is not None else None,
        status=status,
        request_json=json.dumps(request_data or {}, default=str),
    )


def finish_execution(row: AIExecutionModel, status: str, result: Any, duration_ms: float | None = None):
    row.status = status
    row.result_json = json.dumps(result, default=str)
    row.ended_at = datetime.datetime.now()
    row.duration_ms = str(round(duration_ms, 3)) if duration_ms is not None else None
    row.save()
    return row


def list_executions(limit: int = 100, resource_type: str | None = None):
    query = AIExecutionModel.select().order_by(AIExecutionModel.id.desc())
    if resource_type:
        query = query.where(AIExecutionModel.resource_type == resource_type)
    return [row.as_dict() for row in query.limit(limit)]


def list_bindings():
    return [
        row.as_dict()
        for row in AIBindingModel.select().order_by(AIBindingModel.source_ref, AIBindingModel.target_ref)
    ]


def create_binding(payload: dict[str, Any]) -> AIBindingModel:
    required = ("source_type", "source_ref", "target_type", "target_ref")
    missing = [key for key in required if not str(payload.get(key) or "").strip()]
    if missing:
        raise ValueError("Missing binding fields: " + ", ".join(missing))
    try:
        return AIBindingModel.create(
            source_type=str(payload["source_type"]).strip(),
            source_ref=str(payload["source_ref"]).strip(),
            target_type=str(payload["target_type"]).strip(),
            target_ref=str(payload["target_ref"]).strip(),
            status=payload.get("status") or "Active",
            config=json.dumps(payload.get("config") or {}, default=str),
        )
    except IntegrityError as exc:
        raise ValueError("Binding already exists") from exc


def delete_binding(binding_id: int):
    row = AIBindingModel.get_by_id(binding_id)
    data = row.as_dict()
    row.delete_instance()
    return data


def list_knowledge(status: str | None = None):
    query = AIKnowledgeSourceModel.select().order_by(AIKnowledgeSourceModel.id.desc())
    if status:
        query = query.where(AIKnowledgeSourceModel.status == status)
    return [row.as_dict() for row in query]


def get_knowledge(knowledge_id: int) -> AIKnowledgeSourceModel:
    return AIKnowledgeSourceModel.get_by_id(knowledge_id)


def set_knowledge_approval(knowledge_id: int, approved: bool) -> AIKnowledgeSourceModel:
    row = get_knowledge(knowledge_id)
    row.approval_status = "Approved" if approved else "Rejected"
    row.status = "Approved" if approved else "Rejected"
    row.modified_at = datetime.datetime.now()
    row.save()
    return row


def list_configurations(scope_type: str | None = None, scope_ref: str | None = None):
    query = AIConfigurationModel.select().order_by(
        AIConfigurationModel.config_type, AIConfigurationModel.name
    )
    if scope_type:
        query = query.where(AIConfigurationModel.scope_type == scope_type)
    if scope_ref:
        query = query.where(AIConfigurationModel.scope_ref == scope_ref)
    return [row.as_dict() for row in query]


def get_configuration(config_id: int):
    return AIConfigurationModel.get_by_id(config_id)


def create_configuration(payload: dict[str, Any]) -> AIConfigurationModel:
    name = str(payload.get("name") or "").strip()
    config_type = str(payload.get("config_type") or "runtime").strip().lower()
    scope_type = str(payload.get("scope_type") or "global").strip().lower()
    if not name:
        raise ValueError("name is required")
    if config_type not in SUPPORTED_CONFIG_TYPES:
        raise ValueError(f"Unsupported config_type: {config_type}")
    if scope_type not in SUPPORTED_SCOPES:
        raise ValueError(f"Unsupported scope_type: {scope_type}")
    try:
        return AIConfigurationModel.create(
            name=name,
            config_type=config_type,
            scope_type=scope_type,
            scope_ref=payload.get("scope_ref"),
            status=payload.get("status") or "Draft",
            config_json=json.dumps(payload.get("config") or {}, ensure_ascii=False, default=str),
            secret_ref=payload.get("secret_ref"),
        )
    except IntegrityError as exc:
        raise ValueError(f"Configuration '{name}' already exists") from exc


def update_configuration(config_id: int, payload: dict[str, Any]) -> AIConfigurationModel:
    row = get_configuration(config_id)
    for field in ("name", "config_type", "scope_type", "scope_ref", "status", "secret_ref"):
        if field in payload:
            setattr(row, field, payload[field])
    if "config" in payload:
        row.config_json = json.dumps(payload.get("config") or {}, ensure_ascii=False, default=str)
    row.version += 1
    row.modified_at = datetime.datetime.now()
    row.save()
    return row


def list_production_tools(status: str | None = None):
    query = AIProductionToolModel.select().order_by(
        AIProductionToolModel.category, AIProductionToolModel.display_name
    )
    if status:
        query = query.where(AIProductionToolModel.status == status)
    return [row.as_dict() for row in query]


def upsert_production_tool(payload: dict[str, Any]) -> AIProductionToolModel:
    name = str(payload["name"])
    values = {
        "display_name": payload.get("display_name") or name,
        "category": payload.get("category") or "agent_api",
        "source": payload.get("source") or "Agent",
        "route": payload.get("route"),
        "methods_json": json.dumps(payload.get("methods") or []),
        "status": payload.get("status") or "Discovered",
        "approval_policy": payload.get("approval_policy") or "manual",
        "risk_level": payload.get("risk_level") or "review",
        "config_json": json.dumps(payload.get("config") or {}, default=str),
        "modified_at": datetime.datetime.now(),
    }
    row, created = AIProductionToolModel.get_or_create(name=name, defaults=values)
    if not created:
        for field, value in values.items():
            setattr(row, field, value)
        row.save()
    return row


def update_production_tool(tool_id: int, payload: dict[str, Any]) -> AIProductionToolModel:
    row = AIProductionToolModel.get_by_id(tool_id)
    for field in ("display_name", "category", "status", "approval_policy", "risk_level"):
        if field in payload:
            setattr(row, field, payload[field])
    if "config" in payload:
        row.config_json = json.dumps(payload.get("config") or {}, default=str)
    row.modified_at = datetime.datetime.now()
    row.save()
    return row


def _append_topology_node(
    nodes: list[dict[str, Any]],
    seen: set[str],
    node: dict[str, Any],
) -> None:
    if node["id"] in seen:
        return
    nodes.append(node)
    seen.add(node["id"])


def topology():
    assets = list_assets()
    integrations = list_integrations()
    bindings = list_bindings()
    nodes: list[dict[str, Any]] = []
    seen: set[str] = set()

    for asset in assets:
        _append_topology_node(
            nodes,
            seen,
            {
                "id": f"asset:{asset['type']}:{asset['name']}",
                "kind": asset["type"],
                "name": asset["name"],
                "status": asset.get("status"),
                "project": asset.get("project"),
            },
        )
    for item in integrations:
        _append_topology_node(
            nodes,
            seen,
            {
                "id": f"integration:{item['type']}:{item['id']}",
                "kind": item["type"],
                "name": item["name"],
                "status": item.get("status"),
                "project": item.get("project"),
            },
        )
    for item in list_knowledge():
        _append_topology_node(
            nodes,
            seen,
            {
                "id": f"knowledge:{item['id']}",
                "kind": "knowledge",
                "name": item["title"],
                "status": item["status"],
                "project": None,
            },
        )
    for item in list_production_tools():
        _append_topology_node(
            nodes,
            seen,
            {
                "id": f"production_tool:{item['id']}",
                "kind": "production_tool",
                "name": item["display_name"],
                "status": item["status"],
                "project": None,
            },
        )
    for item in list_a2a_participants():
        _append_topology_node(
            nodes,
            seen,
            {
                "id": f"a2a_participant:{item['name']}",
                "kind": "a2a_participant",
                "name": item["name"],
                "status": item["status"],
                "project": item.get("participant_type"),
            },
        )

    edges = [
        {
            "id": binding["id"],
            "source_type": binding["source_type"],
            "source_ref": binding["source_ref"],
            "target_type": binding["target_type"],
            "target_ref": binding["target_ref"],
            "status": binding["status"],
        }
        for binding in bindings
    ]
    return {"nodes": nodes, "edges": edges}


# ---------------------------------------------------------------------------
# A2A network persistence
# ---------------------------------------------------------------------------


def list_a2a_participants(participant_type: str | None = None, status: str | None = None):
    query = A2AParticipantModel.select().order_by(
        A2AParticipantModel.participant_type, A2AParticipantModel.name
    )
    if participant_type:
        query = query.where(A2AParticipantModel.participant_type == participant_type)
    if status:
        query = query.where(A2AParticipantModel.status == status)
    return [row.as_dict() for row in query]


def get_a2a_participant(name: str) -> A2AParticipantModel:
    return A2AParticipantModel.get(A2AParticipantModel.name == name)


def upsert_a2a_participant(payload: dict[str, Any]) -> A2AParticipantModel:
    name = str(payload.get("name") or "").strip()
    participant_type = str(payload.get("participant_type") or "").strip()
    if not name:
        raise ValueError("participant name is required")
    if participant_type not in {"foundry_agent", "copilot", "remote_a2a"}:
        raise ValueError(f"unsupported A2A participant type: {participant_type}")
    values = {
        "participant_type": participant_type,
        "status": payload.get("status") or "Active",
        "source_ref": payload.get("source_ref"),
        "endpoint": payload.get("endpoint"),
        "agent_card_json": json.dumps(payload.get("agent_card") or {}, ensure_ascii=False, default=str),
        "config_json": json.dumps(payload.get("config") or {}, ensure_ascii=False, default=str),
        "last_seen_at": payload.get("last_seen_at") or datetime.datetime.now(),
        "modified_at": datetime.datetime.now(),
    }
    row, created = A2AParticipantModel.get_or_create(
        name=name, defaults={**values, "created_at": datetime.datetime.now()}
    )
    if not created:
        for field, value in values.items():
            setattr(row, field, value)
        row.save()
    return row


def update_a2a_participant(name: str, payload: dict[str, Any]) -> A2AParticipantModel:
    row = get_a2a_participant(name)
    if "status" in payload:
        row.status = str(payload["status"])
    if "endpoint" in payload:
        row.endpoint = payload.get("endpoint")
    if "agent_card" in payload:
        row.agent_card_json = json.dumps(payload.get("agent_card") or {}, ensure_ascii=False, default=str)
    if "config" in payload:
        row.config_json = json.dumps(payload.get("config") or {}, ensure_ascii=False, default=str)
    row.modified_at = datetime.datetime.now()
    row.save()
    return row


def get_a2a_context(context_id: str, participant_name: str) -> A2AContextModel | None:
    return A2AContextModel.get_or_none(
        (A2AContextModel.context_id == context_id) & (A2AContextModel.participant_name == participant_name)
    )


def set_a2a_context(
    context_id: str,
    participant_name: str,
    remote_context_id: str | None,
    metadata: dict[str, Any] | None = None,
) -> A2AContextModel:
    row = get_a2a_context(context_id, participant_name)
    now = datetime.datetime.now()
    if row is None:
        return A2AContextModel.create(
            context_id=context_id,
            participant_name=participant_name,
            remote_context_id=remote_context_id,
            metadata_json=json.dumps(metadata or {}, ensure_ascii=False, default=str),
            created_at=now,
            modified_at=now,
        )
    row.remote_context_id = remote_context_id
    if metadata is not None:
        row.metadata_json = json.dumps(metadata, ensure_ascii=False, default=str)
    row.modified_at = now
    row.save()
    return row


def create_a2a_task(
    task_id: str,
    context_id: str,
    source: str,
    target: str | None,
    input_data: dict[str, Any],
    state: str = "TASK_STATE_SUBMITTED",
) -> A2ATaskModel:
    now = datetime.datetime.now()
    return A2ATaskModel.create(
        task_id=task_id,
        context_id=context_id,
        source=source,
        target=target,
        state=state,
        input_json=json.dumps(input_data or {}, ensure_ascii=False, default=str),
        result_json="{}",
        trace_json="[]",
        started_at=now,
        modified_at=now,
    )


def update_a2a_task(
    task_id: str,
    *,
    state: str | None = None,
    result: dict[str, Any] | None = None,
    trace: list[dict[str, Any]] | None = None,
    error: str | None = None,
    ended: bool = False,
) -> A2ATaskModel:
    row = A2ATaskModel.get(A2ATaskModel.task_id == task_id)
    if state is not None:
        row.state = state
    if result is not None:
        row.result_json = json.dumps(result, ensure_ascii=False, default=str)
    if trace is not None:
        row.trace_json = json.dumps(trace, ensure_ascii=False, default=str)
    if error is not None:
        row.error = error
    if ended:
        row.ended_at = datetime.datetime.now()
    row.modified_at = datetime.datetime.now()
    row.save()
    return row


def append_a2a_task_trace(task_id: str, event: dict[str, Any]) -> A2ATaskModel:
    row = A2ATaskModel.get(A2ATaskModel.task_id == task_id)
    try:
        trace = json.loads(row.trace_json or "[]")
    except json.JSONDecodeError:
        trace = []
    trace.append(event)
    row.trace_json = json.dumps(trace, ensure_ascii=False, default=str)
    row.modified_at = datetime.datetime.now()
    row.save()
    return row


def list_a2a_tasks(limit: int = 100, context_id: str | None = None):
    query = A2ATaskModel.select().order_by(A2ATaskModel.started_at.desc())
    if context_id:
        query = query.where(A2ATaskModel.context_id == context_id)
    return [row.as_dict() for row in query.limit(max(1, min(int(limit), 1000)))]


def get_a2a_task(task_id: str) -> A2ATaskModel:
    return A2ATaskModel.get(A2ATaskModel.task_id == task_id)


def get_production_tool(tool_id: int) -> AIProductionToolModel:
    return AIProductionToolModel.get_by_id(int(tool_id))


def get_production_tool_by_ref(tool_ref: str | int) -> AIProductionToolModel:
    raw = str(tool_ref).strip()
    if raw.isdigit():
        return get_production_tool(int(raw))
    row = AIProductionToolModel.get_or_none(AIProductionToolModel.name == raw)
    if row is None:
        raise AIProductionToolModel.DoesNotExist()
    return row


def list_bound_production_tools(agent_name: str) -> list[dict[str, Any]]:
    bindings = (
        AIBindingModel.select()
        .where(
            (AIBindingModel.source_type == "agent")
            & (AIBindingModel.source_ref == agent_name)
            & (AIBindingModel.target_type == "production_tool")
            & (AIBindingModel.status == "Active")
        )
        .order_by(AIBindingModel.id)
    )
    result = []
    for binding in bindings:
        try:
            tool = get_production_tool(int(binding.target_ref))
        except (ValueError, AIProductionToolModel.DoesNotExist):
            continue
        if str(tool.status or "").lower() == "disabled":
            continue
        item = tool.as_dict()
        item["binding_id"] = binding.id
        item["binding"] = binding.as_dict()
        result.append(item)
    return result


def get_agent_tool_binding(agent_name: str, tool_id: int) -> AIBindingModel | None:
    return AIBindingModel.get_or_none(
        (AIBindingModel.source_type == "agent")
        & (AIBindingModel.source_ref == agent_name)
        & (AIBindingModel.target_type == "production_tool")
        & (AIBindingModel.target_ref == str(int(tool_id)))
    )


def remove_agent_tool_binding(agent_name: str, tool_id: int) -> dict[str, Any]:
    row = get_agent_tool_binding(agent_name, tool_id)
    if row is None:
        raise AIBindingModel.DoesNotExist()
    data = row.as_dict()
    row.delete_instance()
    return data


def get_execution(execution_id: int) -> AIExecutionModel:
    return AIExecutionModel.get_by_id(int(execution_id))


def update_execution(row: AIExecutionModel, *, status=None, result=None, ended=False, duration_ms=None):
    if status is not None:
        row.status = status
    if result is not None:
        row.result_json = json.dumps(result, default=str)
    if ended:
        row.ended_at = datetime.datetime.now()
    if duration_ms is not None:
        row.duration_ms = str(round(duration_ms, 3))
    row.save()
    return row


def create_tool_call(
    *, execution_id, agent_name, response_id, conversation_id, call_id, tool_id, tool_name, arguments
):
    return AIToolCallModel.create(
        execution_id=int(execution_id),
        agent_name=agent_name,
        response_id=response_id,
        conversation_id=conversation_id,
        call_id=call_id,
        tool_id=tool_id,
        tool_name=tool_name,
        arguments_json=json.dumps(arguments or {}, ensure_ascii=False, default=str),
        status="Pending",
    )


def get_tool_call(tool_call_id: int) -> AIToolCallModel:
    return AIToolCallModel.get_by_id(int(tool_call_id))


def update_tool_call(row, *, status=None, result=None, error=None, approval_id=None, ended=False):
    if status is not None:
        row.status = status
    if result is not None:
        row.result_json = json.dumps(result, ensure_ascii=False, default=str)
    if error is not None:
        row.error = error
    if approval_id is not None:
        row.approval_id = int(approval_id)
    if ended:
        row.ended_at = datetime.datetime.now()
    row.save()
    return row


def list_tool_calls(execution_id=None, limit=500):
    query = AIToolCallModel.select().order_by(AIToolCallModel.id.desc())
    if execution_id is not None:
        query = query.where(AIToolCallModel.execution_id == int(execution_id))
    return [row.as_dict() for row in query.limit(max(1, min(int(limit), 2000)))]


def create_approval_request(
    *,
    execution_id,
    tool_call_id,
    agent_name,
    tool_name,
    risk_level,
    approval_policy,
    request_data,
    requested_by=None,
    expires_at=None,
):
    return AIApprovalRequestModel.create(
        execution_id=int(execution_id),
        tool_call_id=int(tool_call_id),
        agent_name=agent_name,
        tool_name=tool_name,
        risk_level=risk_level or "review",
        approval_policy=approval_policy or "manual",
        status="Pending",
        request_json=json.dumps(request_data or {}, ensure_ascii=False, default=str),
        requested_by=requested_by,
        expires_at=expires_at,
    )


def get_approval_request(approval_id: int) -> AIApprovalRequestModel:
    return AIApprovalRequestModel.get_by_id(int(approval_id))


def list_approval_requests(status=None, execution_id=None, limit=500):
    query = AIApprovalRequestModel.select().order_by(AIApprovalRequestModel.id.desc())
    if status:
        query = query.where(AIApprovalRequestModel.status == status)
    if execution_id is not None:
        query = query.where(AIApprovalRequestModel.execution_id == int(execution_id))
    return [row.as_dict() for row in query.limit(max(1, min(int(limit), 2000)))]


def decide_approval_request(approval_id: int, decision: str, *, decided_by=None, note=None):
    row = get_approval_request(approval_id)
    normalized = str(decision or "").strip().lower()
    if normalized not in {"approved", "rejected"}:
        raise ValueError("decision must be Approved or Rejected")
    if row.status != "Pending":
        raise ValueError(f"approval request is already {row.status}")
    now = datetime.datetime.now()
    if row.expires_at and now > row.expires_at:
        error = "Foundry function-call approval window expired"
        row.status = "Expired"
        row.decided_at = now
        row.decided_by = decided_by
        row.decision_note = note or error
        row.save()

        tool_call = get_tool_call(row.tool_call_id)
        update_tool_call(
            tool_call,
            status="Expired",
            result={"ok": False, "error": error, "approval_id": row.id},
            error=error,
            ended=True,
        )
        execution = get_execution(row.execution_id)
        update_execution(
            execution,
            status="Expired",
            result={"phase": "expired", "error": error, "approval_id": row.id},
            ended=True,
        )
        raise ValueError("approval request has expired; re-run the agent task")
    row.status = "Approved" if normalized == "approved" else "Rejected"
    row.decided_by = decided_by
    row.decision_note = note
    row.decided_at = now
    row.save()
    return row
