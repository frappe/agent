from __future__ import annotations

import datetime
import json
from typing import Any

from peewee import AutoField, CharField, DateTimeField, IntegerField, Model, TextField

from agent.job import agent_database


class AIControlModel(Model):
    class Meta:
        database = agent_database


def _decode_json(value: str | None, fallback=None):
    if fallback is None:
        fallback = {}
    try:
        return json.loads(value or "{}")
    except json.JSONDecodeError:
        return fallback


class AIIntegrationModel(AIControlModel):
    """A protocol/integration endpoint managed by Agent AI Control Center."""

    id = AutoField()
    name = CharField(unique=True)
    integration_type = CharField(index=True)  # mcp | a2a | api | webhook | ai_tool
    status = CharField(default="Draft", index=True)
    project = CharField(null=True, index=True)
    endpoint = TextField(null=True)
    auth_type = CharField(default="none")
    secret_ref = CharField(null=True)
    config = TextField(default="{}")
    created_at = DateTimeField(default=datetime.datetime.now)
    modified_at = DateTimeField(default=datetime.datetime.now)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.integration_type,
            "status": self.status,
            "project": self.project,
            "endpoint": self.endpoint,
            "auth_type": self.auth_type,
            "secret_ref": self.secret_ref,
            "config": _decode_json(self.config),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


class FoundryAssetModel(AIControlModel):
    """Local inventory/cache of Foundry assets. Foundry remains source of truth."""

    id = AutoField()
    project = CharField(index=True)
    asset_type = CharField(index=True)  # agent | model | tool | toolbox | connection
    external_id = CharField(null=True)
    name = CharField(index=True)
    source = CharField(default="Foundry")
    status = CharField(null=True)
    version = CharField(null=True)
    metadata_json = TextField(default="{}")
    last_synced_at = DateTimeField(default=datetime.datetime.now)

    class Meta:
        database = agent_database
        indexes = ((("project", "asset_type", "name"), True),)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "project": self.project,
            "type": self.asset_type,
            "external_id": self.external_id,
            "name": self.name,
            "source": self.source,
            "status": self.status,
            "version": self.version,
            "metadata": _decode_json(self.metadata_json),
            "last_synced_at": self.last_synced_at.isoformat() if self.last_synced_at else None,
        }


class AIBindingModel(AIControlModel):
    """Graph edge between an Agent/asset and a model, integration, tool, or knowledge source."""

    id = AutoField()
    source_type = CharField(index=True)
    source_ref = CharField(index=True)
    target_type = CharField(index=True)
    target_ref = CharField(index=True)
    status = CharField(default="Active", index=True)
    config = TextField(default="{}")
    created_at = DateTimeField(default=datetime.datetime.now)
    modified_at = DateTimeField(default=datetime.datetime.now)

    class Meta:
        database = agent_database
        indexes = ((("source_type", "source_ref", "target_type", "target_ref"), True),)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_type": self.source_type,
            "source_ref": self.source_ref,
            "target_type": self.target_type,
            "target_ref": self.target_ref,
            "status": self.status,
            "config": _decode_json(self.config),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


class AIExecutionModel(AIControlModel):
    """Audit/observation record for UI actions and protocol/tool operations."""

    id = AutoField()
    action = CharField(index=True)
    resource_type = CharField(index=True)
    resource_id = CharField(null=True)
    status = CharField(default="Pending", index=True)
    request_json = TextField(default="{}")
    result_json = TextField(default="{}")
    started_at = DateTimeField(default=datetime.datetime.now)
    ended_at = DateTimeField(null=True)
    duration_ms = CharField(null=True)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "action": self.action,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "status": self.status,
            "request": _decode_json(self.request_json),
            "result": _decode_json(self.result_json),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_ms": self.duration_ms,
        }


class AIKnowledgeSourceModel(AIControlModel):
    """Original and processed knowledge artefact with full provenance."""

    id = AutoField()
    title = CharField(index=True)
    original_filename = CharField()
    stored_filename = CharField()
    mime_type = CharField(null=True)
    extension = CharField(null=True, index=True)
    size_bytes = IntegerField(default=0)
    sha256 = CharField(index=True)
    status = CharField(default="Uploaded", index=True)
    inspection_status = CharField(default="Pending", index=True)
    approval_status = CharField(default="Pending", index=True)
    extract_method = CharField(null=True)
    raw_path = TextField()
    extracted_path = TextField(null=True)
    clean_path = TextField(null=True)
    document_path = TextField(null=True)
    char_count = IntegerField(default=0)
    line_count = IntegerField(default=0)
    metadata_json = TextField(default="{}")
    foundry_file_id = CharField(null=True)
    vector_store_id = CharField(null=True, index=True)
    created_at = DateTimeField(default=datetime.datetime.now)
    modified_at = DateTimeField(default=datetime.datetime.now)

    def as_dict(self, include_paths: bool = False) -> dict[str, Any]:
        data = {
            "id": self.id,
            "title": self.title,
            "original_filename": self.original_filename,
            "mime_type": self.mime_type,
            "extension": self.extension,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "status": self.status,
            "inspection_status": self.inspection_status,
            "approval_status": self.approval_status,
            "extract_method": self.extract_method,
            "char_count": self.char_count,
            "line_count": self.line_count,
            "metadata": _decode_json(self.metadata_json),
            "foundry_file_id": self.foundry_file_id,
            "vector_store_id": self.vector_store_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }
        if include_paths:
            data["paths"] = {
                "raw": self.raw_path,
                "extracted": self.extracted_path,
                "clean": self.clean_path,
                "document": self.document_path,
            }
        return data


class AITrainingSessionModel(AIControlModel):
    id = AutoField()
    agent_name = CharField(index=True)
    title = CharField()
    status = CharField(default="Active", index=True)
    conversation_id = CharField(null=True, index=True)
    configuration_ref = CharField(null=True)
    metadata_json = TextField(default="{}")
    created_at = DateTimeField(default=datetime.datetime.now)
    modified_at = DateTimeField(default=datetime.datetime.now)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "agent_name": self.agent_name,
            "title": self.title,
            "status": self.status,
            "conversation_id": self.conversation_id,
            "configuration_ref": self.configuration_ref,
            "metadata": _decode_json(self.metadata_json),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


class AITrainingMessageModel(AIControlModel):
    id = AutoField()
    session_id = IntegerField(index=True)
    role = CharField(index=True)
    content = TextField()
    response_id = CharField(null=True)
    metadata_json = TextField(default="{}")
    created_at = DateTimeField(default=datetime.datetime.now)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "session_id": self.session_id,
            "role": self.role,
            "content": self.content,
            "response_id": self.response_id,
            "metadata": _decode_json(self.metadata_json),
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class AIConfigurationModel(AIControlModel):
    id = AutoField()
    name = CharField(unique=True)
    config_type = CharField(index=True)  # runtime | model | behavior | knowledge | tools | safety
    scope_type = CharField(default="global", index=True)  # global | agent | tool | integration
    scope_ref = CharField(null=True, index=True)
    status = CharField(default="Draft", index=True)
    version = IntegerField(default=1)
    config_json = TextField(default="{}")
    secret_ref = CharField(null=True)
    created_at = DateTimeField(default=datetime.datetime.now)
    modified_at = DateTimeField(default=datetime.datetime.now)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "config_type": self.config_type,
            "scope_type": self.scope_type,
            "scope_ref": self.scope_ref,
            "status": self.status,
            "version": self.version,
            "config": _decode_json(self.config_json),
            "secret_ref": self.secret_ref,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


class AIProductionToolModel(AIControlModel):
    id = AutoField()
    name = CharField(unique=True)
    display_name = CharField()
    category = CharField(index=True)
    source = CharField(default="Agent", index=True)
    route = TextField(null=True)
    methods_json = TextField(default="[]")
    status = CharField(default="Discovered", index=True)
    approval_policy = CharField(default="manual", index=True)
    risk_level = CharField(default="review", index=True)
    config_json = TextField(default="{}")
    last_test_status = CharField(null=True)
    last_tested_at = DateTimeField(null=True)
    created_at = DateTimeField(default=datetime.datetime.now)
    modified_at = DateTimeField(default=datetime.datetime.now)

    def as_dict(self) -> dict[str, Any]:
        try:
            methods = json.loads(self.methods_json or "[]")
        except json.JSONDecodeError:
            methods = []
        return {
            "id": self.id,
            "name": self.name,
            "display_name": self.display_name,
            "category": self.category,
            "source": self.source,
            "route": self.route,
            "methods": methods,
            "status": self.status,
            "approval_policy": self.approval_policy,
            "risk_level": self.risk_level,
            "config": _decode_json(self.config_json),
            "last_test_status": self.last_test_status,
            "last_tested_at": self.last_tested_at.isoformat() if self.last_tested_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


class AIToolCallModel(AIControlModel):
    """Audit record for one client-side function call requested by a Foundry agent."""

    id = AutoField()
    execution_id = IntegerField(index=True)
    agent_name = CharField(index=True)
    response_id = CharField(null=True, index=True)
    conversation_id = CharField(null=True, index=True)
    call_id = CharField(index=True)
    tool_id = IntegerField(null=True, index=True)
    tool_name = CharField(index=True)
    arguments_json = TextField(default="{}")
    status = CharField(default="Pending", index=True)
    approval_id = IntegerField(null=True, index=True)
    result_json = TextField(default="{}")
    error = TextField(null=True)
    started_at = DateTimeField(default=datetime.datetime.now)
    ended_at = DateTimeField(null=True)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "execution_id": self.execution_id,
            "agent_name": self.agent_name,
            "response_id": self.response_id,
            "conversation_id": self.conversation_id,
            "call_id": self.call_id,
            "tool_id": self.tool_id,
            "tool_name": self.tool_name,
            "arguments": _decode_json(self.arguments_json),
            "status": self.status,
            "approval_id": self.approval_id,
            "result": _decode_json(self.result_json),
            "error": self.error,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
        }


class AIApprovalRequestModel(AIControlModel):
    """Human approval gate for a sensitive Foundry-requested Agent tool call."""

    id = AutoField()
    execution_id = IntegerField(index=True)
    tool_call_id = IntegerField(index=True)
    agent_name = CharField(index=True)
    tool_name = CharField(index=True)
    risk_level = CharField(default="review", index=True)
    approval_policy = CharField(default="manual", index=True)
    status = CharField(default="Pending", index=True)
    request_json = TextField(default="{}")
    requested_by = CharField(null=True)
    decided_by = CharField(null=True)
    decision_note = TextField(null=True)
    created_at = DateTimeField(default=datetime.datetime.now)
    expires_at = DateTimeField(null=True, index=True)
    decided_at = DateTimeField(null=True)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "execution_id": self.execution_id,
            "tool_call_id": self.tool_call_id,
            "agent_name": self.agent_name,
            "tool_name": self.tool_name,
            "risk_level": self.risk_level,
            "approval_policy": self.approval_policy,
            "status": self.status,
            "request": _decode_json(self.request_json),
            "requested_by": self.requested_by,
            "decided_by": self.decided_by,
            "decision_note": self.decision_note,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "decided_at": self.decided_at.isoformat() if self.decided_at else None,
        }


class A2AParticipantModel(AIControlModel):
    """One participant in the local Alazab A2A network."""

    id = AutoField()
    name = CharField(unique=True)
    participant_type = CharField(index=True)  # foundry_agent | copilot | remote_a2a
    status = CharField(default="Active", index=True)
    source_ref = CharField(null=True, index=True)
    endpoint = TextField(null=True)
    agent_card_json = TextField(default="{}")
    config_json = TextField(default="{}")
    last_seen_at = DateTimeField(null=True)
    created_at = DateTimeField(default=datetime.datetime.now)
    modified_at = DateTimeField(default=datetime.datetime.now)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "participant_type": self.participant_type,
            "status": self.status,
            "source_ref": self.source_ref,
            "endpoint": self.endpoint,
            "agent_card": _decode_json(self.agent_card_json),
            "config": _decode_json(self.config_json),
            "last_seen_at": self.last_seen_at.isoformat() if self.last_seen_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


class A2AContextModel(AIControlModel):
    """Maps one A2A context to a participant-specific remote conversation."""

    id = AutoField()
    context_id = CharField(index=True)
    participant_name = CharField(index=True)
    remote_context_id = CharField(null=True)
    metadata_json = TextField(default="{}")
    created_at = DateTimeField(default=datetime.datetime.now)
    modified_at = DateTimeField(default=datetime.datetime.now)

    class Meta:
        database = agent_database
        indexes = ((("context_id", "participant_name"), True),)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "context_id": self.context_id,
            "participant_name": self.participant_name,
            "remote_context_id": self.remote_context_id,
            "metadata": _decode_json(self.metadata_json),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


class A2ATaskModel(AIControlModel):
    """Persistent task/audit record for delegations in the local A2A network."""

    id = AutoField()
    task_id = CharField(unique=True, index=True)
    context_id = CharField(index=True)
    source = CharField(index=True)
    target = CharField(null=True, index=True)
    state = CharField(default="TASK_STATE_SUBMITTED", index=True)
    input_json = TextField(default="{}")
    result_json = TextField(default="{}")
    trace_json = TextField(default="[]")
    error = TextField(null=True)
    started_at = DateTimeField(default=datetime.datetime.now)
    ended_at = DateTimeField(null=True)
    modified_at = DateTimeField(default=datetime.datetime.now)

    def as_dict(self) -> dict[str, Any]:
        try:
            trace = json.loads(self.trace_json or "[]")
        except json.JSONDecodeError:
            trace = []
        return {
            "id": self.id,
            "task_id": self.task_id,
            "context_id": self.context_id,
            "source": self.source,
            "target": self.target,
            "state": self.state,
            "input": _decode_json(self.input_json),
            "result": _decode_json(self.result_json),
            "trace": trace,
            "error": self.error,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


AI_CONTROL_TABLES = [
    AIIntegrationModel,
    FoundryAssetModel,
    AIBindingModel,
    AIExecutionModel,
    AIKnowledgeSourceModel,
    AITrainingSessionModel,
    AITrainingMessageModel,
    AIConfigurationModel,
    AIProductionToolModel,
    AIToolCallModel,
    AIApprovalRequestModel,
    A2AParticipantModel,
    A2AContextModel,
    A2ATaskModel,
]
