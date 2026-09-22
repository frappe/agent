from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class FoundryConfiguration:
    project_endpoint: str
    project_name: str | None = None

    @classmethod
    def from_environment(cls):
        endpoint = (os.environ.get("FOUNDRY_PROJECT_ENDPOINT") or "").strip().rstrip("/")
        if not endpoint:
            raise RuntimeError("FOUNDRY_PROJECT_ENDPOINT is not configured")
        project_name = endpoint.rsplit("/", 1)[-1] if "/api/projects/" in endpoint else None
        return cls(project_endpoint=endpoint, project_name=project_name)


class FoundryClient:
    """Thin adapter around Microsoft Foundry SDK and its project-scoped OpenAI client."""

    def __init__(self, config: FoundryConfiguration | None = None):
        self.config = config or FoundryConfiguration.from_environment()

    def _project(self):
        try:
            from azure.ai.projects import AIProjectClient
            from azure.identity import DefaultAzureCredential
        except ImportError as exc:
            raise RuntimeError(
                "Foundry SDK dependencies are not installed: azure-ai-projects and azure-identity"
            ) from exc
        return AIProjectClient(
            endpoint=self.config.project_endpoint,
            credential=DefaultAzureCredential(),
        )

    @staticmethod
    def _to_dict(value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if hasattr(value, "as_dict"):
            return value.as_dict()
        if hasattr(value, "model_dump"):
            return value.model_dump()
        if hasattr(value, "to_dict"):
            return value.to_dict()
        if hasattr(value, "__dict__"):
            return {k: v for k, v in value.__dict__.items() if not k.startswith("_")}
        return {"value": str(value)}

    def list_agents(self) -> list[dict[str, Any]]:
        project = self._project()
        return [self._to_dict(x) for x in project.agents.list()]

    def list_deployments(self) -> list[dict[str, Any]]:
        project = self._project()
        return [self._to_dict(x) for x in project.deployments.list()]

    def list_toolboxes(self) -> list[dict[str, Any]]:
        project = self._project()
        return [self._to_dict(x) for x in project.toolboxes.list()]

    def list_connections(self) -> list[dict[str, Any]]:
        project = self._project()
        return [self._to_dict(x) for x in project.connections.list()]

    def set_agent_enabled(self, agent_name: str, enabled: bool) -> dict[str, Any]:
        project = self._project()
        if enabled:
            project.agents.enable(agent_name)
        else:
            project.agents.disable(agent_name)
        return {"agent": agent_name, "enabled": enabled}

    def chat_with_agent(
        self,
        agent_name: str,
        input_text: str,
        conversation_id: str | None = None,
    ) -> dict[str, Any]:
        """Use Foundry Conversations + Responses against a persisted Foundry agent."""
        project = self._project()
        openai = project.get_openai_client(agent_name=agent_name)
        if not conversation_id:
            conversation = openai.conversations.create()
            conversation_id = conversation.id
        response = openai.responses.create(
            conversation=conversation_id,
            input=input_text,
        )
        usage = self._to_dict(getattr(response, "usage", None))
        return {
            "agent_name": agent_name,
            "conversation_id": conversation_id,
            "response_id": getattr(response, "id", None),
            "output_text": getattr(response, "output_text", "") or "",
            "usage": usage,
        }

    def responses_with_tools(
        self,
        model: str,
        input_items: Any,
        tools: list[dict[str, Any]],
        instructions: str | None = None,
        previous_response_id: str | None = None,
    ) -> dict[str, Any]:
        """Run one project-scoped Responses API turn with explicit function tools.

        This adapter does not choose a model, tool, token limit, or routing policy.
        The caller supplies the exact deployment and the complete tool contract.
        """
        deployment = str(model or "").strip()
        if not deployment:
            raise ValueError("model deployment is required")
        project = self._project()
        openai = project.get_openai_client()
        kwargs: dict[str, Any] = {
            "model": deployment,
            "input": input_items,
            "tools": tools,
        }
        if instructions:
            kwargs["instructions"] = instructions
        if previous_response_id:
            kwargs["previous_response_id"] = previous_response_id
        response = openai.responses.create(**kwargs)

        outputs: list[dict[str, Any]] = []
        for item in getattr(response, "output", None) or []:
            data = self._to_dict(item)
            # Keep the fields needed by the orchestration loop stable even if
            # the SDK's nested serialization shape changes.
            for field in ("type", "name", "call_id", "arguments", "id", "status"):
                value = getattr(item, field, None)
                if value is not None:
                    data[field] = value
            outputs.append(data)

        return {
            "id": getattr(response, "id", None),
            "model": getattr(response, "model", deployment),
            "output_text": getattr(response, "output_text", "") or "",
            "output": outputs,
            "usage": self._to_dict(getattr(response, "usage", None)),
            "status": getattr(response, "status", None),
        }

    def get_agent(self, agent_name: str) -> dict[str, Any]:
        project = self._project()
        return self._to_dict(project.agents.get(agent_name=agent_name))

    def get_agent_version(self, agent_name: str, agent_version: str) -> dict[str, Any]:
        project = self._project()
        return self._to_dict(
            project.agents.get_version(agent_name=agent_name, agent_version=str(agent_version))
        )

    @staticmethod
    def _latest_version_id(agent: Any) -> str:
        versions = getattr(agent, "versions", None)
        latest = None
        if isinstance(versions, dict):
            latest = versions.get("latest")
        elif versions is not None:
            latest = getattr(versions, "latest", None)
        if isinstance(latest, dict):
            value = latest.get("version") or latest.get("id")
        else:
            value = getattr(latest, "version", None) or getattr(latest, "id", None)
        if value is None:
            raise RuntimeError("Foundry agent latest version could not be resolved")
        return str(value).rsplit(":", 1)[-1]

    @staticmethod
    def _tool_name(tool: Any) -> str | None:
        if isinstance(tool, dict):
            value = tool.get("name")
            if value:
                return str(value)
            function = tool.get("function")
            if isinstance(function, dict) and function.get("name"):
                return str(function["name"])
            return None
        value = getattr(tool, "name", None)
        if value:
            return str(value)
        function = getattr(tool, "function", None)
        value = getattr(function, "name", None) if function is not None else None
        return str(value) if value else None

    def list_agent_tools(self, agent_name: str) -> list[dict[str, Any]]:
        project = self._project()
        record = project.agents.get(agent_name=agent_name)
        version_id = self._latest_version_id(record)
        version = project.agents.get_version(agent_name=agent_name, agent_version=version_id)
        definition = getattr(version, "definition", None)
        tools = getattr(definition, "tools", None) or []
        return [self._to_dict(tool) for tool in tools]

    def sync_agent_function_tools(
        self,
        agent_name: str,
        function_tools: list[dict[str, Any]],
        *,
        managed_prefix: str = "agent_tool_",
    ) -> dict[str, Any]:
        """Publish one new Foundry agent version with Agent-managed function tools.

        Existing non-Agent tools are preserved. Previously published Agent-managed
        function tools are replaced, making the operation idempotent at the
        logical configuration level while retaining Foundry version history.
        """
        try:
            from azure.ai.projects.models import FunctionTool
        except ImportError as exc:
            raise RuntimeError("azure-ai-projects with FunctionTool support is required") from exc

        project = self._project()
        record = project.agents.get(agent_name=agent_name)
        version_id = self._latest_version_id(record)
        version = project.agents.get_version(agent_name=agent_name, agent_version=version_id)
        definition = copy.deepcopy(getattr(version, "definition", None))
        if definition is None:
            raise RuntimeError(f"Foundry agent '{agent_name}' has no version definition")
        if not hasattr(definition, "tools"):
            raise RuntimeError(
                f"Foundry agent '{agent_name}' definition does not support function tools"
            )

        existing = list(getattr(definition, "tools", None) or [])
        retained = [
            tool
            for tool in existing
            if not str(self._tool_name(tool) or "").startswith(managed_prefix)
        ]

        managed = []
        for spec in function_tools:
            name = str(spec.get("name") or "").strip()
            if not name.startswith(managed_prefix):
                raise ValueError(f"Managed function tool name must start with '{managed_prefix}'")
            parameters = spec.get("parameters") or {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            }
            managed.append(
                FunctionTool(
                    name=name,
                    parameters=parameters,
                    description=str(spec.get("description") or name),
                    strict=bool(spec.get("strict", True)),
                )
            )

        definition.tools = [*retained, *managed]
        created = project.agents.create_version(
            agent_name=agent_name,
            definition=definition,
        )
        data = self._to_dict(created)
        return {
            "agent_name": agent_name,
            "previous_version": version_id,
            "new_version": data.get("version") or data.get("id"),
            "managed_tool_count": len(managed),
            "preserved_tool_count": len(retained),
            "agent_version": data,
        }

    def _serialize_response(self, response: Any, *, agent_name: str, conversation_id: str) -> dict[str, Any]:
        outputs: list[dict[str, Any]] = []
        for item in getattr(response, "output", None) or []:
            data = self._to_dict(item)
            for field in ("type", "name", "call_id", "arguments", "id", "status"):
                value = getattr(item, field, None)
                if value is not None:
                    data[field] = value
            outputs.append(data)
        model_extra = getattr(response, "model_extra", None) or {}
        return {
            "agent_name": agent_name,
            "conversation_id": conversation_id,
            "response_id": getattr(response, "id", None),
            "output_text": getattr(response, "output_text", "") or "",
            "output": outputs,
            "usage": self._to_dict(getattr(response, "usage", None)),
            "status": getattr(response, "status", None),
            "agent_session_id": model_extra.get("agent_session_id") if isinstance(model_extra, dict) else None,
        }

    def invoke_agent(
        self,
        agent_name: str,
        input_items: Any,
        *,
        conversation_id: str | None = None,
    ) -> dict[str, Any]:
        """Invoke a persisted Foundry agent over its dedicated Responses endpoint."""
        project = self._project()
        openai = project.get_openai_client(agent_name=agent_name)
        if not conversation_id:
            conversation = openai.conversations.create()
            conversation_id = conversation.id
        response = openai.responses.create(
            conversation=conversation_id,
            input=input_items,
        )
        return self._serialize_response(
            response,
            agent_name=agent_name,
            conversation_id=conversation_id,
        )

    def submit_tool_outputs(
        self,
        agent_name: str,
        conversation_id: str,
        outputs: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not conversation_id:
            raise ValueError("conversation_id is required")
        if not outputs:
            raise ValueError("at least one function_call_output is required")
        project = self._project()
        openai = project.get_openai_client(agent_name=agent_name)
        response = openai.responses.create(
            conversation=conversation_id,
            input=outputs,
        )
        return self._serialize_response(
            response,
            agent_name=agent_name,
            conversation_id=conversation_id,
        )

    def create_vector_store(self, name: str) -> dict[str, Any]:
        project = self._project()
        openai = project.get_openai_client()
        vector_store = openai.vector_stores.create(name=name)
        return self._to_dict(vector_store)

    def list_vector_stores(self) -> list[dict[str, Any]]:
        project = self._project()
        openai = project.get_openai_client()
        result = openai.vector_stores.list()
        return [self._to_dict(x) for x in getattr(result, "data", result)]

    def publish_file_to_vector_store(self, path: str | Path, vector_store_id: str) -> dict[str, Any]:
        project = self._project()
        openai = project.get_openai_client()
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(str(file_path))
        with file_path.open("rb") as handle:
            uploaded = openai.vector_stores.files.upload_and_poll(
                vector_store_id=vector_store_id,
                file=handle,
            )
        data = self._to_dict(uploaded)
        return {
            "vector_store_id": vector_store_id,
            "file": data,
            "file_id": data.get("file_id") or data.get("id"),
        }

    def capability_summary(self) -> dict[str, Any]:
        return {
            "project_endpoint": self.config.project_endpoint,
            "project_name": self.config.project_name,
            "adapter_implemented": {
                "agents": True,
                "deployments": True,
                "connections": True,
                "toolboxes": True,
                "responses": True,
                "conversations": True,
                "function_tools": True,
                "tool_outputs": True,
                "file_search": True,
                "vector_stores": True,
            },
            "integration_registry_types": ["mcp", "a2a", "api", "webhook", "ai_tool"],
            "note": "Implemented/configured metadata only. Live connectivity is established by sync/test/channel operations.",
        }
