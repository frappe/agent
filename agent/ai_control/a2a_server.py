"""Standards-compliant A2A v1.0 sidecar for the Alazab orchestration runtime.

The sidecar exposes one orchestrator Agent Card. Internal Foundry agents and
Copilot remain virtual/trusted participants managed by Agent AI Control Center.
"""

from __future__ import annotations

import asyncio
import os

from agent.ai_control.a2a_runtime import send_message


def _settings() -> tuple[str, int, str, str]:
    host = (os.environ.get("A2A_RUNTIME_HOST") or "127.0.0.1").strip()
    port = int(os.environ.get("A2A_RUNTIME_PORT") or "8766")
    rpc_path = (os.environ.get("A2A_RUNTIME_RPC_PATH") or "/api/v1/jsonrpc/").strip()
    if not rpc_path.startswith("/"):
        rpc_path = "/" + rpc_path
    if not rpc_path.endswith("/"):
        rpc_path += "/"
    public_url = (os.environ.get("A2A_PUBLIC_URL") or "").strip().rstrip("/")
    interface_url = f"{public_url}{rpc_path}" if public_url else f"http://{host}:{port}{rpc_path}"
    return host, port, rpc_path, interface_url


def build_app():
    try:
        from a2a.helpers import (
            get_message_text,
            new_task_from_user_message,
            new_text_artifact_update_event,
            new_text_status_update_event,
        )
        from a2a.server.agent_execution import AgentExecutor
        from a2a.server.request_handlers import DefaultRequestHandler
        from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
        from a2a.server.tasks import InMemoryTaskStore
        from a2a.types import AgentCapabilities, AgentCard, AgentInterface, AgentSkill, TaskState
        from starlette.applications import Starlette
    except ImportError as exc:
        raise RuntimeError(
            'A2A runtime dependencies are not installed. Install the Agent extra: pip install -e ".[ai-a2a]"'
        ) from exc

    host, port, rpc_path, interface_url = _settings()

    card = AgentCard(
        name="Alazab A2A Orchestrator",
        description="GPT-5.6 Sol orchestration layer for trusted Foundry agents and the Copilot Frappe/Bench participant.",
        supported_interfaces=[
            AgentInterface(
                protocol_binding="JSONRPC",
                protocol_version="1.0",
                url=interface_url,
            )
        ],
        version="1.0.0",
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain", "application/json"],
        capabilities=AgentCapabilities(streaming=True),
        skills=[
            AgentSkill(
                id="alazab-a2a-orchestration",
                name="Alazab A2A orchestration",
                description="Routes tasks through GPT-5.6 Sol to trusted Foundry agents and Copilot according to participant capabilities.",
                tags=["a2a", "foundry", "copilot", "frappe", "gpt-5.6-sol"],
                input_modes=["text/plain"],
                output_modes=["text/plain", "application/json"],
                examples=[
                    "Inspect the ERP state and ask the maintenance agent to assess the operational impact."
                ],
            )
        ],
    )

    class AlazabA2AExecutor(AgentExecutor):
        async def execute(self, context, event_queue) -> None:
            task = context.current_task or new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)
            await event_queue.enqueue_event(
                new_text_status_update_event(
                    task_id=task.id,
                    context_id=task.context_id,
                    state=TaskState.TASK_STATE_WORKING,
                    text="GPT-5.6 Sol is routing the A2A task.",
                )
            )
            message = get_message_text(context.message)
            result = await asyncio.to_thread(
                send_message,
                message,
                source="a2a-client",
                context_id=task.context_id,
            )
            output_text = result.get("output_text") or __import__("json").dumps(
                result, ensure_ascii=False, default=str
            )
            await event_queue.enqueue_event(
                new_text_artifact_update_event(
                    task_id=task.id,
                    context_id=task.context_id,
                    name="result",
                    text=output_text,
                )
            )
            await event_queue.enqueue_event(
                new_text_status_update_event(
                    task_id=task.id,
                    context_id=task.context_id,
                    state=TaskState.TASK_STATE_COMPLETED,
                    text="A2A orchestration completed.",
                )
            )

        async def cancel(self, context, event_queue) -> None:
            raise RuntimeError("A2A task cancellation is not implemented by this runtime")

    request_handler = DefaultRequestHandler(
        agent_executor=AlazabA2AExecutor(),
        task_store=InMemoryTaskStore(),
        agent_card=card,
    )
    routes = []
    routes.extend(create_agent_card_routes(card))
    routes.extend(create_jsonrpc_routes(request_handler, rpc_url=rpc_path))
    return Starlette(routes=routes)


def main():
    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError("uvicorn is required for the A2A sidecar") from exc
    host, port, _, _ = _settings()
    uvicorn.run(build_app(), host=host, port=port)


if __name__ == "__main__":
    main()
