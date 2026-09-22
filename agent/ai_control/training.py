from __future__ import annotations

import datetime
import json
from typing import Any

from agent.ai_control.foundry import FoundryClient
from agent.ai_control.models import AITrainingMessageModel, AITrainingSessionModel


def list_sessions(agent_name: str | None = None):
    query = AITrainingSessionModel.select().order_by(AITrainingSessionModel.modified_at.desc())
    if agent_name:
        query = query.where(AITrainingSessionModel.agent_name == agent_name)
    return [row.as_dict() for row in query]


def get_session(session_id: int) -> AITrainingSessionModel:
    return AITrainingSessionModel.get_by_id(session_id)


def create_session(payload: dict[str, Any]) -> AITrainingSessionModel:
    agent_name = str(payload.get("agent_name") or "").strip()
    if not agent_name:
        raise ValueError("agent_name is required")
    title = str(payload.get("title") or f"Training · {agent_name}").strip()
    return AITrainingSessionModel.create(
        agent_name=agent_name,
        title=title,
        configuration_ref=payload.get("configuration_ref"),
        metadata_json=json.dumps(payload.get("metadata") or {}, ensure_ascii=False, default=str),
    )


def list_messages(session_id: int):
    query = (
        AITrainingMessageModel.select()
        .where(AITrainingMessageModel.session_id == session_id)
        .order_by(AITrainingMessageModel.id)
    )
    return [row.as_dict() for row in query]


def add_message(session_id: int, role: str, content: str, response_id: str | None = None, metadata=None):
    return AITrainingMessageModel.create(
        session_id=session_id,
        role=role,
        content=content,
        response_id=response_id,
        metadata_json=json.dumps(metadata or {}, ensure_ascii=False, default=str),
    )


def send_training_message(session_id: int, content: str) -> dict[str, Any]:
    session = get_session(session_id)
    content = str(content or "").strip()
    if not content:
        raise ValueError("message content is required")

    user_message = add_message(session.id, "user", content)
    foundry = FoundryClient()
    result = foundry.chat_with_agent(
        agent_name=session.agent_name,
        input_text=content,
        conversation_id=session.conversation_id,
    )

    if not session.conversation_id and result.get("conversation_id"):
        session.conversation_id = result["conversation_id"]
    session.modified_at = datetime.datetime.now()
    session.save()

    assistant_message = add_message(
        session.id,
        "assistant",
        result.get("output_text") or "",
        response_id=result.get("response_id"),
        metadata={
            "conversation_id": result.get("conversation_id"),
            "usage": result.get("usage"),
        },
    )
    return {
        "session": session.as_dict(),
        "user": user_message.as_dict(),
        "assistant": assistant_message.as_dict(),
        "runtime": result,
    }
