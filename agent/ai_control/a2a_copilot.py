"""Signed internal A2A client for the Copilot Frappe/Bench participant."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Any
from urllib.parse import urljoin

import requests

A2A_INTERNAL_PROTOCOL = "alazab-a2a-internal/v1"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


class CopilotA2AConfiguration:
    def __init__(self, base_url: str, secret: str, timeout: float = 120.0):
        self.base_url = base_url.rstrip("/")
        self.secret = secret
        self.timeout = timeout

    @classmethod
    def from_environment(cls):
        base_url = (
            os.environ.get("A2A_COPILOT_BASE_URL") or os.environ.get("COPILOT_CHANNEL_BASE_URL") or ""
        ).strip()
        secret = (os.environ.get("ALAZAB_AGENT_CHANNEL_SECRET") or "").strip()
        timeout = float(
            os.environ.get("A2A_COPILOT_TIMEOUT") or os.environ.get("COPILOT_CHANNEL_TIMEOUT") or "120"
        )
        if not base_url:
            raise RuntimeError("A2A_COPILOT_BASE_URL is not configured")
        if not secret:
            raise RuntimeError("ALAZAB_AGENT_CHANNEL_SECRET is not configured")
        return cls(base_url=base_url, secret=secret, timeout=timeout)

    @property
    def dispatch_url(self) -> str:
        return urljoin(self.base_url + "/", "api/method/copilot.api.a2a.dispatch")


class CopilotA2AClient:
    def __init__(self, config: CopilotA2AConfiguration | None = None):
        self.config = config or CopilotA2AConfiguration.from_environment()

    def _headers(self, payload: dict[str, Any]) -> dict[str, str]:
        timestamp = str(int(time.time()))
        message = f"{timestamp}.{_canonical_json(payload)}".encode()
        signature = hmac.new(self.config.secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
        return {
            "Content-Type": "application/json",
            "X-Alazab-Timestamp": timestamp,
            "X-Alazab-Signature": signature,
        }

    def dispatch(
        self, action: str, arguments: dict[str, Any] | None = None, request_id: str | None = None
    ) -> dict[str, Any]:
        payload = {
            "protocol": A2A_INTERNAL_PROTOCOL,
            "request_id": request_id or str(uuid.uuid4()),
            "action": action,
            "arguments": arguments or {},
            "origin": "agent-a2a",
        }
        response = requests.post(
            self.config.dispatch_url,
            json={"payload": payload},
            headers=self._headers(payload),
            timeout=self.config.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict) and "message" in data:
            data = data["message"]
        if not isinstance(data, dict):
            raise RuntimeError("Copilot A2A endpoint returned a non-object response")
        if data.get("ok") is False:
            error = data.get("error") or {}
            raise RuntimeError(str(error.get("message") or error or "Copilot A2A request failed"))
        return data

    def get_card(self) -> dict[str, Any]:
        return self.dispatch("card.get").get("result") or {}

    def send_message(
        self,
        message: str,
        context_id: str,
        task_id: str,
        source: str,
        configuration: str,
    ) -> dict[str, Any]:
        return (
            self.dispatch(
                "message.send",
                {
                    "message": message,
                    "context_id": context_id,
                    "task_id": task_id,
                    "source": source,
                    "configuration": configuration,
                },
            ).get("result")
            or {}
        )
