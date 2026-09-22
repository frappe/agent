"""Operator CLI for local A2A integration checks."""

from __future__ import annotations

import argparse
import json

from agent.ai_control.a2a_runtime import send_message, status, sync_participants, tasks
from agent.ai_control.store import list_a2a_participants


def _print(value):
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def main():
    parser = argparse.ArgumentParser(prog="agent-ai-a2a-check")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    sub.add_parser("sync")
    sub.add_parser("participants")
    p_tasks = sub.add_parser("tasks")
    p_tasks.add_argument("--limit", type=int, default=20)
    p_send = sub.add_parser("send")
    p_send.add_argument("message")
    p_send.add_argument("--context-id")
    args = parser.parse_args()

    if args.command == "status":
        _print(status())
    elif args.command == "sync":
        _print(sync_participants())
    elif args.command == "participants":
        _print(list_a2a_participants())
    elif args.command == "tasks":
        _print(tasks(limit=args.limit))
    elif args.command == "send":
        _print(send_message(args.message, source="operator-cli", context_id=args.context_id))


if __name__ == "__main__":
    main()
