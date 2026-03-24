# This script will let press know about all the running benches
# Request format: {"benches": ["bench1", "bench2"]}
from __future__ import annotations

import json
import subprocess

import requests

AGENT_CONFIG_PATH = "/home/frappe/agent/config.json"
PRESS_ENDPOINT = "/api/method/press.api.bench.identify_zombie_benches"


def get_credentials() -> tuple[str, str, str]:
    """Get server, agent token and press url from config"""
    with open(AGENT_CONFIG_PATH) as f:
        config = json.load(f)

    return config["name"], config["access_token"], config.get("press_url", "https://cloud.frappe.io")


def get_running_benches() -> list[str]:
    """List all running benches"""
    result = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True)
    return result.stdout.strip().split("\n")


def main():
    server, agent_token, press_url = get_credentials()
    benches = get_running_benches()

    if not benches:
        return

    requests.post(
        f"{press_url}{PRESS_ENDPOINT}",
        headers={
            "Content-Type": "application/json",
            "server": server,
            "agent-token": agent_token,
        },
        json={"benches": benches},
    )


if __name__ == "__main__":
    main()
