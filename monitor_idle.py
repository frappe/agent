#!/usr/bin/env python
from __future__ import annotations

import json
import os
import sys
import time

import requests

if "--agent-directory" in sys.argv:
    AGENT_DIRECTORY = sys.argv[sys.argv.index("--agent-directory") + 1]
else:
    print("Error: --agent-directory argument required")
    sys.exit(1)

IDLE_THRESHOLD = 300  # After which the bench will be requested to die


def get_agent_config() -> dict[str, str | int]:
    """Get agent configuration"""
    with open(os.path.join(AGENT_DIRECTORY, "config.json")) as agent_config:
        return json.loads(agent_config.read())


def check_idle_bench(bench_path: str) -> bool:
    """Check if the modified time of the gunicorn access log is older than IDLE_THRESHOLD"""
    access_log = os.path.join(bench_path, "logs", "gunicorn.access.log")
    if not os.path.exists(access_log):
        return True

    current_time = time.time()
    last_modified = os.stat(access_log).st_mtime
    idle_time = current_time - last_modified

    return idle_time > IDLE_THRESHOLD


def inform_master(press_url: str, config: dict) -> None:
    """Let the master know of idle benches"""
    try:
        requests.post(
            f"{press_url}/press.api.server.benches_are_idle",
            data={"server": config["name"]},
            headers={"Authorization": f"Bearer {config['access_token']}"},
            timeout=10,
        )
        print(f"Informed master at {press_url} that benches are idle")
    except Exception as e:
        print(f"Error informing master: {e}")


def main() -> None:
    """Check if all benches are idle and let the master know"""
    config = get_agent_config()
    benches_are_idle = False
    benches_directory = config["benches_directory"]
    benches = os.scandir(benches_directory)

    for bench in benches:
        if not bench.name.startswith("bench-"):
            continue

        is_idle = check_idle_bench(bench.path)
        if not is_idle:
            print(f"Bench {bench.name} is still active")
            benches_are_idle = False
            break

        print(f"Bench {bench.name} is idle")
        benches_are_idle = True

    if benches_are_idle:
        inform_master(config["press_url"], config)
    else:
        print("Not all benches are idle, skipping master notification")


if __name__ == "__main__":
    main()
