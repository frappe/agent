#!/usr/bin/env python
# Inform master in case a bench has not servered any request for
# the last <idle_threshold> duration; In case access log is not found
# assume that the bench is being prepared

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field

import requests


@dataclass
class IdleMonitor:
    agent_directory: str
    check_interval: int = 5
    idle_threshold: int = 300
    shared_benches_directory: str = "/home/frappe/shared"
    config: dict[str] = field(default_factory=dict)

    def __post_init__(self):
        self.config = self.get_config()

    def get_config(self) -> dict[str, str | int]:
        """Read only agent config"""
        with open(os.path.join(self.agent_directory, "config.json")) as agent_config:
            return json.loads(agent_config.read())

    def check_idle_slave(self, bench_path: str) -> bool:
        """Check if the modified time of the gunicorn access log is older than IDLE_THRESHOLD"""
        server_name = self.config["name"]
        access_log = os.path.join(bench_path, "logs", f"{server_name}-gunicorn.access.log")
        if not os.path.exists(access_log):
            # We might be waiting to recieve requests from proxy
            # We might also be waiting for a new bench job to finish
            return False

        current_time = time.time()
        last_modified = os.stat(access_log).st_mtime
        idle_time = current_time - last_modified

        return idle_time > self.idle_threshold

    def inform_master(self) -> None:
        """Let the master know of idle benches"""
        try:
            requests.post(
                f"{self.config['press_url']}/api/method/press.api.server.benches_are_idle",
                data={"server": self.config["name"], "access_token": self.config["access_token"]},
                timeout=10,
            )
            print(f"Informed master at {self.config['press_url']} that benches are idle")
        except Exception as e:
            print(f"Error informing master: {e}")

    def should_monitor(self):
        """Ensure this never runs on a primary server as it kills slaves"""
        print("Not running idle monitor relying on prometheus for this.")
        sys.exit(1)

    @property
    def boot_time(self) -> int:
        """Get boot time of the server"""
        with open("/proc/uptime") as f:
            return float(f.read().split()[0])

    def monitor(self) -> None:
        self.should_monitor()
        benches_are_idle = False
        benches_directory = "/home/frappe/shared"  # This is where benches are on secondary server!
        benches = list(os.scandir(benches_directory))

        while True:
            for bench in benches:
                if not bench.name.startswith("bench-"):
                    continue

                is_idle = self.check_idle_slave(bench.path)
                if not is_idle:
                    print(f"Bench {bench.name} is active")
                    benches_are_idle = False
                    break

                print(f"Bench {bench.name} is idle")
                benches_are_idle = True

            # ensure that server has been up for long enough
            if benches_are_idle and self.boot_time > self.idle_threshold:
                self.inform_master()
            else:
                print("Not all benches are idle, skipping master notification")

            time.sleep(self.check_interval)


if __name__ == "__main__":
    if "--agent-directory" in sys.argv:
        agent_directory = sys.argv[sys.argv.index("--agent-directory") + 1]
    else:
        print("No agent directory supplied!")
        sys.exit(1)

    IdleMonitor(
        agent_directory=agent_directory,
        check_interval=5,
        idle_threshold=300,
    ).monitor()
