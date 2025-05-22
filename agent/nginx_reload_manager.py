from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from datetime import datetime
from typing import Literal

import redis

PENDING_QUEUE = "nginx||pending_queue"
PROCESSING_QUEUE = "nginx||processing_queue"
RELOAD_REQUEST_STATUS_FORMAT = "nginx_reload_status||{}"
RELOAD_STATUS_TYPE = Literal["Queued", "Success", "Failure", "Skipped"]


class NginxReloadManager:
    redis_instance = None
    batch_size = 10000

    def __init__(self, directory=None, debug=False):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.last_reload_at: datetime = None
        self.exit_requested = False
        self.debug = debug

    def request_reload(self, request_id: str):
        self.redis.rpush(PENDING_QUEUE, request_id)
        self.redis.set(RELOAD_REQUEST_STATUS_FORMAT.format(request_id), "Queued")

    def get_status(self, request_id: str, not_found_status: RELOAD_STATUS_TYPE) -> RELOAD_STATUS_TYPE | None:
        status = self.redis.get(RELOAD_REQUEST_STATUS_FORMAT.format(request_id))
        if status is None:
            return not_found_status
        return status

    def process_requests(self):
        # Handle Signals
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        # Set last_reload_at to current time
        self.last_reload_at = datetime.now()

        # Start Loop
        while not self.exit_requested:
            try:
                is_mandatory_reload_required = (
                    datetime.now() - self.last_reload_at
                ).total_seconds() >= self.max_interval_without_reload_minutes * 60

                job_ids = self._dequeue_jobs()
                self.log(f"Dequeued {len(job_ids)} jobs")
                start = time.time()

                if job_ids or is_mandatory_reload_required:
                    status = self._reload_nginx()
                    self.log(f"Nginx Reload : {status} | {len(job_ids)} Jobs", print_always=True)
                    if status != "Skipped":
                        self._update_status_and_cleanup(job_ids, status)

                elapsed = time.time() - start
                sleep_time = max(0, self.max_permissible_wait_time - elapsed)
                if sleep_time:
                    time.sleep(sleep_time)
                    self.log(f"Waiting for {sleep_time:.2f} seconds before next check")
                else:
                    self.log("No sleep time, moving to next iteration")
            except Exception as e:
                import traceback

                traceback.print_exc()
                self.log(f"Error during processing requests : {e!s}")

    def _reload_nginx(self) -> RELOAD_STATUS_TYPE:
        from agent.proxy import Proxy

        if self._should_skip_nginx_reload():
            return "Skipped"

        try:
            proxy = Proxy()
            proxy._generate_proxy_config()
            subprocess.run("sudo systemctl reload nginx", shell=True, check=True)
            self.last_reload_at = datetime.now()
            return "Success"
        except Exception as e:
            import traceback

            traceback.print_exc()
            self.log(f"Error while reloading nginx : {e!s}")
            return "Failure"

    def _should_skip_nginx_reload(self):
        """
        ├─983331 nginx: worker process is shutting down
        ├─983332 nginx: worker process is shutting down
        ├─983333 nginx: worker process is shutting down
        ├─983334 nginx: worker process is shutting down
        ├─983361 nginx: worker process
        ├─983362 nginx: worker process
        ├─983363 nginx: worker process
        ├─983364 nginx: worker process
        └─983369 nginx: cache manager process
        """
        try:
            status = subprocess.run(
                "sudo systemctl status nginx", capture_output=True, shell=True
            ).stdout.decode("utf-8")
            total_workers = status.count("nginx: worker process")
            dying_workers = status.count("nginx: worker process is shutting down")
            active_workers = max(1, total_workers - dying_workers)
            return (dying_workers / active_workers) >= 3
        except Exception as e:
            self.log(f"Failed to check nginx status : {e!s}")
            return False

    def _dequeue_jobs(self) -> list[str]:
        # Fetch up to 10000 jobs, which might not be hit ever
        job_ids = self.redis.lrange(PENDING_QUEUE, 0, self.batch_size)
        if job_ids:
            # Trim pending queue
            self.redis.ltrim(PENDING_QUEUE, len(job_ids), -1)
            # Move jobs to processing queue
            self.redis.rpush(PROCESSING_QUEUE, *job_ids)

        # Fetch job IDs from processing queue
        # Important to fetch from processing queue to ensure than if worker died previously,
        # old jobs in processing queue are still processed
        return self.redis.lrange(PROCESSING_QUEUE, 0, self.batch_size)

    def _update_status_and_cleanup(self, job_ids: list[str], status: str):
        for job_id in job_ids:
            self.redis.set(RELOAD_REQUEST_STATUS_FORMAT.format(job_id), status)
        self.redis.ltrim(PROCESSING_QUEUE, len(job_ids), -1)

    def log(self, message, print_always=False):
        if not self.debug and not print_always:
            return
        print(f"[{datetime.now()}] {message!s}")

    def exit_gracefully(self, signum, frame):
        self.exit_requested = True
        self.log("Requested to exit gracefully")

    # Properties
    @property
    def redis(self):
        if not self.redis_instance:
            self.redis_instance = redis.Redis(
                port=self.config.get("redis_port", 25025),
                decode_responses=True,
            )
        return self.redis_instance

    @property
    def config(self) -> dict:
        if not hasattr(self, "_config"):
            with open(self.config_file, "r") as f:
                self._config = json.load(f)
        return self._config

    @property
    def max_permissible_wait_time(self) -> float:
        return 60 / self.max_reloads_per_minute

    @property
    def max_reloads_per_minute(self) -> int:
        return self.config.get("max_reloads_per_minute", 30)

    @property
    def max_interval_without_reload_minutes(self) -> int:
        """Maximum allowed minutes without a reload before forcing a voluntary reload."""
        return self.config.get("max_interval_without_reload_minutes", 10)


if __name__ == "__main__":
    manager = NginxReloadManager(debug=True)
    manager.process_requests()
