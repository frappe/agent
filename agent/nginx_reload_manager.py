from __future__ import annotations

import contextlib
import glob
import json
import os
import re
import signal
import subprocess
import time
import traceback
from datetime import datetime
from enum import Enum, auto

import redis

PENDING_QUEUE = "nginx||pending_queue"
PROCESSING_QUEUE = "nginx||processing_queue"
RELOAD_REQUEST_STATUS_FORMAT = "nginx_reload_status||{}"


class ReloadStatus(Enum):
    Queued = "Queued"
    Success = "Success"
    Failure = "Failure"
    Skipped = "Skipped"


class ManagerState(Enum):
    INIT = auto()
    FETCH_JOBS = auto()
    RELOAD_PENDING = auto()
    RELOAD_SUCCESS = auto()
    RELOAD_FAILURE = auto()
    AUTO_FIX_CONFIG = auto()
    WAIT = auto()


class NginxReloadManager:
    redis_instance = None
    batch_size = 1000

    def __init__(self, directory=None, debug=False):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.last_reload_at: datetime = None
        self.exit_requested = False
        self.debug = debug
        self.state = ManagerState.INIT

    def request_reload(self, request_id: str):
        self.redis.rpush(PENDING_QUEUE, request_id)
        self.redis.set(RELOAD_REQUEST_STATUS_FORMAT.format(request_id), ReloadStatus.Queued.value)

    def get_status(self, request_id: str, not_found_status: ReloadStatus) -> ReloadStatus | None:
        status = self.redis.get(RELOAD_REQUEST_STATUS_FORMAT.format(request_id))
        if status is None:
            return not_found_status
        return ReloadStatus(status)

    def process_requests(self):
        # Handle Signals
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.last_reload_at = datetime.now()
        self.start_time = datetime.now()
        self.state: ReloadStatus = ManagerState.FETCH_JOBS
        self.job_ids = []
        self.error = None

        while not self.exit_requested:
            try:
                self._process_state()
            except Exception:
                traceback.print_exc()
                self.state = ManagerState.WAIT

    def _process_state(self):  # noqa: C901
        if self.state == ManagerState.FETCH_JOBS:
            self.start_time = datetime.now()
            self.job_ids = self._dequeue_jobs()
            if self.job_ids or self.is_mandatory_reload_required:
                self.state = ManagerState.RELOAD_PENDING
                return

            self.state = ManagerState.WAIT

        elif self.state == ManagerState.RELOAD_PENDING:
            if self._should_skip_nginx_reload():
                self.state = ManagerState.WAIT
                return

            status = self._reload_nginx()
            self.log(f"Reload {status.name} | {len(self.job_ids)} requests", print_always=True)
            self.state = (
                ManagerState.RELOAD_SUCCESS
                if status == ReloadStatus.Success
                else ManagerState.AUTO_FIX_CONFIG
            )

        elif self.state == ManagerState.RELOAD_SUCCESS:
            self._update_status_and_cleanup(self.job_ids, ReloadStatus.Success)
            self.last_reload_at = datetime.now()
            self.state = ManagerState.WAIT

        elif self.state == ManagerState.RELOAD_FAILURE:
            self._update_status_and_cleanup(self.job_ids, ReloadStatus.Failure)
            self.state = ManagerState.WAIT

        elif self.state == ManagerState.AUTO_FIX_CONFIG:
            if isinstance(self.error, subprocess.CalledProcessError):
                error_msg = self.error.stderr or ""
                domain = self._find_conflicting_domain_from_error_message(error_msg)
                if domain and self._fix_conflicting_domain_in_config(domain):
                    self.state = ManagerState.RELOAD_PENDING
                    return

            self.state = ManagerState.RELOAD_FAILURE

        elif self.state == ManagerState.WAIT:
            elapsed_time = datetime.now() - self.start_time
            if (sleep_time := self.max_permissible_wait_time - elapsed_time.total_seconds()) > 0:
                self.log(f"Waiting for {sleep_time:.2f} seconds before next check")
                time.sleep(sleep_time)

            self.state = ManagerState.FETCH_JOBS

    # Private methods
    def _reload_nginx(self) -> ReloadStatus:
        from agent.proxy import Proxy

        try:
            proxy = Proxy()

            proxy._generate_proxy_config()
            subprocess.run(
                "sudo nginx -s reload",
                shell=True,
                check=True,
                capture_output=True,
                text=True,
            )

            self.last_reload_at = datetime.now()
            return ReloadStatus.Success
        except subprocess.CalledProcessError as e:
            error_msg = f"NGINX reload failed (code: {e.returncode})\n"
            error_msg += f"Command: {e.cmd}\n"
            if e.stdout:
                error_msg += f"Stdout: {e.stdout}\n"
            if e.stderr:
                error_msg += f"Stderr: {e.stderr}"
            
            self.error = error_msg
            self.log(error_msg)
            traceback.print_exc()
            return ReloadStatus.Failure
        except Exception as e:
            self.error = e

            traceback.print_exc()
            self.log(f"Error while reloading nginx : {e!s}")
            return ReloadStatus.Failure

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
            total_workers = status.count("nginx: worker process") + status.count("nginx: master process")
            dying_workers = status.count("nginx: worker process is shutting down")
            active_workers = max(1, total_workers - dying_workers)
            return dying_workers >= active_workers or dying_workers >= 4
        except Exception as e:
            self.log(f"Failed to check nginx status : {e!s}")
            return False

    def _find_conflicting_domain_from_error_message(self, error_message: str) -> str | None:
        match = re.search(r'conflicting parameter "(.*?)"', error_message)
        if not match:
            return None
        return match.group(1)

    def _fix_conflicting_domain_in_config(self, domain: str) -> bool:
        self.log(f"Domain {domain} is conflicting, attempting auto fix", print_always=True)

        # Find the paths
        paths = glob.glob(os.path.join(self.directory, f"nginx/upstreams/*/{domain}"))
        paths.sort(key=lambda x: os.path.getmtime(x))

        if len(paths) < 2:
            self.log(f"Not able to auto fix the issue for domain : {domain!s}", print_always=True)
            return False

        # Keep newest file and delete the rest
        to_keep = paths[-1]
        self.log(f"Keeping {to_keep}", print_always=True)
        to_delete = paths[:-1]
        for path in to_delete:
            with contextlib.suppress(FileNotFoundError):
                # In case RQ worker removed the file
                # before we could delete it
                os.remove(path)
            self.log(f"Deleted {path}", print_always=True)

        return True

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
        jobs = self.redis.lrange(PROCESSING_QUEUE, 0, self.batch_size)
        self.log(f"Dequeued {len(job_ids)} jobs")
        return jobs

    def _update_status_and_cleanup(self, job_ids: list[str], status: ReloadStatus):
        for job_id in job_ids:
            self.redis.set(RELOAD_REQUEST_STATUS_FORMAT.format(job_id), status.value)
        self.redis.ltrim(PROCESSING_QUEUE, len(job_ids), -1)

    def exit_gracefully(self, signum, frame):
        if self.exit_requested:
            self.log("Stopping forefully")
            exit(0)

        self.exit_requested = True
        self.log("Requested to exit gracefully")

    def log(self, message, print_always=False):
        if not self.debug and not print_always:
            return
        print(f"[{datetime.now()}] {message!s}")

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

    @property
    def is_mandatory_reload_required(self) -> bool:
        """Check if a mandatory reload is required."""
        return (
            self.last_reload_at
            and (datetime.now() - self.last_reload_at).total_seconds()
            >= self.max_interval_without_reload_minutes * 60
        )


if __name__ == "__main__":
    manager = NginxReloadManager()
    manager.process_requests()
