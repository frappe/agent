from __future__ import annotations

import json
import os
import re
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from cryptography.fernet import Fernet
from filelock import FileLock

from agent.base import Base
from agent.job import job, step

WORKLOAD_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
IMAGE_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:@-]{0,254}$")
ENVIRONMENT_FILE_MAX_AGE = 24 * 60 * 60


class InvalidWorkload(ValueError):
    pass


class Workload(Base):
    def __init__(self, server):
        super().__init__()
        self.server = server
        self.directory = self._workloads_directory()

    @property
    def job_record(self):
        return self.server.job_record

    @property
    def step_record(self):
        return self.server.step_record

    @step_record.setter
    def step_record(self, value):
        self.server.step_record = value

    def prepare_deploy(self, config: dict, environment: dict[str, str]):
        deployment = WorkloadConfig(config, environment)
        deployment.validate()
        directory = self._create_directory(deployment.name)
        self._remove_expired_environments(directory)
        environment_file = self._write_environment(directory, deployment.environment)
        try:
            return self.deploy(deployment.public_config(), str(environment_file))
        except Exception:
            environment_file.unlink(missing_ok=True)
            raise

    @job("Deploy Workload", priority="low")
    def deploy(self, config: dict, environment_file: str):
        deployment = WorkloadConfig(config, {})
        deployment.validate()
        try:
            lock_path = Path(environment_file).parent / ".deploy.lock"
            with FileLock(lock_path, timeout=300), self._decrypted_environment(
                Path(environment_file)
            ) as decrypted:
                self._deploy(deployment, decrypted)
        finally:
            Path(environment_file).unlink(missing_ok=True)

    @step("Deploy Workload")
    def _deploy(self, deployment: WorkloadConfig, environment_file: Path):
        directory = self._create_directory(deployment.name)
        self.server.execute(["docker", "pull", deployment.image])
        self._replace_container(deployment, environment_file)
        self._write_config(directory, deployment.config_without_secrets())

    def status(self, name: str):
        validate_name(name)
        result = self.server.execute(
            ["docker", "inspect", "--format", "{{json .State}}", name],
            non_zero_throw=False,
        )
        if result["returncode"]:
            return {"name": name, "status": "missing"}
        return {"name": name, "status": json.loads(result["output"])}

    def logs(self, name: str, lines: int = 200):
        validate_name(name)
        if not 1 <= lines <= 1000:
            raise InvalidWorkload("Log line count must be between 1 and 1000")
        result = self.server.execute(
            ["docker", "logs", "--tail", str(lines), name],
            skip_output_log=True,
        )
        return {"name": name, "logs": result["output"]}

    def cleanup_cancelled_environment(self, job_data: str):
        data = json.loads(job_data)
        if data.get("function") != "deploy":
            return
        arguments = data.get("args", [])
        if len(arguments) < 2:
            return
        self._remove_queued_environment(arguments[1])

    @job("Rollback Workload", priority="low")
    def rollback(self, name: str):
        validate_name(name)
        previous = f"{name}-previous"
        failed = f"{name}-failed"
        if not self._container_exists(previous):
            raise InvalidWorkload("No previous workload deployment is available")
        self.server.execute(["docker", "stop", name])
        try:
            self.server.execute(["docker", "rename", name, failed])
        except Exception:
            self.server.execute(["docker", "start", name])
            raise
        try:
            self.server.execute(["docker", "rename", previous, name])
            self.server.execute(["docker", "start", name])
        except Exception:
            if self._container_exists(name):
                self.server.execute(["docker", "rename", name, previous])
            self.server.execute(["docker", "rename", failed, name])
            self.server.execute(["docker", "start", name])
            raise
        self._remove_container(failed)

    def _workloads_directory(self) -> str:
        configured = self.server.config.get("workloads_directory")
        if configured:
            return configured
        return os.path.join(os.path.dirname(self.server.benches_directory), "workloads")

    def _create_directory(self, name: str) -> Path:
        root = Path(self.directory)
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        root.chmod(0o700)
        directory = root / name
        directory.mkdir(mode=0o700, exist_ok=True)
        directory.chmod(0o700)
        return directory

    def _write_environment(self, directory: Path, environment: dict[str, str]) -> Path:
        content = "".join(f"{key}={value}\n" for key, value in sorted(environment.items()))
        encrypted = self._environment_cipher().encrypt(content.encode())
        return self._write_protected_file(directory, "environment-queued-", encrypted)

    @contextmanager
    def _decrypted_environment(self, encrypted_path: Path):
        content = self._environment_cipher().decrypt(encrypted_path.read_bytes())
        path = self._write_protected_file(encrypted_path.parent, "environment-runtime-", content)
        try:
            yield path
        finally:
            path.unlink(missing_ok=True)

    def _environment_cipher(self) -> Fernet:
        key_path = Path(self.directory) / ".environment.key"
        with FileLock(f"{key_path}.lock", timeout=30):
            if not key_path.exists():
                self._write_exclusive_file(key_path, Fernet.generate_key())
            key_path.chmod(0o600)
            return Fernet(key_path.read_bytes())

    def _write_protected_file(self, directory: Path, prefix: str, content: bytes) -> Path:
        descriptor, name = tempfile.mkstemp(prefix=prefix, dir=directory)
        path = Path(name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as file:
                descriptor = -1
                file.write(content)
        except Exception:
            if descriptor != -1:
                os.close(descriptor)
            path.unlink(missing_ok=True)
            raise
        return path

    def _write_exclusive_file(self, path: Path, content: bytes):
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as file:
                descriptor = -1
                file.write(content)
        finally:
            if descriptor != -1:
                os.close(descriptor)

    def _remove_expired_environments(self, directory: Path):
        cutoff = time.time() - ENVIRONMENT_FILE_MAX_AGE
        for path in directory.glob("environment-queued-*"):
            if path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)

    def _remove_queued_environment(self, name: str):
        root = Path(self.directory).resolve()
        path = Path(name).resolve()
        if os.path.commonpath((root, path)) != str(root):
            raise InvalidWorkload("Queued environment path is outside the workloads directory")
        if not path.name.startswith("environment-queued-"):
            raise InvalidWorkload("Invalid queued environment path")
        path.unlink(missing_ok=True)

    def _write_config(self, directory: Path, config: dict):
        path = directory / "config.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(config, indent=2, sort_keys=True))
        temporary.chmod(0o600)
        temporary.replace(path)

    def _replace_container(self, deployment: WorkloadConfig, environment_file: Path):
        previous = f"{deployment.name}-previous"
        candidate = f"{deployment.name}-candidate"
        self._remove_container(candidate)
        self._remove_container(previous)
        current_exists = self._container_exists(deployment.name)
        if current_exists:
            self.server.execute(["docker", "rename", deployment.name, previous])
            try:
                self.server.execute(["docker", "stop", previous])
            except Exception:
                self.server.execute(["docker", "rename", previous, deployment.name])
                raise
        try:
            self.server.execute(deployment.run_command(candidate, environment_file))
            self._wait_until_healthy(deployment)
        except Exception:
            self._remove_container(candidate)
            if current_exists:
                self.server.execute(["docker", "rename", previous, deployment.name])
                self.server.execute(["docker", "start", deployment.name])
            raise
        self.server.execute(["docker", "rename", candidate, deployment.name])

    def _wait_until_healthy(self, deployment: WorkloadConfig):
        deadline = time.monotonic() + deployment.health_timeout
        url = f"http://127.0.0.1:{deployment.host_port}{deployment.health_path}"
        while time.monotonic() < deadline:
            try:
                with urlopen(url, timeout=2) as response:
                    if response.status < 400:
                        return
            except (OSError, URLError):
                time.sleep(1)
        raise RuntimeError(f"Workload health check failed on {deployment.health_path}")

    def _container_exists(self, name: str) -> bool:
        result = self.server.execute(["docker", "inspect", name], non_zero_throw=False)
        return result["returncode"] == 0

    def _remove_container(self, name: str):
        if self._container_exists(name):
            self.server.execute(["docker", "rm", "--force", name])


class WorkloadConfig:
    def __init__(self, config: dict, environment: dict[str, str]):
        self.name = config.get("name", "")
        self.image = config.get("image", "")
        self.container_port = config.get("container_port")
        self.host_port = config.get("host_port")
        self.health_path = config.get("health_path", "/")
        self.health_timeout = config.get("health_timeout", 60)
        self.environment = environment
        self.environment_keys = config.get("environment_keys", sorted(environment))

    def validate(self):
        validate_name(self.name)
        if not IMAGE_REFERENCE.fullmatch(self.image):
            raise InvalidWorkload("Invalid Docker image reference")
        self._validate_port(self.container_port, "container_port")
        self._validate_port(self.host_port, "host_port")
        if not isinstance(self.health_path, str) or not self.health_path.startswith("/"):
            raise InvalidWorkload("Health path must start with /")
        if not isinstance(self.health_timeout, int) or not 1 <= self.health_timeout <= 300:
            raise InvalidWorkload("Health timeout must be between 1 and 300 seconds")
        self._validate_environment()

    def _validate_port(self, port, field: str):
        if not isinstance(port, int) or not 1024 <= port <= 65535:
            raise InvalidWorkload(f"{field} must be between 1024 and 65535")

    def _validate_environment(self):
        if not isinstance(self.environment, dict):
            raise InvalidWorkload("Environment must be an object")
        for key, value in self.environment.items():
            if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", key):
                raise InvalidWorkload(f"Invalid environment variable name: {key}")
            if not isinstance(value, str) or "\n" in value or "\r" in value:
                raise InvalidWorkload(f"Invalid environment variable value: {key}")

    def run_command(self, container_name: str, environment_file: Path) -> list[str]:
        return [
            "docker",
            "run",
            "--detach",
            "--name",
            container_name,
            "--restart",
            "unless-stopped",
            "--env-file",
            str(environment_file),
            "--publish",
            f"127.0.0.1:{self.host_port}:{self.container_port}",
            self.image,
        ]

    def public_config(self) -> dict:
        return self.config_without_secrets()

    def config_without_secrets(self) -> dict:
        return {
            "name": self.name,
            "image": self.image,
            "container_port": self.container_port,
            "host_port": self.host_port,
            "health_path": self.health_path,
            "health_timeout": self.health_timeout,
            "environment_keys": self.environment_keys,
        }


def validate_name(name: str):
    if not isinstance(name, str) or not WORKLOAD_NAME.fullmatch(name):
        raise InvalidWorkload("Workload name must contain lowercase letters, numbers, or hyphens")
