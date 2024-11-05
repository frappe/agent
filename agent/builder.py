from __future__ import annotations

import os
import shlex
import subprocess
from datetime import datetime
from subprocess import Popen
from typing import TYPE_CHECKING

import docker

from agent.base import Base
from agent.job import Job, Step, job, step

if TYPE_CHECKING:
    from typing import Literal

    OutputKey = Literal["build", "push"]
    Output = dict[OutputKey, list[str]]


class ImageBuilder(Base):
    output: Output

    def __init__(
        self,
        filename: str,
        image_repository: str,
        image_tag: str,
        no_cache: bool,
        no_push: bool,
        registry: dict,
    ) -> None:
        super().__init__()

        # Image push params
        self.image_repository = image_repository
        self.image_tag = image_tag
        self.registry = registry

        # Build context, params
        self.filename = filename
        self.filepath = os.path.join(
            get_image_build_context_directory(),
            self.filename,
        )
        self.no_cache = no_cache
        self.no_push = no_push
        self.last_published = datetime.now()
        self.build_failed = False

        cwd = os.getcwd()
        self.config_file = os.path.join(cwd, "config.json")

        # Lines from build and push are sent to press for processing
        # and updating the respective Deploy Candidate
        self.output = {
            "build": [],
            "push": [],
        }
        self.push_output_lines = []

        self.job = None
        self.step = None

    @property
    def job_record(self):
        if self.job is None:
            self.job = Job()
        return self.job

    @property
    def step_record(self):
        if self.step is None:
            self.step = Step()
        return self.step

    @step_record.setter
    def step_record(self, value):
        self.step = value

    @job("Run Remote Builder")
    def run_remote_builder(self):
        try:
            return self._build_and_push()
        finally:
            self._cleanup_context()

    def _build_and_push(self):
        self._build_image()
        if not self.build_failed and not self.no_push:
            self._push_docker_image()
        return self.data

    @step("Build Image")
    def _build_image(self):
        # Note: build command and environment are different from when
        # build runs on the press server.
        command = self._get_build_command()
        environment = self._get_build_environment()
        result = self._run(
            command=command,
            environment=environment,
            input_filepath=self.filepath,
        )
        self.output["build"] = []
        self._publish_docker_build_output(result)
        return {"output": self.output["build"]}

    def _get_build_command(self) -> str:
        command = "docker buildx build --platform linux/amd64"
        command = f"{command} -t {self._get_image_name()}"

        if self.no_cache:
            command = f"{command} --no-cache"

        return f"{command} - "

    def _get_build_environment(self) -> dict:
        environment = os.environ.copy()
        environment.update(
            {
                "DOCKER_BUILDKIT": "1",
                "BUILDKIT_PROGRESS": "plain",
                "PROGRESS_NO_TRUNC": "1",
            }
        )
        return environment

    def _publish_docker_build_output(self, result):
        for line in result:
            self.output["build"].append(line)
            self._publish_throttled_output(False)
        self._publish_throttled_output(True)

    @step("Push Docker Image")
    def _push_docker_image(self):
        environment = os.environ.copy()
        client = docker.from_env(environment=environment)
        auth_config = {
            "username": self.registry["username"],
            "password": self.registry["password"],
            "serveraddress": self.registry["url"],
        }
        try:
            for line in client.images.push(
                self.image_repository,
                self.image_tag,
                stream=True,
                decode=True,
                auth_config=auth_config,
            ):
                self.output["push"].append(line)
                self._publish_throttled_output(False)
        except Exception:
            self._publish_throttled_output(True)
            raise
        return self.output["push"]

    def _publish_throttled_output(self, flush: bool):
        if flush:
            self.publish_data(self.output)
            return

        now = datetime.now()
        if (now - self.last_published).total_seconds() <= 1:
            return

        self.last_published = now
        self.publish_data(self.output)

    def _get_image_name(self):
        return f"{self.image_repository}:{self.image_tag}"

    def _run(
        self,
        command: str,
        environment: dict,
        input_filepath: str,
    ):
        with open(input_filepath, "rb") as input_file:
            process = Popen(
                shlex.split(command),
                stdin=input_file,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=environment,
                universal_newlines=True,
            )

        yield from process.stdout

        process.stdout.close()
        input_file.close()

        return_code = process.wait()
        self._publish_throttled_output(True)

        self.build_failed = return_code != 0
        self.data.update({"build_failed": self.build_failed})

    @step("Cleanup Context")
    def _cleanup_context(self):
        if not os.path.exists(self.filepath):
            return {"cleanup": False}

        os.remove(self.filepath)
        return {"cleanup": True}


def get_image_build_context_directory():
    path = os.path.join(os.getcwd(), "build_context")
    if not os.path.exists(path):
        os.makedirs(path)
    return path
