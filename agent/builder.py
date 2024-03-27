import os
import shlex
import subprocess
from subprocess import Popen

import docker

from agent.base import Base
from agent.job import Job, Step, job, step


class ImageBuilder(Base):
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

        cwd = os.getcwd()
        self.config_file = os.path.join(cwd, "config.json")

        # Lines from build and push are sent to press for processing
        # and updating the respective Deploy Candidate
        self.build_image_lines = []
        self.push_image_lines = []

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
        self._build_image()
        if not self.no_push:
            self._push_docker_image()
        self._cleanup_context()
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
        self._publish_docker_build_output(result)

    def _get_build_command(self) -> str:
        command = "docker build"
        command = f"{command} -t {self._get_image_name()}"

        if self.no_cache:
            command = f"{command} --no-cache"

        command = f"{command} - "
        return command

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
            self.build_image_lines.append(line)
            self.publish_build_image_output()
        self.publish_build_image_output(True)

    @step("Push Docker Image")
    def _push_docker_image(self):
        self.push_image_lines = []
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
                self.push_image_lines.append(line)
                self.publish_push_image_output()
        except Exception:
            # TODO: Handle this
            raise

    def publish_build_image_output(self, flush=False):
        if not flush and (len(self.build_image_lines) % 25 != 0):
            return

        if len(self.build_image_lines) == 0:
            return

        self.publish_data({"build": self.build_image_lines})
        # self.build_image_lines = []

    def publish_push_image_output(self, flush=False):
        if not flush and (len(self.push_image_lines) % 5) != 0:
            return

        if len(self.push_image_lines) == 0:
            return

        self.publish_lines(self.push_image_lines, "push")
        self.publish_data({"push": self.push_image_lines})
        # self.push_image_lines = []

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

        for line in process.stdout:
            yield line

        process.stdout.close()
        input_file.close()

        return_code = process.wait()
        self.publish_build_image_output(True)

        if return_code:
            # TODO: Handle this properly
            raise subprocess.CalledProcessError(return_code, command)

    @step("Cleanup Context")
    def _cleanup_context(self):
        if os.path.exists(self.filepath):
            os.remove(self.filepath)


def get_image_build_context_directory():
    path = os.path.join(os.getcwd(), "build_context")
    if not os.path.exists(path):
        os.makedirs(path)
    return path
