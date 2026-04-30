from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
import time
from datetime import datetime
from subprocess import Popen
from typing import TYPE_CHECKING, TypedDict

import docker
import jinja2

from agent.base import Base
from agent.exceptions import AgentException, RegistryDownException
from agent.job import Job, Step, job, step
from agent.utils import is_registry_healthy

if TYPE_CHECKING:
    from typing import Literal

    OutputKey = Literal["pre-build", "build", "push"]
    Output = dict[OutputKey, list[str]]


class AppInfo(TypedDict):
    app: str
    url: str
    release: str
    source: str
    hash: str
    branch: str


class CloneError(AgentException):
    pass


class ImageBuilder(Base):
    output: Output

    def __init__(
        self,
        # filename: str,
        image_repository: str,
        image_tag: str,
        no_cache: bool,
        no_push: bool,
        registry: dict,
        platform: str,
        build_token: str,
        dockerfile: str,
        clone_instructions: list[AppInfo],
        group: str,
        build_name: str,
        deploy_candidate_params: dict,
    ) -> None:
        super().__init__()

        # Image push params
        self.image_repository = image_repository
        self.image_tag = image_tag
        self.registry = registry
        self.platform = platform

        # Build context, params
        # self.filename = filename
        # self.filepath = os.path.join(
        #     get_image_build_context_directory(),
        #     self.filename,
        # )
        self.dockerfile = dockerfile
        self.clone_instructions = clone_instructions
        self.group = group
        self.build_name = build_name
        self.deploy_candidate_params = deploy_candidate_params

        self.no_cache = no_cache
        self.no_push = no_push
        self.last_published = datetime.now()
        self.build_failed = False
        self.build_token = build_token
        self.secret_path = None

        cwd = os.getcwd()
        self.config_file = os.path.join(cwd, "config.json")
        self.build_config_path = os.path.join(os.getcwd(), "repo", "agent", "build_configs")

        # Lines from build and push are sent to press for processing
        # and updating the respective Deploy Candidate
        self.output = {"pre-build": [], "build": [], "push": []}
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
        self._clone_repositories()
        self._prepare_build_context()

        # try:
        #     return self._build_and_push()
        # finally:
        #     self._cleanup_context()

    def _build_and_push(self):
        self._build_image()
        if not self.build_failed and not self.no_push:
            self._push_docker_image()
        return self.data

    def _clone_repository(self, app_info: AppInfo, clone_dir: str):
        """Clone the repository for the given app"""
        repo_url = app_info["url"]
        branch = app_info["branch"]
        source = app_info["source"]
        commit_hash = app_info["hash"]
        clone_path = os.path.join(clone_dir, source, commit_hash[:10])
        output = f"git clone {app_info['app']}"

        if os.path.exists(clone_path):
            return output + " CACHED\n"

        output += "\n"
        os.makedirs(clone_path, exist_ok=True)

        try:
            output += self._run_git_command("git init", clone_path) + "\n"

            origin_exists = "origin" in self._run_git_command("git remote", clone_path).split()
            if origin_exists:
                output += self._run_git_command(f"git remote set-url origin {repo_url}", clone_path) + "\n"
            else:
                output += self._run_git_command(f"git remote add origin {repo_url}", clone_path) + "\n"

            output += self._run_git_command("git config credential.helper ''", clone_path) + "\n"
            output += self._run_git_command(f"git fetch --depth 1 origin {commit_hash}", clone_path) + "\n"

            output += self._run_git_command(f"git checkout -B {branch}", clone_path) + "\n"
            output += self._run_git_command(f"git checkout {commit_hash}", clone_path) + "\n"
        except CloneError as e:
            raise CloneError(
                {"traceback": f"Failed to clone repository for {app_info['app']} - {e!s}"}
            ) from None

        return output

    def _generate_build_config_files(self):
        """Generate redis and supervisor config for build"""
        build_directory = os.path.join(get_builds_directory(), self.group, self.build_name)
        configs_to_generate = ["redis-cache.conf", "redis-queue.conf", "supervisor.conf"]
        template_folder = os.path.join(self.build_config_path, "config")
        dest_folder = os.path.join(build_directory, "config")

        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder, exist_ok=True)

        for config in configs_to_generate:
            template_config_file = os.path.join(template_folder, config)
            with open(template_config_file, "r") as file:
                template = jinja2.Template(file.read())

            rendered_config = template.render(doc=self.deploy_candidate_params, platform=self.platform)
            rendered_config_path = os.path.join(dest_folder, config)

            with open(rendered_config_path, "w") as output_file:
                output_file.write(rendered_config)

    def _copy_build_config_files(self):
        """Copy generic build config files to the build context"""
        build_directory = os.path.join(get_builds_directory(), self.group, self.build_name)

        for filename in ["common_site_config.json", "supervisord.conf", ".vimrc"]:
            shutil.copy(os.path.join(self.build_config_path, filename), build_directory)

        shutil.copytree(
            os.path.join(self.build_config_path, "redis"),
            os.path.join(build_directory, "redis"),
        )

    @step("Clone Repositories")
    def _clone_repositories(self):
        """Clone the apps passed from build instructions"""
        import time
        clone_directory = get_clone_directory()
        self.output["pre-build"] = []

        for app_info in self.clone_instructions:
            self.output["pre-build"].append(
                self._clone_repository(app_info, clone_directory),
            )
            self._publish_throttled_output(True)
            time.sleep(300) # Remove post testing


        return self.output["pre-build"]

    @step("Prepare Build Context")
    def _prepare_build_context(self):
        """Clone the apps passed from build instructions"""
        clone_directory = get_clone_directory()
        build_directory = os.path.join(get_builds_directory(), self.group, self.build_name)

        if not os.path.exists(build_directory):
            os.makedirs(build_directory, exist_ok=True)

        for app_info in self.clone_instructions:
            source_path = os.path.join(clone_directory, app_info["source"], app_info["hash"][:10])
            dest_path = os.path.join(build_directory, "apps", app_info["app"])

            if os.path.exists(dest_path):
                shutil.rmtree(dest_path)

            shutil.copytree(source_path, dest_path, symlinks=True)

        self._copy_build_config_files()
        self._generate_build_config_files()

        with open(os.path.join(build_directory, "Dockerfile"), "w") as dockerfile:
            dockerfile.write(self.dockerfile)

        with open(os.path.join(build_directory, "apps.txt"), "w") as apps_file:
            apps_file.write(
                "\n".join([app_info["app"] for app_info in self.clone_instructions]) + "\n",
            )

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
        command = f"docker buildx build --platform {self.platform}"

        if self.build_token:
            with tempfile.NamedTemporaryFile(
                delete=False,
                mode="w",
                prefix="buildtoken-secret-",
            ) as tmp:
                tmp.write(self.build_token)
                os.chmod(tmp.name, 0o600)
                self.secret_path = tmp.name

            command = f"{command} --secret id=build_token,src={self.secret_path}"

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

    def _wait_for_registry_recovery(self):
        """Wait for registry to recover after restart"""
        time.sleep(60)

    @step("Push Docker Image")
    def _push_docker_image(self):
        max_retries = 3
        environment = os.environ.copy()
        client = docker.from_env(environment=environment, timeout=5 * 60)

        for attempt in range(max_retries):
            self.output["push"].append({"id": "Retry", "output": "", "status": f"Success {attempt}"})
            try:
                if not is_registry_healthy(
                    self.registry["url"], self.registry["username"], self.registry["password"]
                ):
                    raise RegistryDownException("Registry is currently down")

                self._push_image(client)

                if not is_registry_healthy(
                    self.registry["url"], self.registry["username"], self.registry["password"]
                ):
                    raise RegistryDownException("Registry became unhealthy after push")

                return self.output["push"]

            except RegistryDownException as e:
                if attempt == max_retries - 1:
                    self._publish_throttled_output(True)
                    raise Exception("Failed to push image after multiple attempts") from e

                self._wait_for_registry_recovery()

            except Exception:
                self._publish_throttled_output(True)
                raise

        return None

    def _push_image(self, client):
        auth_config = {
            "username": self.registry["username"],
            "password": self.registry["password"],
            "serveraddress": self.registry["url"],
        }
        for line in client.images.push(
            self.image_repository,
            self.image_tag,
            stream=True,
            decode=True,
            auth_config=auth_config,
        ):
            self.output["push"].append(line)
            self._publish_throttled_output(False)

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

    def _run_git_command(self, command: str, cwd: str) -> str:
        """Run a git command in a directory"""
        process = Popen(
            shlex.split(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=cwd,
            universal_newlines=True,
        )
        output, _ = process.communicate()
        return_code = process.returncode

        self.build_failed = return_code != 0
        if self.build_failed:
            self.data.update({"build_failed": True})

        if return_code != 0:
            raise CloneError(f"Git command failed: {command}\nOutput: {output}")

        return output

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

        if self.secret_path:
            os.remove(self.secret_path)

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


def get_clone_directory():
    path = os.path.join(os.getcwd(), ".clones")
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_builds_directory():
    path = os.path.join(os.getcwd(), ".docker-builds")
    if not os.path.exists(path):
        os.makedirs(path)
    return path
