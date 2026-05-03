from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
import time
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from subprocess import Popen
from typing import TYPE_CHECKING, TypedDict

import docker
import jinja2
import semantic_version as sv

from agent.base import Base
from agent.build_utils.validations import get_package_manager_files
from agent.exceptions import AgentException, RegistryDownException
from agent.job import Job, Step, job, step
from agent.utils import is_registry_healthy

if TYPE_CHECKING:
    from typing import Literal

    from agent.build_utils.validations import PackageManagers

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


class ContextValidationError(AgentException):
    def __init__(
        self,
        message: str,
        app,
        actual: str | None = None,
        expected: str | None = None,
        package: str | None = None,
    ):
        super().__init__(
            {
                "message": message,
                "app": app,
                "actual": actual,
                "expected": expected,
                "package": package,
            }
        )


class BuildWarning(Warning):
    pass


@dataclass
class JobContext:
    job: Job | None = None
    step: Step | None = None


class JobMixin:
    _job_context: JobContext

    @property
    def job_record(self):
        if self._job_context.job is None:
            self._job_context.job = Job()
        return self._job_context.job

    @property
    def step_record(self):
        if self._job_context.step is None:
            self._job_context.step = Step()
        return self._job_context.step

    @step_record.setter
    def step_record(self, value):
        self._job_context.step = value


@dataclass
class ContextManager(Base, JobMixin):
    # We need to keep the job context same everywhere.
    # Therefore pass this from the ImageBuilder
    _job_context: JobContext
    clone_instructions: list[AppInfo]
    group: str
    build_name: str
    dockerfile: str
    platform: Literal["arm64", "x86_64"]
    build_config_path: str = field(
        default_factory=lambda: os.path.join(os.getcwd(), "repo", "agent", "build_configs")
    )
    deploy_candidate_params: dict = field(default_factory=dict)

    def __post_init__(self):
        super().__init__()
        self.output: dict[str, list[str]] = {"pre-build": []}
        self.build_directory = os.path.join(get_builds_directory(), self.group, self.build_name)

    def _copy_build_config_files(self):
        """Copy generic build config files to the build context"""
        for filename in ["common_site_config.json", "supervisord.conf", ".vimrc"]:
            shutil.copy(os.path.join(self.build_config_path, filename), self.build_directory)

        shutil.copytree(
            os.path.join(self.build_config_path, "redis"),
            os.path.join(self.build_directory, "redis"),
        )

    def _generate_build_config_files(self):
        """Generate redis and supervisor config for build"""
        configs_to_generate = ["redis-cache.conf", "redis-queue.conf", "supervisor.conf"]
        template_folder = os.path.join(self.build_config_path, "config")
        dest_folder = os.path.join(self.build_directory, "config")

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

    def _clone_repository(self, app_info: AppInfo, clone_dir: str):
        """Clone the repository for the given app"""
        source = app_info["source"]
        commit_hash = app_info["hash"]
        clone_path = os.path.join(clone_dir, source, commit_hash[:10])

        if os.path.exists(clone_path):
            return f"git clone {app_info['app']} CACHED\n"

        os.makedirs(clone_path, exist_ok=True)

        commands = [
            "git init",
            f"git remote add origin {app_info['url']}",
            "git config credential.helper ''",
            f"git fetch --depth 1 origin {commit_hash}",
            f"git checkout -B {app_info['branch']}",
            f"git checkout {commit_hash}",
        ]

        output = f"git clone {app_info['app']}\n"
        try:
            for command in commands:
                result = self.execute(command, directory=clone_path)
                output += result.get("output", "") + "\n"
        except AgentException as e:
            cleanup_parital_clones(clone_path)
            raise CloneError(
                {
                    "traceback": (
                        f"Failed to clone repository for {app_info['app']} - {e.data.get('output', '')}"
                    )
                }
            ) from None

        return output

    @step("Clone Repositories")
    def clone_repositories(self):
        """Clone the apps passed from build instructions"""
        clone_directory = get_clone_directory()
        self.output["pre-build"] = []

        for app_info in self.clone_instructions:
            self.output["pre-build"].append(
                self._clone_repository(app_info, clone_directory),
            )
            self.publish_data(self.output)

        return self.output["pre-build"]

    @step("Prepare Build Context")
    def prepare_build_context(self):
        """Clone the apps passed from build instructions and return the build context directory path"""
        clone_directory = get_clone_directory()

        if not os.path.exists(self.build_directory):
            os.makedirs(self.build_directory, exist_ok=True)

        for app_info in self.clone_instructions:
            source_path = os.path.join(clone_directory, app_info["source"], app_info["hash"][:10])
            dest_path = os.path.join(self.build_directory, "apps", app_info["app"])

            if os.path.exists(dest_path):
                shutil.rmtree(dest_path)

            shutil.copytree(source_path, dest_path, symlinks=True)

        self._copy_build_config_files()
        self._generate_build_config_files()

        with open(os.path.join(self.build_directory, "Dockerfile"), "w") as dockerfile:
            dockerfile.write(self.dockerfile)

        with open(os.path.join(self.build_directory, "apps.txt"), "w") as apps_file:
            apps_file.write(
                "\n".join([app_info["app"] for app_info in self.clone_instructions]) + "\n",
            )


@dataclass
class ValidationManager(Base, JobMixin):
    _job_context: JobContext
    dependencies: dict[str, str]

    def get_dependency_version(self, dependency_name: str) -> str | None:
        for dep, version in self.dependencies.items():
            if dep.replace("_VERSION", "").casefold() == dependency_name.casefold():
                return version

        raise Exception(f"Dependency version not found for {dependency_name}")

    @step("Run Validations")
    def validate(self, apps: list[str], build_directory: str):
        repo_path_map = {}
        for app in apps:
            repo_path_map[app] = os.path.join(build_directory, "apps", app)

        self.pmf = get_package_manager_files(repo_path_map)
        self._validate()

    def _validate(self):
        self._validate_python_dependency_files()
        self._validate_python_requirement()
        self._validate_node_requirement()

    @staticmethod
    def check_version(actual: str, expected: str) -> bool:
        # Python version mentions on press dont mention the patch version.
        if actual.count(".") == 1:
            actual += ".0"

        sv_actual = sv.Version(actual)
        sv_expected = sv.SimpleSpec(expected)

        return sv_actual in sv_expected

    def _validate_python_requirement(self):
        actual = self.get_dependency_version("python")
        for app, pm in self.pmf.items():
            self._validate_python_version(app, actual, pm)

    def _validate_python_version(self, app: str, actual: str, pm: PackageManagers):
        expected = (pm["pyproject"] or {}).get("project", {}).get("requires-python")
        if expected is None or self.check_version(actual, expected):
            return

        # Do not change args without updating deploy_notifications.py
        raise ContextValidationError(
            "Incompatible Python version found",
            app,
            actual,
            expected,
        )

    def _validate_node_requirement(self):
        actual = self.get_dependency_version("node")
        for app, pm in self.pmf.items():
            self._validate_node_version(app, actual, pm)

    def _validate_node_version(self, app: str, actual: str, pm: PackageManagers):
        for pckj in pm["packagejsons"]:
            expected = pckj.get("engines", {}).get("node")
            if expected is None or self.check_version(actual, expected):
                continue

            package_name = pckj.get("name")

            # Do not change args without updating deploy_notifications.py
            raise ContextValidationError(
                "Incompatible Node version found",
                app,
                actual,
                expected,
                package_name,
            )

    def _validate_python_dependency_files(self) -> None:
        """Check pyproject.toml and requirements.txt for each app."""
        for app, pm in self.pmf.items():
            repo_path = Path(pm["repo_path"])
            has_pyproject = (repo_path / "pyproject.toml").exists()
            has_requirements = (repo_path / "requirements.txt").exists()

            if not has_pyproject and not has_requirements:
                raise ContextValidationError(
                    "No python dependency file found",
                    app,
                )

            if has_pyproject and has_requirements:
                warnings.warn(
                    f"Both pyproject.toml and requirements.txt found for app '{app}'. "
                    "pyproject.toml file will have precedence.",
                    BuildWarning,
                    stacklevel=2,
                    source={"app": app},
                )

            elif has_requirements and not has_pyproject:
                warnings.warn(
                    f"App '{app}' uses only requirements.txt. Consider migrating to pyproject.toml.",
                    BuildWarning,
                    stacklevel=2,
                    source={"app": app},
                )


class ImageBuilder(Base, JobMixin):
    output: Output

    def __init__(
        self,
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

        self._job_context = JobContext()

        self.context_manager = ContextManager(
            clone_instructions=clone_instructions,
            build_name=build_name,
            group=group,
            dockerfile=dockerfile,
            deploy_candidate_params=deploy_candidate_params,
            platform=platform,
            _job_context=self._job_context,
        )
        self.build_directory = self.context_manager.build_directory
        self.validation_manager = ValidationManager(
            _job_context=self._job_context,
            dependencies=deploy_candidate_params.get("dependencies"),
        )

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
        self.output = {"build": [], "push": []}
        self.push_output_lines = []

    @job("Run Remote Builder")
    def run_remote_builder(self):
        self.context_manager.clone_repositories()
        self.context_manager.prepare_build_context()
        self.validation_manager.validate(
            apps=[app_info["app"] for app_info in self.context_manager.clone_instructions],
            build_directory=self.context_manager.build_directory,
        )

        # try:
        #     return self._build_and_push()
        # finally:
        #     self._cleanup_context()

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


def cleanup_parital_clones(clone_path: str):
    """Cleanup partially clone repositories which might behave as cache and cause build failures"""
    shutil.rmtree(clone_path, ignore_errors=True)
