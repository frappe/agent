import datetime
import json
import os
import re
import shlex
import subprocess
import uuid
from subprocess import Popen

import docker
import dockerfile

from agent.base import AgentException, Base
from agent.job import job, Job


class ImageBuilder(Base):
	def __init__(self, filename: str, image_repository: str, image_tag: str, no_cache: bool, registry: dict,
	             build_steps: dict, **kwargs) -> None:
		super().__init__()
		self.directory = os.getcwd()
		self.config_file = os.path.join(self.directory, "config.json")
		self.job = None
		self.filename = filename
		self.image_repository = image_repository
		self.image_tag = image_tag
		self.no_cache = no_cache
		self.registry = registry
		self.build_steps = build_steps
		self.build_output = ""
		self.docker_image_id = None
		self._validate_registry()

	@property
	def job_record(self):
		if self.job is None:
			self.job = Job()
		return self.job

	def _validate_registry(self):
		if not self.registry.get("url"):
			raise AgentException("registry.url is required")
		if not self.registry.get("username"):
			raise AgentException("registry.username is required")
		if not self.registry.get("password"):
			raise AgentException("registry.password is required")

	@job("Build And Push Image", priority="high", is_yielding_output=True)
	def build_and_push_image(self):
		for o in self._build_image():
			yield o
		for o in self._push_docker_image():
			yield o

	def _build_image(self):
		import platform
		command = "docker build"
		# check if it's running on apple silicon mac
		if (
				platform.machine() == "arm64"
				and platform.system() == "Darwin"
				and platform.processor() == "arm"
		):
			command = f"{command}x build --platform linux/amd64"

		environment = os.environ.copy()
		environment.update(
			{"DOCKER_BUILDKIT": "1", "BUILDKIT_PROGRESS": "plain", "PROGRESS_NO_TRUNC": "1"}
		)
		filepath = os.path.join(get_image_build_context_directory(), self.filename)
		command = f"{command} -t {self._get_image_name()} --no-cache -"
		result = self._run(
			command,
			environment,
			input_filepath=filepath
		)
		return self._parse_docker_build_result(result)

	def _parse_docker_build_result(self, result):
		lines = []
		last_update = datetime.datetime.now()
		steps = dict()
		for line in result:
			line = self._ansi_escape(line)
			lines.append(line)
			# Strip appended newline
			line = line.strip()
			# Skip blank lines
			if not line:
				continue
			unusual_line = False
			try:
				# Remove step index from line
				step_index, line = line.split(maxsplit=1)
				try:
					step_index = int(step_index[1:])
				except ValueError:
					line = str(step_index) + " " + line
					step_index = sorted(steps)[-1]
					unusual_line = True

				# Parse first line and add step to steps dict
				if step_index not in steps and line.startswith("[stage-"):
					name = line.split("]", maxsplit=1)[1].strip()
					match = re.search("`#stage-(.*)`", name)
					if name.startswith("RUN") and match:
						flags = dockerfile.parse_string(name)[0].flags
						if flags:
							name = name.replace(flags[0], "")
						name = name.replace(match.group(0), "").strip().replace("   ", " \\\n  ")[4:]
						stage_slug, step_slug = match.group(1).split("-", maxsplit=1)
						step = self._find(
							self.build_steps,
							lambda x: x["stage_slug"] == stage_slug and x["step_slug"] == step_slug,
						)

						step["step_index"] = step_index
						step["command"] = name
						step["status"] = "Running"
						step["output"] = ""

						if stage_slug == "apps":
							step["command"] = f"bench get-app {step_slug}"
						steps[step_index] = step

				elif step_index in steps:
					# Parse rest of the lines
					step = self._find(self.build_steps, lambda x: x["step_index"] == step_index)
					# step = steps[step_index]
					if line.startswith("sha256:"):
						step["hash"] = line[7:]
					elif line.startswith("DONE"):
						step["status"] = "Success"
						step["duration"] = float(line.split()[1][:-1])
					elif line == "CACHED":
						step["status"] = "Success"
						step["cached"] = True
					elif line.startswith("ERROR"):
						step["status"] = "Failure"
						step["output"] += line[7:] + "\n"
					else:
						if unusual_line:
							# This line doesn't contain any docker step info
							output = line
						else:
							# Preserve additional whitespaces while splitting
							time, _, output = line.partition(" ")
						step["output"] += output + "\n"
				elif line.startswith("writing image"):
					self.docker_image_id = line.split()[2].split(":")[1]

				# Publish Progress
				if (datetime.datetime.now() - last_update).total_seconds() > 1:
					self.build_output = "".join(lines)
					yield self._generate_output()
					last_update = datetime.datetime.now()
			except Exception:
				import traceback
				print("Error in parsing line:", line)
				traceback.print_exc()

		self.build_output = "".join(lines)
		yield self._generate_output()

	def _push_docker_image(self):
		step = self._find(self.build_steps, lambda x: x["stage_slug"] == "upload")
		step["status"] = "Running"
		start_time = datetime.datetime.now()
		# publish progress
		yield self._generate_output()

		try:
			environment = os.environ.copy()

			client = docker.from_env(environment=environment)
			step["output"] = ""
			output = []
			last_update = datetime.datetime.now()

			for line in client.images.push(
					self.image_repository, self.image_tag, stream=True, decode=True, auth_config={
						"username": self.registry["username"],
						"password": self.registry["password"],
						"serveraddress": self.registry["url"],
					}
			):
				if "id" not in line.keys():
					continue

				line_output = f'{line["id"]}: {line["status"]} {line.get("progress", "")}'

				existing = self._find(output, lambda x: x["id"] == line["id"])
				if existing:
					existing["output"] = line_output
				else:
					output.append({"id": line["id"], "output": line_output})

				if (datetime.datetime.now() - last_update).total_seconds() > 1:
					step["output"] = "\n".join(ll["output"] for ll in output)
					yield self._generate_output()
					last_update = datetime.datetime.now()

			end_time = datetime.datetime.now()
			step["output"] = "\n".join(ll["output"] for ll in output)
			step["duration"] = round((end_time - start_time).total_seconds(), 1)
			step["status"] = "Success"
			yield self._generate_output()
		except Exception:
			step.status = "Failure"
			yield self._generate_output()
			raise

	def _get_image_name(self):
		return f"{self.image_repository}:{self.image_tag}"

	def _generate_output(self):
		return {
			"build_output": self.build_output,
			"build_steps": self.build_steps,
		}

	def _run(self, command, environment=None, directory=None, input_filepath=None):
		input_file = None
		if input_filepath:
			input_file = open(input_filepath, "rb")
		process = Popen(
			shlex.split(command),
			stdin=input_file,
			stdout=subprocess.PIPE,
			stderr=subprocess.STDOUT,
			env=environment,
			cwd=directory,
			universal_newlines=True
		)
		for line in process.stdout:
			yield line
		process.stdout.close()
		return_code = process.wait()
		input_file.close()
		if return_code:
			raise subprocess.CalledProcessError(return_code, command)

	def _ansi_escape(self, t: str):
		# Reference:
		# https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python
		ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
		return ansi_escape.sub("", t)

	def _find(self, iterable, predicate) -> dict:
		for item in iterable:
			if predicate(item):
				return item
		return {}

def get_image_build_context_directory():
	return os.path.join(os.getcwd(), "build_context")

def store_image_build_context(tarfile) -> str:
	filename = f"{uuid.uuid4()}.tar"
	path = os.path.join(get_image_build_context_directory(), filename)
	with open(path, "wb") as f:
		f.write(tarfile)
	return filename
