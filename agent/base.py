from __future__ import annotations

import json
import subprocess
import traceback
from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING

import redis

from agent.job import connection
from agent.log_browser import LogBrowser
from agent.utils import get_execution_result

if TYPE_CHECKING:
    from typing import Any

    from agent.job import Job, Step


class Base:
    if TYPE_CHECKING:
        job_record: Job | None
        step_record: Step | None

    def __init__(self):
        self.directory = None
        self.config_file = None
        self.name = None
        self.data = {}

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    def execute(
        self,
        command,
        directory=None,
        input=None,
        skip_output_log=False,
        executable=None,
        non_zero_throw=True,
    ):
        directory = directory or self.directory
        start = datetime.now()
        self.skip_output_log = skip_output_log
        self.data = get_execution_result(command, directory, start)
        self.log()
        output = ""
        try:
            output, returncode = self.run_subprocess(
                command,
                directory,
                input,
                executable,
                non_zero_throw,
            )
        except subprocess.CalledProcessError as e:
            output = str(e.output or "")
            returncode = e.returncode
            self.data.update(
                {
                    "status": "Failure",
                    "traceback": "".join(traceback.format_exc()),
                }
            )
            raise AgentException(self.data) from e
        else:
            self.data.update({"status": "Success"})
        finally:
            end = datetime.now()
            self.data.update(
                {
                    "returncode": returncode,
                    "duration": end - start,
                    "end": end,
                    "output": output,
                }
            )
            self.log()
        return self.data

    def run_subprocess(self, command, directory, input, executable, non_zero_throw=True):
        # Start a child process and start reading output immediately
        with subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE if input else None,
            cwd=directory,
            shell=True,
            executable=executable,
        ) as process:
            if input:
                process._stdin_write(input.encode())

            output = self.parse_output(process)
            returncode = process.poll() or 0
            # This is equivalent of check=True
            # Raise an exception if the process returns a non-zero return code
            if non_zero_throw and returncode:
                raise subprocess.CalledProcessError(returncode, command, output=output)
        return output, returncode

    def parse_output(self, process) -> str:
        if not process.stdout:
            return ""

        line = b""
        lines = []
        # This is equivalent of remove_crs
        # Make sure output matches what'll be shown in the terminal
        # This won't work for top, htop etc, but good enough to handle progress bars
        for char in iter(partial(process.stdout.read, 1), b""):
            if char == b"" and process.poll() is not None:
                break
            if char == b"\r":
                # Publish output and then wipe current line.
                # Include the overwritten line in the output
                self.publish_lines([*lines, line.decode(errors="replace")])
                line = b""
            elif char == b"\n":
                lines.append(line.decode(errors="replace"))
                line = b""
                self.publish_lines(lines)
            else:
                line += char

        if line:
            lines.append(line.decode(errors="replace"))
        self.publish_lines(lines)
        return "\n".join(lines)

    def publish_lines(self, lines: list[str]):
        output = "\n".join(lines)
        self.data.update({"output": output})
        self.update_redis()

    def publish_data(self, data: Any):
        if not isinstance(data, str):
            data = json.dumps(data, default=str)

        self.data.update({"output": data})
        self.update_redis()

    def update_redis(self):
        if not (redis_key := self.get_redis_key()):
            return

        value = json.dumps(self.data, default=str)
        self.push_redis_value(redis_key, value)
        self.redis.expire(redis_key, 60 * 60 * 6)

    def push_redis_value(self, key: str, value: str):
        if "output" not in self.data:
            self.redis.rpush(key, value)

        try:
            self.redis.lset(key, -1, value)
        except redis.exceptions.ResponseError as e:
            if "no such key" in str(e):
                self.redis.rpush(key, value)

    def get_redis_key(self):
        if not self.job_record:
            return None

        if not hasattr(self.job_record, "model"):
            return None

        key = f"agent:job:{self.job_record.model.id}"
        if self.step_record and hasattr(self.step_record, "model"):
            return f"{key}:step:{self.step_record.model.id}"

        return key

    @property
    def redis(self):
        return connection()

    @property
    def config(self):
        with open(self.config_file, "r") as f:
            return json.load(f)

    def setconfig(self, value, indent=1):
        with open(self.config_file, "w") as f:
            json.dump(value, f, indent=indent, sort_keys=True)

    def log(self):
        data = self.data.copy()
        if self.skip_output_log:
            data.update({"output": ""})
        print(json.dumps(data, default=str))
        self.update_redis()

    @property
    def logs(self):
        log_browser = LogBrowser(self.logs_directory)
        return log_browser.logs

    def retrieve_log(self, name):
        log_browser = LogBrowser(self.logs_directory)
        return log_browser.retrieve_log(name)

    def retrieve_merged_log(
        self, name, page_start=0, page_length=10, log_level=None, search_query=None, order_by=None
    ):
        log_browser = LogBrowser(self.logs_directory)
        return log_browser.retrieve_merged_log(
            name, page_start, page_length, log_level, search_query, order_by
        )


class AgentException(Exception):
    def __init__(self, data):
        self.data = data
