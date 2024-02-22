import json
import os
import subprocess
import traceback
from datetime import datetime
from functools import partial
from agent.job import connection


class Base:
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
    ):
        directory = directory or self.directory
        start = datetime.now()
        self.skip_output_log = skip_output_log
        self.data = {
            "command": command,
            "directory": directory,
            "start": start,
            "status": "Running",
        }
        self.log()
        try:
            output = self.run_subprocess(command, directory, input, executable)
        except subprocess.CalledProcessError as e:
            output = e.output
            self.data.update(
                {
                    "status": "Failure",
                    "returncode": e.returncode,
                    "traceback": "".join(traceback.format_exc()),
                }
            )
            raise AgentException(self.data)
        else:
            self.data.update({"status": "Success"})
        finally:
            end = datetime.now()
            self.data.update(
                {
                    "duration": end - start,
                    "end": end,
                    "output": output,
                }
            )
            self.log()
        return self.data

    def run_subprocess(self, command, directory, input, executable):
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
            retcode = process.poll()
            # This is equivalent of check=True
            # Raise an exception if the process returns a non-zero return code
            if retcode:
                raise subprocess.CalledProcessError(
                    retcode, command, output=output
                )
        return output

    def parse_output(self, process):
        lines = []
        # This is equivalent of remove_crs
        # Make sure output matches what'll be shown in the terminal
        # This won't work for top, htop etc, but good enough to handle progress bars
        if process.stdout:
            line = ""
            for char in iter(partial(process.stdout.read, 1), ""):
                char = char.decode(errors="replace"))
                if char == "" and process.poll() is not None:
                    break
                elif char == "\r":
                    # Publish output and then wipe current line.
                    # Include the overwritten line in the output
                    self.publish_output(lines + [line])
                    line = ""
                elif char == "\n":
                    lines.append(line)
                    line = ""
                    self.publish_output(lines)
                else:
                    line += char
            if line:
                lines.append(line)
            self.publish_output(lines)
        return "\n".join(lines)

    def publish_output(self, lines):
        output = "\n".join(lines)
        self.data.update({"output": output})
        self.update_redis()

    def update_redis(self):
        if not self.redis_key:
            return
        value = json.dumps(self.data, default=str)

        if "output" in self.data:
            self.redis.lset(self.redis_key, -1, value)
        else:
            self.redis.rpush(self.redis_key, value)
        self.redis.expire(self.redis_key, 60 * 60 * 6)

    @property
    def redis_key(self):
        if self.job_record and getattr(self.job_record, "model", None):
            key = f"agent:job:{self.job_record.model.id}"
            if self.step_record and getattr(self.step_record, "model", None):
                return f"{key}:step:{self.step_record.model.id}"
            return key
        return None

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
        def path(file):
            return os.path.join(self.logs_directory, file)

        def modified_time(file):
            return os.path.getctime(path(file))

        try:
            log_files = sorted(
                os.listdir(self.logs_directory),
                key=modified_time,
                reverse=True,
            )
            payload = []

            for x in log_files:
                stats = os.stat(path(x))
                payload.append(
                    {
                        "name": x,
                        "size": stats.st_size / 1000,
                        "created": str(datetime.fromtimestamp(stats.st_ctime)),
                        "modified": str(
                            datetime.fromtimestamp(stats.st_mtime)
                        ),
                    }
                )

            return payload

        except FileNotFoundError:
            return []

    def retrieve_log(self, name):
        if name not in {x["name"] for x in self.logs}:
            return ""
        log_file = os.path.join(self.logs_directory, name)
        return open(log_file).read()


class AgentException(Exception):
    def __init__(self, data):
        self.data = data
