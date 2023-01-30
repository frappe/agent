import json
import os
import subprocess
import traceback
from datetime import datetime
from functools import partial


class Base:
    job = None
    step = None
    data = {}
    skip_output_log = False

    def __init__(self):
        self.directory = None
        self.config_file = None
        self.name = None
        self

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    def execute(self, command, directory=None, input=None):
        directory = directory or self.directory
        start = datetime.now()
        self.data = {
            "command": command,
            "directory": directory,
            "start": start,
        }
        self.log()
        try:
            self.run_subprocess(command, directory, input)
        except subprocess.CalledProcessError as e:
            self.data.update(
                {
                    "returncode": e.returncode,
                    "traceback": "".join(traceback.format_exc()),
                }
            )
            raise AgentException(self.data)
        finally:
            end = datetime.now()
            self.data.update({"duration": end - start, "end": end})
            self.log()
        return self.data

    def run_subprocess(self, command, directory, input):
        with subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE if input else None,
            cwd=directory,
            shell=True,
        ) as process:
            if input:
                process._stdin_write(input.encode())

            self.parse_output(process)
            retcode = process.poll()
            if retcode:
                raise subprocess.CalledProcessError(retcode, process.args)

    def parse_output(self, process):
        lines = []
        if process.stdout:
            line = ""
            for char in iter(partial(process.stdout.read, 1), ""):
                char = char.decode()
                if char == "" and process.poll() is not None:
                    break
                elif char == "\r":
                    lines[-1] = line
                    line = ""
                    self.publish_output(lines)
                elif char == "\n":
                    lines.append(line)
                    line = ""
                    self.publish_output(lines)
                else:
                    line += char
            if line:
                lines.append(line)
            self.publish_output(lines)

    def publish_output(self, lines):
        output = "\n".join(lines)
        self.data["output"] = output
        print(output)

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
            data.pop("output", None)
        print(json.dumps(data, default=str))

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
