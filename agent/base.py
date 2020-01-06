import json
import subprocess
import traceback
from datetime import datetime


class Base:
    def __init__(self):
        self.directory = None
        self.config_file = None
        self.name = None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    def execute(self, command, directory=None):
        directory = directory or self.directory
        start = datetime.now()
        data = {"command": command, "directory": directory, "start": start}
        try:
            process = subprocess.run(
                command,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=directory,
                shell=True,
            )
        except subprocess.CalledProcessError as e:
            end = datetime.now()
            data.update({"duration": end - start, "end": end})
            data.update(
                {
                    "output": e.output.decode().strip(),
                    "returncode": e.returncode,
                    "traceback": "".join(traceback.format_exc()),
                }
            )
            raise AgentException(data)

        end = datetime.now()
        data.update(
            {
                "duration": end - start,
                "end": end,
                "output": process.stdout.decode().strip(),
            }
        )
        return data

    @property
    def config(self):
        with open(self.config_file, "r") as f:
            return json.load(f)

    def setconfig(self, value):
        with open(self.config_file, "w") as f:
            json.dump(value, f, indent=4, sort_keys=True)


class AgentException(Exception):
    def __init__(self, data):
        self.data = data
