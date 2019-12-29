import json
import subprocess


class Base:
    def __init__(self):
        self.directory = None
        self.config_file = None
        self.name = None

    def execute(self, command, directory=None):
        directory = directory or self.directory
        try:
            process = subprocess.run(
                command,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=directory,
                shell=True,
            )
            return process.stdout.decode().strip()
        except subprocess.CalledProcessError as e:
            raise e

    @property
    def config(self):
        with open(self.config_file, "r") as f:
            return json.load(f)

    @config.setter
    def config(self, value):
        with open(self.config_file, "w") as f:
            json.dump(value, f, indent=4, sort_keys=True)
