import os

from agent.server import Server


class SSHProxy(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]

        self.ssh_directory = os.path.join(self.directory, "ssh")

        self.job = None
        self.step = None
