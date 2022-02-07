import os

from agent.job import job, step
from agent.server import Server


class ProxySQL(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]

        self.proxysql_admin_password = self.config.get(
            "proxysql_admin_password"
        )
        self.job = None
        self.step = None
