from __future__ import annotations

import os

from agent.job import job, step
from agent.server import Server


class ProxySQL(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]

        self.proxysql_admin_password = self.config.get("proxysql_admin_password")
        self.job = None
        self.step = None

    def proxysql_execute(self, command):
        command = (
            "mysql -h 127.0.0.1 -P 6032 "
            f"-u frappe -p{self.proxysql_admin_password} "
            f"--disable-column-names -e '{command}'"
        )
        return self.execute(command)

    @job("Add User to ProxySQL")
    def add_user_job(
        self,
        username: str,
        password: str,
        database: str,
        max_connections: int,
        backend: dict,
    ):
        self.add_backend(backend)
        self.add_user(username, password, database, max_connections, backend)

    @job("Add Backend to ProxySQL")
    def add_backend_job(self, backend):
        self.add_backend(backend)

    @step("Add Backend to ProxySQL")
    def add_backend(self, backend):
        backend_id = backend["id"]
        backend_ip = backend["ip"]
        if self.proxysql_execute(f"SELECT 1 from mysql_servers where hostgroup_id = {backend_id}")["output"]:
            return
        commands = [
            ("INSERT INTO mysql_servers (hostgroup_id, hostname) " f'VALUES ({backend_id}, "{backend_ip}")'),
            "LOAD MYSQL SERVERS TO RUNTIME",
            "SAVE MYSQL SERVERS TO DISK",
        ]
        for command in commands:
            self.proxysql_execute(command)

    @step("Add User to ProxySQL")
    def add_user(self, username: str, password: str, database: str, max_connections: int, backend: dict):
        backend_id = backend["id"]
        commands = [
            (
                "INSERT INTO mysql_users ( "
                "username, password, default_hostgroup, default_schema, "
                "use_ssl, max_connections) "
                "VALUES ( "
                f'"{username}", "{password}", {backend_id}, "{database}", '
                f"1, {max_connections})"
            ),
            "LOAD MYSQL USERS TO RUNTIME",
            "SAVE MYSQL USERS FROM RUNTIME",
            "SAVE MYSQL USERS TO DISK",
        ]
        for command in commands:
            self.proxysql_execute(command)

    @job("Remove User from ProxySQL")
    def remove_user_job(self, username):
        self.remove_user(username)

    @step("Remove User from ProxySQL")
    def remove_user(self, username):
        commands = [
            f'DELETE FROM mysql_users WHERE username = "{username}"',
            "LOAD MYSQL USERS TO RUNTIME",
            "SAVE MYSQL USERS FROM RUNTIME",
            "SAVE MYSQL USERS TO DISK",
        ]
        for command in commands:
            self.proxysql_execute(command)
