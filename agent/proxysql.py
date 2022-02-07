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

    def proxysql_execute(self, command):
        command = (
            "mysql -h 127.0.0.1 -P 6032 "
            f"-u frappe -p{self.proxysql_admin_password} "
            f"--disable-column-names -e '{command}'"
        )
        return self.execute(command)

    @job("Add User to ProxySQL")
    def add_user_job(self, username, password, database, backend):
        self.add_user(username, password, database, backend)

    @step("Add User to ProxySQL")
    def add_user(self, username, password, database, backend):
        backend_id = backend["id"]
        commands = [
            (
                "INSERT INTO mysql_users ( "
                "username, password, default_hostgroup, default_schema, "
                "use_ssl, max_connections) "
                "VALUES ( "
                f'"{username}", "{password}", {backend_id}, "{database}", '
                "1, 16)"
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
