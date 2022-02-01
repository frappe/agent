import os
import tempfile

from agent.job import job, step
from agent.server import Server


class SSHProxy(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]

        self.ssh_directory = os.path.join(self.directory, "ssh")

        self.job = None
        self.step = None

    def docker_execute(self, command):
        command = f"docker exec ssh {command}"
        return self.execute(command)

    @job("Add User to Proxy")
    def add_user_job(self, name, principal, ssh, certificate):
        self.add_user(name)
        self.add_certificate(name, certificate)
        self.add_principal(name, principal, ssh)

    @step("Add User to Proxy")
    def add_user(self, name):
        return self.docker_execute(f"useradd -m -p '*' {name}")

    @step("Add Certificate to User")
    def add_certificate(self, name, certificate):
        self.docker_execute(f"mkdir /home/{name}/.ssh")
        for key, value in certificate.items():
            source = tempfile.mkstemp()[1]
            with open(source, "w") as f:
                f.wrte(value)
            target = f"/home/{name}/.ssh/{key}.pub"
            self.execute(f"docker cp {source} {target}")
            self.docker_execute(f"chown {name}:{name} {target}")
            os.remove(source)

    @step("Add Principal to User")
    def add_principal(self, name, principal, ssh):
        cd_command = "cd frappe-bench; exec bash --login"
        force_command = (
            f"ssh frappe@{ssh['ip']} -p {ssh['port']} -t '{cd_command}'"
        )
        principal_line = f'restrict,pty,command="{force_command}" {principal}'
        source = tempfile.mkstemp()[1]
        with open(source, "w") as f:
            f.wrte(principal_line)
        target = f"/etc/ssh/principals/{name}"
        self.execute(f"docker cp {source} {target}")
        self.docker_execute(f"chown root:root {target}")
        os.remove(source)

    @job("Remove User from Proxy")
    def remove_user_job(self, name):
        self.remove_user(name)
        self.remove_principal(name)

    @step("Remove User from Proxy")
    def remove_user(self, name):
        return self.docker_execute(f"userdel -f -r {name}")

    @step("Remove Principal from User")
    def remove_principal(self, name):
        command = f"rm /etc/ssh/principals/{name}"
        return self.docker_execute(command)
