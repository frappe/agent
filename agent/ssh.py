import os

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
            self.docker_execute(
                f"""bash -c 'echo "{value}" > /home/{name}/.ssh/{key}'""",
            )

    @step("Add Principal to User")
    def add_principal(self, name, principal, ssh):
        force_command = f"ssh -vvv frappe@{ssh['ip']} -p {ssh['port']}"
        bash_command = (
            f'echo command=\\"{force_command}\\" {principal} '
            f"> /etc/ssh/principals/{name}"
        )
        return self.docker_execute(f"bash -c '{bash_command}'")

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
