import os
from agent.base import Base
from agent.job import step


class Container(Base):
    def __init__(self, name, server):
        self.name = name
        self.server = server
        self.directory = os.path.join(self.server.containers_directory, name)
        self.config_file = os.path.join(self.directory, "config.json")
        self.image = self.config.get("image")
        if not (
            os.path.isdir(self.directory) and os.path.exists(self.config_file)
        ):
            raise Exception

    def dump(self):
        return {
            "name": self.name,
            "config": self.config,
        }

    def execute(self, command, input=None):
        return super().execute(command, directory=self.directory, input=input)

    def docker_execute(self, command, input=None):
        interactive = "-i" if input else ""
        command = f"docker exec {interactive} {self.name} {command}"
        return self.execute(command, input=input)

    @step("Start Container")
    def start(self):
        try:
            self.stop()
        except Exception:
            pass

        command = (
            "docker run -d "
            f" {self.mounts} "
            f" {self.ports} "
            f" {self.environment_variables} "
            f"--restart always --hostname {self.name} "
            f"--name {self.name} {self.config['image']}"
        )
        return self.execute(command)

    @property
    def mounts(self):
        mounts = []
        for mount in self.config["mounts"]:
            mounts.append(
                (
                    f"-v {mount['source']}:{mount['destination']}"
                    f":{mount['options']}"
                )
            )
        return " ".join(mounts)

    @property
    def ports(self):
        ports = []
        for port in self.config["ports"]:
            ports.append(
                (
                    f"-p {port['host_ip']}:{port['host_port']}"
                    f":{port['container_port']}/{port['protocol']}"
                )
            )
        return " ".join(ports)

    @property
    def environment_variables(self):
        environment_variables = []
        for key, value in self.config["environment_variables"].items():
            environment_variables.append((f"-e {key}={value}"))
        return " ".join(environment_variables)

    @step("Stop Container")
    def stop(self):
        self.execute(f"docker stop {self.name}")
        return self.execute(f"docker rm {self.name}")

    @property
    def job_record(self):
        return self.server.job_record

    @property
    def step_record(self):
        return self.server.step_record
