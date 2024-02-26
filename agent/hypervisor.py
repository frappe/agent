import os
from agent.job import job, step
from agent.server import Server
from agent.base import Base


class Hypervisor(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]

        self.vagrant_directory = self.config["vagrant_directory"]
        self.job = None
        self.step = None

    def vagrant_execute(self, command, directory=None):
        command = f"vagrant {command}"
        return self.execute(
            command, directory=directory or self.vagrant_directory
        )

    @job("Create Cluster")
    def create_cluster(self, name):
        cluster = Cluster(name, self)
        if os.path.exists(cluster.directory):
            raise Exception("Cluster already exists")
        os.makedirs(cluster.directory)
        cluster.generate_vagrantfile()
        return cluster.show_vagrant_status()


class Cluster(Base):
    def __init__(self, name, hypervisor, mounts=None):
        self.name = name
        self.hypervisor = hypervisor
        self.directory = os.path.join(self.hypervisor.vagrant_directory, name)
        self.vagrant_file = os.path.join(self.directory, "Vagrantfile")

    def vagrant_execute(self, command, directory=None):
        command = f"vagrant {command}"
        return self.execute(command, directory=self.directory)

    @step("Generate Vagrantfile")
    def generate_vagrantfile(self):
        config = {}
        self.hypervisor._render_template(
            "vagrant/Vagrantfile.jinja2", config, self.vagrant_file
        )

    @step("Show Vagrant Status")
    def show_vagrant_status(self):
        self.vagrant_execute("status")

    @property
    def job_record(self):
        return self.hypervisor.job_record

    @property
    def step_record(self):
        return self.hypervisor.step_record

    @step_record.setter
    def step_record(self, value):
        self.hypervisor.step_record = value
