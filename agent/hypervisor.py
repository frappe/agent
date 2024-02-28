import os
import shutil
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

    def dump(self):
        return {
            "name": self.name,
            "clusters": {
                name: cluster.dump() for name, cluster in self.clusters.items()
            },
        }

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

    @job("Delete Cluster")
    def delete_cluster(self, cluster):
        cluster = Cluster(cluster, self)
        cluster.stop_all_machines()
        cluster.destroy_all_machines()
        shutil.rmtree(cluster.directory)
        return self.show_vagrant_global_status()

    @step("Show Vagrant Global Status")
    def show_vagrant_global_status(self):
        self.vagrant_execute("global-status")

    @property
    def clusters(self):
        clusters = {}
        for directory in os.listdir(self.vagrant_directory):
            if os.path.isdir(os.path.join(self.vagrant_directory, directory)):
                clusters[directory] = Cluster(directory, self)
        return clusters


class Cluster(Base):
    def __init__(self, name, hypervisor, mounts=None):
        self.name = name
        self.hypervisor = hypervisor
        self.directory = os.path.join(self.hypervisor.vagrant_directory, name)
        self.config_file = os.path.join(self.directory, "config.json")
        self.vagrant_file = os.path.join(self.directory, "Vagrantfile")

    def dump(self):
        return {
            "name": self.name,
        }

    def vagrant_execute(self, command, directory=None):
        command = f"vagrant {command}"
        return self.execute(command, directory=self.directory)

    @step("Generate Vagrantfile")
    def generate_vagrantfile(self):
        config = {}
        self.hypervisor._render_template(
            "vagrant/Vagrantfile.jinja2", config, self.vagrant_file
        )
        self.vagrant_execute("validate")

    @step("Show Vagrant Status")
    def show_vagrant_status(self):
        self.vagrant_execute("status")

    @step("Stop All Machines")
    def stop_all_machines(self):
        self.vagrant_execute("halt -f")

    @step("Destroy All Machines")
    def destroy_all_machines(self):
        self.vagrant_execute("destroy -f --no-parallel")

    @property
    def job_record(self):
        return self.hypervisor.job_record

    @property
    def step_record(self):
        return self.hypervisor.step_record

    @step_record.setter
    def step_record(self, value):
        self.hypervisor.step_record = value
