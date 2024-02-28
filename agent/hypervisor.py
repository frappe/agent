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
        cluster.setconfig(
            {
                "machines": {},
            }
        )
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
            "machines": {
                name: {
                    "machine": Machine(name, self).dump(),
                    "config": machine,
                }
                for name, machine in self.config["machines"].items()
            },
        }

    def vagrant_execute(self, command, directory=None):
        command = f"vagrant {command}"
        return self.execute(command, directory=self.directory)

    @step("Generate Vagrantfile")
    def generate_vagrantfile(self):
        config = {
            "machines": self.config["machines"].values(),
        }
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

    @job("Create Machine")
    def create_machine(self, name, image, size, network, disks, user_data):
        self.create_machine_config(
            name, image, size, network, disks, user_data
        )
        self.generate_vagrantfile()
        return self.show_vagrant_status()

    @step("Create Machine Config")
    def create_machine_config(
        self, name, image, size, network, disks, user_data
    ):
        config = self.config
        config["machines"][name] = {
            "name": name,
            "image": image,
            "size": size,
            "network": network,
            "disks": disks,
            "user_data": user_data,
        }
        self.setconfig(config)

    @job("Delete Machine")
    def delete_machine(self, name):
        self.delete_machine_config(name)
        self.generate_vagrantfile()
        return self.show_vagrant_status()

    @step("Delete Machine Config")
    def delete_machine_config(self, name):
        config = self.config
        config["machines"].pop(name)
        self.setconfig(config)

    @property
    def machines(self):
        return {
            machine: Machine(machine, self)
            for machine in self.config.get("machines", [])
        }

    @property
    def job_record(self):
        return self.hypervisor.job_record

    @property
    def step_record(self):
        return self.hypervisor.step_record

    @step_record.setter
    def step_record(self, value):
        self.hypervisor.step_record = value


class Machine(Base):
    def __init__(self, name, cluster):
        self.name = name
        self.cluster = cluster

    def vagrant_execute(self, command):
        command = f"vagrant {command} {self.name}"
        return self.execute(command, directory=self.cluster.directory)

    def dump(self):
        return {
            "name": self.name,
        }

    @job("Start Machine")
    def start_job(self):
        return self.start()

    @step("Start Machine")
    def start(self):
        return self.vagrant_execute("up")

    @property
    def job_record(self):
        return self.cluster.hypervisor.job_record

    @property
    def step_record(self):
        return self.cluster.hypervisor.step_record

    @step_record.setter
    def step_record(self, value):
        self.cluster.hypervisor.step_record = value
