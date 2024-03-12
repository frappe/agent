import json
import os
import shutil
from csv import DictReader
from itertools import groupby
from operator import itemgetter

from agent.job import job, step
from agent.server import Server
from agent.base import Base

IGNORE_VAGRANT_TYPES = [
    "action",
    "box-info",
    "Description",
    "metadata",
    "ui",
]


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
            "global-status": self._parse_vagrant_global_status(
                self.vagrant_execute("--machine-readable global-status")[
                    "output"
                ]
            ),
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

    def _parse_vagrant_machine_readable(self, output):
        parsed = DictReader(
            output.splitlines(),
            fieldnames=["timestamp", "target", "type", "data"],
        )
        parsed = list(
            filter(lambda row: row["type"] not in IGNORE_VAGRANT_TYPES, parsed)
        )
        return parsed

    def _parse_vagrant_global_status(self, output):
        statuses = []
        status = {}
        for row in self._parse_vagrant_machine_readable(output):
            KIND_MAP = {
                "machine-id": "name",
                "state": "state",
                "machine-home": "home",
                "provider-name": "provider",
            }
            status[KIND_MAP[row["type"]]] = row["data"]
            if len(status) == 4:
                statuses.append(status)
                status = {}
        return statuses


class Cluster(Base):
    def __init__(self, name, hypervisor, mounts=None):
        self.name = name
        self.hypervisor = hypervisor
        self.directory = os.path.join(self.hypervisor.vagrant_directory, name)
        self.config_file = os.path.join(self.directory, "config.json")
        self.vagrant_file = os.path.join(self.directory, "Vagrantfile")

    def dump(self):
        self._parse_vagrant_status(
            self.vagrant_execute("--machine-readable status")["output"]
        )
        return {
            "name": self.name,
            "machines": {
                name: {
                    "machine": Machine(name, self).dump(),
                    "config": machine,
                }
                for name, machine in self.config["machines"].items()
            },
            "status": self._parse_vagrant_status(
                self.vagrant_execute("--machine-readable status")["output"]
            ),
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
        self.show_vagrant_status()
        machine = Machine(name, self)
        return machine.start()

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
        machine = Machine(name, self)
        machine.terminate()
        self.delete_machine_config(name)
        self.generate_vagrantfile()
        return self.show_vagrant_status()

    @step("Delete Machine Config")
    def delete_machine_config(self, name):
        config = self.config
        config["machines"].pop(name)
        self.setconfig(config)

    @step("Update Machine Config")
    def update_machine_config(self, name, new_config):
        config = self.config
        config["machines"][name].update(new_config)
        self.setconfig(config)

    @job("Reload Cluster")
    def reload_job(self):
        self.generate_vagrantfile()
        return self.reload()

    @step("Reload Cluster")
    def reload(self):
        return self.vagrant_execute("reload -f")

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

    def _parse_vagrant_status(self, output):
        parsed = self.hypervisor._parse_vagrant_machine_readable(output)
        statuses = []
        for target, rows in groupby(parsed, itemgetter("target")):
            status = {row["type"]: row["data"] for row in rows}
            statuses.append(
                {
                    "name": target,
                    "state": status.get("state"),
                    "provider": status.get("provider-name"),
                }
            )
        return statuses


class Machine(Base):
    def __init__(self, name, cluster):
        self.name = name
        self.cluster = cluster

    def execute(self, command):
        return super().execute(command, directory=self.cluster.directory)

    def vagrant_execute(self, command):
        command = f"vagrant {command} {self.name}"
        return self.execute(command)

    def dump(self):
        return {
            "name": self.name,
            "status": self.cluster._parse_vagrant_status(
                self.vagrant_execute(f"--machine-readable status {self.name}")[
                    "output"
                ]
            ),
        }

    @job("Start Machine")
    def start_job(self):
        return self.start()

    @step("Start Machine")
    def start(self):
        return self.vagrant_execute("up")

    @job("Stop Machine")
    def stop_job(self):
        return self.stop()

    @step("Stop Machine")
    def stop(self):
        return self.vagrant_execute("halt -f")

    @job("Reboot Machine")
    def reboot_job(self):
        return self.reboot()

    @step("Reboot Machine")
    def reboot(self):
        return self.vagrant_execute("reload -f")

    @job("Resize Machine")
    def resize_job(self, size):
        self.cluster.update_machine_config(self.name, {"size": size})
        self.cluster.generate_vagrantfile()
        return self.reload()

    @step("Reload Machine")
    def reload(self):
        return self.vagrant_execute("reload -f")

    @job("Terminate Machine")
    def terminate_job(self):
        return self.terminate()

    @step("Terminate Machine")
    def terminate(self):
        return self.vagrant_execute("destroy -f")

    @job("Resize Disk")
    def resize_disk_job(self, index, size):
        disks = self.cluster.config["machines"][self.name]["disks"]
        disks[index]["size"] = size
        self.cluster.update_machine_config(self.name, {"disks": disks})
        self.cluster.generate_vagrantfile()
        return self.resize_disk(index, size)

    @step("Resize Disk")
    def resize_disk(self, index, size):
        payload = {
            "execute": "query-block",
        }
        disks = json.loads(
            self.execute(
                f"virsh qemu-monitor-command {self.name} '{json.dumps(payload)}'"
            )["output"]
        )
        node = disks["return"][index]["inserted"]["node-name"]
        payload = {
            "execute": "block_resize",
            "arguments": {
                "node-name": node,
                "size": size * (10**9),
            },
        }
        return self.execute(
            f"virsh qemu-monitor-command {self.name} '{json.dumps(payload)}'"
        )

    @property
    def job_record(self):
        return self.cluster.hypervisor.job_record

    @property
    def step_record(self):
        return self.cluster.hypervisor.step_record

    @step_record.setter
    def step_record(self, value):
        self.cluster.hypervisor.step_record = value
