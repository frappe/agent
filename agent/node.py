import json
import os
import shutil
from typing import Dict

from agent.server import Server
from agent.job import Job, Step, job, step
from agent.container import Container


class Node(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]
        self.containers_directory = self.config.get(
            "containers_directory", os.path.join(self.directory, "containers")
        )
        self.archived_directory = os.path.join(
            os.path.dirname(self.containers_directory), "archived"
        )
        self.job = None
        self.step = None

    def dump(self):
        return {
            "name": self.name,
            "containers": {
                name: container.dump()
                for name, container in self.containers.items()
            },
            "config": self.config,
        }

    @job("New Container", priority="low")
    def new_container(self, name, config, registry):
        if registry:
            self.docker_login(registry)
        self.container_init(name, config)
        bench = Container(name, self)
        bench.start()

    @step("Initialize Container")
    def container_init(self, name, config):
        if not os.path.exists(self.containers_directory):
            os.mkdir(self.containers_directory)

        container_directory = os.path.join(self.containers_directory, name)
        os.mkdir(container_directory)

        config_file = os.path.join(container_directory, "config.json")
        with open(config_file, "w") as f:
            json.dump(config, f, indent=1, sort_keys=True)

    @job("Archive Container", priority="low")
    def archive_container(self, name):
        container_directory = os.path.join(self.containers_directory, name)
        if not os.path.exists(container_directory):
            return
        container = Container(name, self)
        container.stop()
        self.container_exists(name)
        self.move_container_to_archived_directory(name)

    @step("Move Container to Archived Directory")
    def move_container_to_archived_directory(self, container_name):
        if not os.path.exists(self.archived_directory):
            os.mkdir(self.archived_directory)
        target = os.path.join(self.archived_directory, container_name)
        if os.path.exists(target):
            shutil.rmtree(target)
        container_directory = os.path.join(
            self.containers_directory, container_name
        )
        self.execute(f"mv {container_directory} {self.archived_directory}")

    @property
    def containers(self) -> Dict[str, Container]:
        containers = {}
        for directory in os.listdir(self.containers_directory):
            try:
                containers[directory] = Container(directory, self)
            except Exception:
                pass
        return containers

    @property
    def job_record(self):
        if self.job is None:
            self.job = Job()
        return self.job

    @property
    def step_record(self):
        if self.step is None:
            self.step = Step()
        return self.step
