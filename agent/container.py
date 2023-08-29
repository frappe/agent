import os
import tempfile
from agent.base import Base
from agent.job import step


class Container(Base):
    def __init__(self, name, server):
        self.name = name
        self.server = server
        self.directory = os.path.join(self.server.containers_directory, name)
        self.config_file = os.path.join(self.directory, "config.json")
        self.container_file = os.path.join(
            self.server.systemd_directory,
            f"{self.name}.container",
        )
        self.network_service = f"overlay-{self.config['network']}.service"
        self.attach_script = os.path.join(self.directory, "attach.sh")
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
        self.create_mount_directories()
        self.create_attach_script()
        quadlet_result = self.create_container_file()
        self.reload_systemd()
        self.start_container_unit()
        return quadlet_result

    def create_mount_directories(self):
        for mount in self.config["mounts"]:
            os.makedirs(mount["source"], exist_ok=True)

    def create_container_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_unit = os.path.join(
                temporary_directory, f"{self.name}.container"
            )
            self.server._render_template(
                "container/container.jinja2",
                {
                    "name": self.name,
                    "image": self.image,
                    "mounts": self.mounts,
                    "ports": self.ports,
                    "environment_variables": self.environment_variables,
                    "attach_script": self.attach_script,
                },
                temporary_unit,
            )

            quadlet = (
                "/usr/lib/systemd/system-generators/podman-system-generator"
            )
            quadlet_result = self.execute(
                f"QUADLET_UNIT_DIRS={temporary_directory} {quadlet} -dryrun"
            )
            os.rename(temporary_unit, self.container_file)
        return quadlet_result

    def reload_systemd(self):
        self.execute("sudo systemctl daemon-reload")

    def start_container_unit(self):
        name = self.name
        self.execute(f"sudo systemctl start {name}.service")

    @step("Create Overlay Network")
    def create_overlay_network(self):
        self.create_network_service()
        self.reload_systemd()
        self.start_network_service()

    def create_network_service(self):
        network_service_file = os.path.join(
            os.path.join(self.server.systemd_directory, self.network_service)
        )
        self.server._render_template(
            "container/network.jinja2",
            {
                "namespace": self.config["network"],
                "network": self.config["network"],
            },
            network_service_file,
        )
        # Ask systemd to create a symlink to the network service file
        self.execute(f"sudo systemctl enable {network_service_file}")

    def start_network_service(self):
        self.execute(f"sudo systemctl start {self.network_service}")

    def create_attach_script(self):
        self.server._render_template(
            "container/attach.jinja2",
            {
                "namespace": self.config["network"],
                "name": self.name,
                "ip_address": self.config["ip_address"],
                "mac_address": self.config["mac_address"],
            },
            self.attach_script,
        )

    @step("Add ARP and FDB entries")
    def add_arp_and_fdb_entries(self):
        namespace = self.config["network"]
        network = self.config["network"]
        results = []
        for peer in self.config["peers"]:
            commands = [
                f"sudo ip netns exec {namespace} ip neighbor add {peer['ip_address']} lladdr {peer['mac_address']} dev vx-{network} nud permanent",
                f"sudo ip netns exec {namespace} bridge fdb add {peer['mac_address']} dev vx-{network} self dst {peer['node_ip_address']} vni 1 port 4789",
            ]
            for command in commands:
                results.append(self.execute(command))
        return results

    @step("Delete Overlay Network")
    def delete_overlay_network(self):
        namespace = self.config["network"]
        commands = [
            f"sudo ip netns delete {namespace}",
        ]
        results = []
        for command in commands:
            results.append(self.execute(command))
        return results

    @property
    def mounts(self):
        return [
            f"{mount['source']}:{mount['destination']}:{mount['options']}"
            for mount in self.config["mounts"]
        ]

    @property
    def ports(self):
        return [
            (
                f"{port['host_ip']}:{port['host_port']}"
                f":{port['container_port']}/{port['protocol']}"
            )
            for port in self.config["ports"]
        ]

    @property
    def environment_variables(self):
        return [
            (f"{key}={value}")
            for key, value in self.config["environment_variables"].items()
        ]

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
