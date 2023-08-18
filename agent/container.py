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

    @step("Create Overlay Network")
    def create_overlay_network(self):
        namespace = self.config["network"]
        network = self.config["network"]
        commands = [
            # Create Bridge
            f"sudo ip netns add {namespace}",
            f"sudo ip netns exec {namespace} ip link add dev br0 type bridge",
            f"sudo ip netns exec {namespace} ip addr add dev br0 10.0.0.0/8",
            # Attach VxLan to Bridge
            f"sudo ip link add dev vx-{network} type vxlan id 1 proxy learning l2miss l3miss dstport 4789",
            f"sudo ip link set vx-{network} netns {namespace}",
            f"sudo ip netns exec {namespace} ip link set vx-{network} master br0",
            # Bring up interfaces
            f"sudo ip netns exec {namespace} ip link set vx-{network} up",
            f"sudo ip netns exec {namespace} ip link set br0 up",
        ]
        results = []
        for command in commands:
            results.append(self.execute(command))
        return results

    @step("Attach Container to Overlay Network")
    def attach_to_overlay_network(self):
        namespace = self.config["network"]
        container_namespace_path = self.execute(
            f"docker inspect --format='{{{{ .NetworkSettings.SandboxKey }}}}' {self.name}"
        )["output"]
        container_namespace = container_namespace_path.split("/")[-1]

        commands = [
            f"sudo ln -sf /var/run/docker/netns/{container_namespace} /var/run/netns/{container_namespace}",
            # create veth interfaces
            f"sudo ip link add dev veth-1-{self.name} mtu 1450 type veth peer name veth-2-{self.name} mtu 1450",
            # attach first peer to the bridge in our overlay namespace
            f"sudo ip link set dev veth-1-{self.name} netns {namespace}",
            f"sudo ip netns exec {namespace} ip link set veth-1-{self.name} master br0",
            f"sudo ip netns exec {namespace} ip link set veth-1-{self.name} up",
            # crate symlink to be able to use ip netns commands
            f"sudo ip link set dev veth-2-{self.name} netns {container_namespace}",
            # move second peer tp container network namespace and configure it
            f"sudo ip netns exec {container_namespace} ip link set dev veth-2-{self.name} name eth0 address {self.config['mac_address']}",
            f"sudo ip netns exec {container_namespace} ip addr add dev eth0 {self.config['ip_address']}/8",
            f"sudo ip netns exec {container_namespace} ip link set dev eth0 up",
            # Clean up symlink
            f"sudo rm /var/run/netns/{container_namespace}",
        ]
        results = []
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
