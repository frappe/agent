from __future__ import annotations

import os
from typing import TYPE_CHECKING

from agent.base import Base
from agent.job import step

if TYPE_CHECKING:
    from agent.server import Server


class Devbox(Base):
    def __init__(
        self,
        devbox_name: str,
        server: Server,
        vnc_password: str | None = None,
        codeserver_password: str | None = None,
        websockify_port: int | None = None,
        vnc_port: int | None = None,
        codeserver_port: int | None = None,
        browser_port: int | None = None,
    ):
        self.devbox_name = devbox_name
        self.server = server
        self.directory = os.path.join(self.server.devboxes_directory, devbox_name)
        self.websockify_port = websockify_port
        self.vnc_port = vnc_port
        self.codeserver_port = codeserver_port
        self.browser_port = browser_port
        self.vnc_password = vnc_password
        self.codeserver_password = codeserver_password
        self.job = None
        self.step = None
        self.status = None

    @property
    def job_record(self):
        return self.server.job_record

    @property
    def step_record(self):
        return self.server.step_record

    @step_record.setter
    def step_record(self, value):
        self.server.step_record = value

    @step("Devbox Setup NGINX")
    def setup_nginx(self, is_devbox=False):
        from filelock import FileLock

        with FileLock(os.path.join(self.directory, "nginx.config.lock")):
            self.generate_nginx_config()
        return self.server._reload_nginx()

    def generate_nginx_config(self):
        config = {
            "devbox_name": self.devbox_name,
            "websockify_port": self.websockify_port,
            "codeserver_port": self.codeserver_port,
            "browser_port": self.browser_port,
        }
        nginx_config = os.path.join(self.directory, "nginx.conf")

        self.server._render_template("devbox/nginx.conf.jinja2", config, nginx_config)

    @step("Create Devbox Database Volume")
    def create_devbox_database_volume(self):
        command = f"docker volume create {self.devbox_name}_db-data"
        return self.execute(command)

    @step("Create Devbox Home Volume")
    def create_devbox_home_volume(self):
        command = f"docker volume create {self.devbox_name}_home"
        return self.execute(command)

    @step("Run Devbox")
    def run_devbox(self):
        command = (
            f'docker run --security-opt="no-new-privileges=false" -d --rm --name {self.devbox_name} '
            f"-p {self.websockify_port}:6969 "
            f"-p {self.codeserver_port}:8443 "
            f"-p {self.vnc_port}:5901 "
            f"-p {self.browser_port}:8000 "
            f"-v {self.devbox_name}_db-data:/var/lib/mysql "
            f'-e PASSWORD="{self.codeserver_password}" '
            f'-e VNC_PASSWORD="{self.vnc_password}" '
            f"-v {self.devbox_name}_home:/home/frappe "
            "arunmathaisk/devbox-image:latest"
        )
        return self.execute(command)

    @step("Stop Devbox")
    def stop_devbox(self):
        self.execute(command=f"docker stop {self.devbox_name}")
        return self.execute(f"docker rm -f {self.devbox_name}")

    def get_devbox_status(self):
        command = f"docker inspect --format='{{{{.State.Status}}}}' {self.devbox_name}"
        # We pass the --rm flag thus cant do inspect on non existing container name or container id
        return self.execute(command, non_zero_throw=False)

    def get_devbox_docker_volumes_size(self):
        database_volume_name = f"{self.devbox_name}_db-data"
        command = (
            f"sudo du -sh $(docker volume inspect --format '{{{{ .Mountpoint }}}}' "
            f"{database_volume_name}) | cut -f1"
        )
        database_volume_size = self.execute(command).get("output")
        home_volume_name = f"{self.devbox_name}_home"
        command = (
            f"sudo du -sh $(docker volume inspect --format '{{{{ .Mountpoint }}}}' "
            f"{home_volume_name}) | cut -f1"
        )
        home_volume_size = self.execute(command).get("output")
        return {"database_volume_size": database_volume_size, "home_volume_size": home_volume_size}
