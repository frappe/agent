from __future__ import annotations

import os
from typing import TYPE_CHECKING

from agent.base import Base
from agent.job import step

if TYPE_CHECKING:
    from agent.server import Server


class Devbox(Base):
    def __init__(self, devbox_name: str, server: Server, websockify_port: int):
        self.devbox_name = devbox_name
        self.server = server
        self.directory = os.path.join(self.server.devboxes_directory, devbox_name)
        self.websockify_port = websockify_port

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
        }
        nginx_config = os.path.join(self.directory, "nginx.conf")

        self.server._render_template("devbox/nginx.conf.jinja2", config, nginx_config)

    @step("Run Devbox")
    def run_devbox(self):
        command = f"docker run -d --rm --name {self.devbox_name} -p {self.websockify_port}:6901 arunmathaisk/erpnext-15:latest"  # noqa: E501
        self.execute(command)
