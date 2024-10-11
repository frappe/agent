from __future__ import annotations

import os

from agent.base import Base
from agent.job import step
from agent.server import Server


class Devbox(Base):
    def __init__(self,devbox_name:str,server: Server,websockify_port):
        self.devbox_name = devbox_name
        self.server = server
        self.directory = os.path.join(self.server.benches_directory,devbox_name)
        self.websockify_port = websockify_port

    @step("Devbox Setup NGINX")
    def setup_nginx(self,is_devbox=False):
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

        self.server._render_template("devbox/nginx.conf.jinja2", config,nginx_config)