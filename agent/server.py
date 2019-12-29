import os

from jinja2 import Environment, PackageLoader

from agent.base import Base


class Server(Base):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")

    def execute(self, command, directory=None):
        return super().execute(command, directory=directory)

    def setup_nginx(self):
        self._generate_nginx_config()
        self._reload_nginx()

    def setup_supervisor(self):
        self._generate_supervisor_config()
        self._update_supervisor()

    def _generate_nginx_config(self):
        nginx_config = os.path.join(self.directory, "nginx.conf")
        self._render_template(
            "nginx.jinja", {"web_port": self.config["web_port"]}, nginx_config,
        )

    def _generate_supervisor_config(self):
        supervisor_config = os.path.join(self.directory, "supervisor.conf")
        self._render_template(
            "supervisor.jinja",
            {
                "web_port": self.config["web_port"],
                "directory": self.directory,
            },
            supervisor_config,
        )

    def _reload_nginx(self):
        self.execute("sudo systemctl reload nginx")

    def _render_template(self, template, context, outfile):
        environment = Environment(loader=PackageLoader("agent", "templates"))
        template = environment.get_template(template)

        with open(outfile, "w") as f:
            f.write(template.render(**context))

    def _update_supervisor(self):
        self.execute("sudo supervisorctl reread")
        self.execute("sudo supervisorctl update")
