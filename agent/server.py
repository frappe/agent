import os

from jinja2 import Environment, PackageLoader
from passlib.hash import pbkdf2_sha256 as pbkdf2

from agent.base import Base
from agent.job import Job, Step, step, job
from agent.bench import Bench


class Server(Base):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]
        self.benches_directory = self.config["benches_directory"]

        self.job = None
        self.step = None

    @step("Bench Initialize")
    def bench_init(self, name, python, repo, branch):
        self.execute(
            f"bench init --frappe-branch {branch} --frappe-path {repo} "
            f"--python {python} {name} --no-backups",
            directory=self.benches_directory,
        )

    @job("New Bench")
    def new_bench(self, name, python, config, apps):
        frappe = list(filter(lambda x: x["name"] == "frappe", apps))[0]
        self.bench_init(name, python, frappe["repo"], frappe["branch"])
        bench = Bench(name, self)
        bench.setconfig(config)
        bench.setup_redis()
        bench.reset_apps(apps)
        bench.setup_requirements()
        bench.build()
        bench.setup_production()
        return bench

    def execute(self, command, directory=None):
        return super().execute(command, directory=directory)

    def setup_authentication(self, password):
        config = self.config
        config["access_token"] = pbkdf2.hash(password)
        self.setconfig(config)

    def setup_nginx(self):
        self._generate_nginx_config()
        self._reload_nginx()

    def setup_supervisor(self):
        self._generate_supervisor_config()
        self._update_supervisor()

    @property
    def benches(self):
        benches = {}
        for directory in os.listdir(self.benches_directory):
            try:
                benches[directory] = Bench(directory, self)
            except Exception:
                pass
        return benches

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
                "redis_port": self.config["redis_port"],
                "workers": self.config["workers"],
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
