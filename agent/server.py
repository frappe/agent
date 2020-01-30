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

    def dump(self):
        return {
            "name": self.name,
            "benches": {
                name: bench.dump() for name, bench in self.benches.items()
            },
            "config": self.config,
        }

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

    def execute(self, command, directory=None):
        return super().execute(command, directory=directory)

    @step("Reload NGINX")
    def reload_nginx(self):
        return self.execute(f"sudo systemctl reload nginx")

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

    def update_agent_web(self):
        directory = os.path.join(self.directory, "repo")
        self.execute("git reset --hard", directory=directory)
        self.execute("git clean -fd", directory=directory)
        self.execute("git fetch upstream", directory=directory)
        self.execute("git merge --ff-only upstream/master", directory=directory)
        self.execute("sudo supervisorctl restart agent:web")

        # TODO: Handle jobs lost because of this. Nobody likes unemployment
        self.execute("sudo supervisorctl restart agent:redis")

        for worker in range(self.config["workers"]):
            worker_name = f"agent:worker-{worker}"
            self.execute(f"sudo supervisorctl restart {worker_name}")

        # Kill yourself. Supervisor will restart agent:agent-web
        exit(0)

    def update_agent_cli(self):
        directory = os.path.join(self.directory, "repo")
        self.execute("git reset --hard", directory=directory)
        self.execute("git clean -fd", directory=directory)
        self.execute("git fetch upstream", directory=directory)
        self.execute("git merge --ff-only upstream/master", directory=directory)

        self.execute("./env/bin/pip install -e repo", directory=self.directory)

        self.execute("sudo supervisorctl restart agent:")
        self._generate_supervisor_config()
        self._update_supervisor()  

        self._generate_nginx_config()
        self._reload_nginx()

    def _generate_nginx_config(self):
        nginx_config = os.path.join(self.directory, "nginx.conf")
        self._render_template(
            "agent/nginx.conf.jinja2",
            {"web_port": self.config["web_port"], "name": self.name},
            nginx_config,
        )

    def _generate_supervisor_config(self):
        supervisor_config = os.path.join(self.directory, "supervisor.conf")
        self._render_template(
            "agent/supervisor.conf.jinja2",
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
