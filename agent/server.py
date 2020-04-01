import os
import shutil

from jinja2 import Environment, PackageLoader
from passlib.hash import pbkdf2_sha256 as pbkdf2

from agent.base import Base
from agent.job import Job, Step, step, job
from agent.bench import Bench
from agent.site import Site


class Server(Base):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]
        self.benches_directory = self.config["benches_directory"]
        self.nginx_directory = self.config["nginx_directory"]

        self.job = None
        self.step = None

    @step("Bench Initialize")
    def bench_init(self, name, python, repo, branch, clone):
        if clone:
            # NOTE: Cloning seems incoherent for now.
            # Unable to articulate the reasons as of now
            command = (
                f"bench init --clone-from {clone} --clone-without-update "
                f"--python {python} {name} --no-backups --no-auto-update"
            )
        else:
            command = (
                f"bench init --frappe-branch {branch} --frappe-path {repo} "
                f"--python {python} {name} --no-backups --no-auto-update"
            )

        return self.execute(command, directory=self.benches_directory)

    def dump(self):
        return {
            "name": self.name,
            "benches": {
                name: bench.dump() for name, bench in self.benches.items()
            },
            "config": self.config,
        }

    @job("New Bench")
    def new_bench(self, name, python, config, apps, clone):
        frappe = list(filter(lambda x: x["name"] == "frappe", apps))[0]
        self.bench_init(name, python, frappe["repo"], frappe["branch"], clone)
        bench = Bench(name, self)
        bench.setconfig(config)
        bench.setup_redis()
        bench.get_apps(apps)
        bench.reset_apps(apps)
        bench.setup_requirements()
        bench.build()
        bench.setup_production()

    @job("Update Site Pull")
    def update_site_pull_job(self, name, source, target):
        source = Bench(source, self)
        target = Bench(target, self)
        site = Site(name, source)

        site.enable_maintenance_mode()
        site.wait_till_ready()

        self.move_site(site, target)
        site = Site(name, target)

        site.disable_maintenance_mode()

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

    @job("Update Site Migrate")
    def update_site_migrate_job(self, name, source, target):
        source = Bench(source, self)
        target = Bench(target, self)
        site = Site(name, source)

        site.enable_maintenance_mode()
        site.wait_till_ready()
        site.clear_backup_directory()
        site.tablewise_backup()

        self.move_site(site, target)
        site = Site(name, target)

        site.migrate()
        site.disable_maintenance_mode()

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

    @job("Recover Failed Site Migration")
    def update_site_recover_job(self, name, source, target):
        source = Bench(source, self)
        target = Bench(target, self)

        site = Site(name, source)
        self.move_site(site, target)
        site = Site(name, target)

        site.restore_touched_tables()
        site.disable_maintenance_mode()

    @step("Move Site")
    def move_site(self, site, target):
        return shutil.move(site.directory, target.sites_directory)

    def execute(self, command, directory=None):
        return super().execute(command, directory=directory)

    @step("Reload NGINX")
    def reload_nginx(self):
        return self._reload_nginx()

    def setup_authentication(self, password):
        config = self.config
        config["access_token"] = pbkdf2.hash(password)
        self.setconfig(config)

    def setup_nginx(self):
        self._generate_nginx_config()
        self._reload_nginx()

    def setup_tls(self):
        self._generate_agent_nginx_config()
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
        self.execute(
            "git merge --ff-only upstream/master", directory=directory
        )

        # TODO: Handle jobs lost because of this. Nobody likes unemployment
        self.execute("sudo supervisorctl restart agent:redis")

        for worker in range(self.config["workers"]):
            worker_name = f"agent:worker-{worker}"
            self.execute(f"sudo supervisorctl restart {worker_name}")

        self.execute("sudo supervisorctl restart agent:web")

    def update_agent_cli(self):
        directory = os.path.join(self.directory, "repo")
        self.execute("git reset --hard", directory=directory)
        self.execute("git clean -fd", directory=directory)
        self.execute("git fetch upstream", directory=directory)
        self.execute(
            "git merge --ff-only upstream/master", directory=directory
        )

        self.execute("./env/bin/pip install -e repo", directory=self.directory)

        self.execute("sudo supervisorctl restart agent:")
        self._generate_supervisor_config()
        self._update_supervisor()

        self._generate_nginx_config()
        self._generate_agent_nginx_config()
        self._reload_nginx()

    def _generate_nginx_config(self):
        nginx_config = os.path.join(self.nginx_directory, "nginx.conf")
        self._render_template(
            "nginx/nginx.conf.jinja2", {}, nginx_config,
        )

    def _generate_agent_nginx_config(self):
        agent_nginx_config = os.path.join(self.directory, "nginx.conf")
        self._render_template(
            "agent/nginx.conf.jinja2",
            {
                "web_port": self.config["web_port"],
                "name": self.name,
                "tls_directory": self.config["tls_directory"],
            },
            agent_nginx_config,
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
                "user": self.config["user"],
            },
            supervisor_config,
        )

    def _reload_nginx(self):
        return self.execute("sudo systemctl reload nginx")

    def _render_template(self, template, context, outfile):
        environment = Environment(loader=PackageLoader("agent", "templates"))
        template = environment.get_template(template)

        with open(outfile, "w") as f:
            f.write(template.render(**context))

    def _update_supervisor(self):
        self.execute("sudo supervisorctl reread")
        self.execute("sudo supervisorctl update")
