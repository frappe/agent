import os
import shutil
import time

from datetime import datetime
from jinja2 import Environment, PackageLoader
from passlib.hash import pbkdf2_sha256 as pbkdf2
from peewee import MySQLDatabase

from agent.base import Base, AgentException
from agent.job import Job, Step, step, job
from agent.bench import Bench
from agent.site import Site


class Server(Base):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]
        self.benches_directory = self.config["benches_directory"]
        self.archived_directory = os.path.join(
            os.path.dirname(self.benches_directory), "archived"
        )
        self.nginx_directory = self.config["nginx_directory"]

        self.job = None
        self.step = None

    @step("Bench Initialize")
    def bench_init(self, name, python, url, branch, clone):
        if clone:
            # NOTE: Cloning seems incoherent for now.
            # Unable to articulate the reasons as of now
            command = (
                f"bench init --clone-from {clone} --clone-without-update "
                "--no-backups --skip-assets --verbose "
                f"--python {python} {name}"
            )
        else:
            command = (
                f"bench init --frappe-branch {branch} --frappe-path {url} "
                "--no-backups --skip-assets --verbose "
                f"--python {python} {name}"
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
        self.bench_init(name, python, frappe["url"], frappe["branch"], clone)
        bench = Bench(name, self)
        bench.update_config(config)
        bench.setup_redis()
        bench.reset_frappe(apps)
        bench.get_apps(apps)
        bench.setup_requirements()
        bench.build()
        bench.setup_production()

    @job("Archive Bench")
    def archive_bench(self, name):
        bench = Bench(name, self)
        bench.disable_production()
        self.move_bench_to_archived_directory(bench)

    @step("Move Bench to Archived Directory")
    def move_bench_to_archived_directory(self, bench):
        if not os.path.exists(self.archived_directory):
            os.mkdir(self.archived_directory)
        target = os.path.join(self.archived_directory, bench.name)
        if os.path.exists(target):
            shutil.rmtree(target)
        self.execute(f"mv {bench.directory} {self.archived_directory}")

    @job("Update Site Pull")
    def update_site_pull_job(self, name, source, target):
        source = Bench(source, self)
        target = Bench(target, self)
        site = Site(name, source)

        site.enable_maintenance_mode()
        site.wait_till_ready()

        self.move_site(site, target)
        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)
        site.migrate()

        site.disable_maintenance_mode()

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

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)

        site.migrate()
        site.disable_maintenance_mode()

    @job("Recover Failed Site Migration")
    def update_site_recover_job(self, name, source, target):
        source = Bench(source, self)
        target = Bench(target, self)

        site = Site(name, source)
        self.move_site(site, target)

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)
        site.restore_touched_tables()

        site.disable_maintenance_mode()

    @step("Move Site")
    def move_site(self, site, target):
        shutil.move(site.directory, target.sites_directory)

    def execute(self, command, directory=None):
        return super().execute(command, directory=directory)

    @step("Reload NGINX")
    def reload_nginx(self):
        return self._reload_nginx()

    @step("Update Supervisor")
    def update_supervisor(self):
        return self._update_supervisor()

    def setup_authentication(self, password):
        config = self.config
        config["access_token"] = pbkdf2.hash(password)
        self.setconfig(config, indent=4)

    def setup_nginx(self):
        self._generate_nginx_config()
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

        self.execute("./env/bin/pip install -e repo", directory=self.directory)
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

    def status(self, mariadb_root_password):
        return {
            "mariadb": self.mariadb_processlist(
                mariadb_root_password=mariadb_root_password
            ),
            "supervisor": self.supervisor_status(),
            "nginx": self.nginx_status(),
            "stats": self.stats(),
            "processes": self.processes(),
            "timestamp": str(datetime.now()),
        }

    def _memory_stats(self):
        free = self.execute("free -t -m")["output"].split("\n")
        memory = {}
        headers = free[0].split()
        for line in free[1:]:
            type, line = line.split(None, 1)
            memory[type.lower()[:-1]] = dict(
                zip(headers, list(map(int, line.split())))
            )
        return memory

    def _cpu_stats(self):
        prev_proc = self.execute("cat /proc/stat")["output"].split("\n")
        time.sleep(0.5)
        now_proc = self.execute("cat /proc/stat")["output"].split("\n")

        # 0   user            Time spent in user mode.
        # 1   nice            Time spent in user mode with low priority
        # 2   system          Time spent in system mode.
        # 3   idle            Time spent in the idle task.
        # 4   iowait          Time waiting for I/O to complete.  This
        # 5   irq             Time servicing interrupts.
        # 6   softirq         Time servicing softirqs.
        # 7   steal           Stolen time
        # 8   guest           Time spent running a virtual CPU for guest OS
        # 9   guest_nice      Time spent running a niced guest

        # IDLE = idle + iowait
        # NONIDLE = user + nice + system + irq + softirq + steal + guest
        #           + guest_nice
        # TOTAL = IDLE + NONIDLE
        # USAGE = TOTAL - IDLE / TOTAL
        cpu = {}
        for prev, now in zip(prev_proc, now_proc):
            if prev.startswith("cpu"):
                type = prev.split()[0]
                prev = list(map(int, prev.split()[1:]))
                now = list(map(int, now.split()[1:]))

                idle = (now[3] + now[4]) - (prev[3] + prev[4])
                total = sum(now) - sum(prev)
                cpu[type] = int(1000 * (total - idle) / total) / 10
        return cpu

    def stats(self):
        load_average = os.getloadavg()
        return {
            "cpu": {
                "usage": self._cpu_stats(),
                "count": os.cpu_count(),
                "load_average": {
                    1: load_average[0],
                    5: load_average[1],
                    15: load_average[2],
                },
            },
            "memory": self._memory_stats(),
        }

    def processes(self):
        processes = []
        try:
            output = self.execute("ps --pid 2 --ppid 2 --deselect u")[
                "output"
            ].split("\n")
            headers = list(filter(None, output[0].split()))
            rows = map(
                lambda s: s.strip().split(None, len(headers) - 1), output[1:]
            )
            processes = [dict(zip(headers, row)) for row in rows]
        except Exception:
            import traceback

            traceback.print_exc()
        return processes

    def mariadb_processlist(self, mariadb_root_password):
        processes = []
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host="localhost",
                port=3306,
            )
            cursor = mariadb.execute_sql("SHOW PROCESSLIST")
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            processes = list(map(lambda x: dict(zip(columns, x)), rows))
        except Exception:
            import traceback

            traceback.print_exc()
        return processes

    def supervisor_status(self, name="all"):
        status = []
        try:
            try:
                supervisor = self.execute(f"sudo supervisorctl status {name}")
            except AgentException as e:
                supervisor = e.data

            for process in supervisor["output"].split("\n"):
                name, description = process.split(None, 1)

                name, *group = name.strip().split(":")
                group = group[0] if group else ""

                state, *description = description.strip().split(None, 1)
                state = state.strip()
                description = description[0].strip() if description else ""

                status.append(
                    {
                        "name": name,
                        "group": group,
                        "state": state,
                        "description": description,
                        "online": state == "RUNNING",
                    }
                )
        except Exception:
            import traceback

            traceback.print_exc()
        return status

    def nginx_status(self):
        try:
            systemd = self.execute("sudo systemctl status nginx")
        except AgentException as e:
            systemd = e.data
        return systemd["output"]

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
