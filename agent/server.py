import json
import os
import shutil
import tempfile
import time
from datetime import datetime
from typing import Dict, List

from jinja2 import Environment, PackageLoader
from passlib.hash import pbkdf2_sha256 as pbkdf2
from peewee import MySQLDatabase

from agent.base import AgentException, Base
from agent.bench import Bench
from agent.job import Job, Step, job, step
from agent.site import Site
from agent.patch_handler import run_patches
from agent.exceptions import BenchNotExistsException


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
        self.hosts_directory = os.path.join(self.nginx_directory, "hosts")

        self.error_pages_directory = os.path.join(
            self.directory, "repo", "agent", "pages"
        )
        self.job = None
        self.step = None

    def docker_login(self, registry):
        url = registry["url"]
        username = registry["username"]
        password = registry["password"]
        return self.execute(f"docker login -u {username} -p {password} {url}")

    @step("Initialize Bench")
    def bench_init(self, name, config):
        bench_directory = os.path.join(self.benches_directory, name)
        os.mkdir(bench_directory)
        directories = ["logs", "sites", "config"]
        for directory in directories:
            os.mkdir(os.path.join(bench_directory, directory))

        bench_config_file = os.path.join(bench_directory, "config.json")
        with open(bench_config_file, "w") as f:
            json.dump(config, f, indent=1, sort_keys=True)

        config.update({"directory": bench_directory, "name": name})
        docker_compose = os.path.join(bench_directory, "docker-compose.yml")
        self._render_template(
            "bench/docker-compose.yml.jinja2", config, docker_compose
        )

        config_directory = os.path.join(bench_directory, "config")
        command = (
            "docker run --rm --net none "
            f"-v {config_directory}:/home/frappe/frappe-bench/configmount "
            f"{config['docker_image']} cp -LR config/. configmount"
        )
        self.execute(command, directory=bench_directory)

        sites_directory = os.path.join(bench_directory, "sites")
        # Copy sites directory from image to host system
        command = (
            "docker run --rm --net none "
            f"-v {sites_directory}:/home/frappe/frappe-bench/sitesmount "
            f"{config['docker_image']} cp -LR sites/. sitesmount"
        )
        return self.execute(command, directory=bench_directory)

    def dump(self):
        return {
            "name": self.name,
            "benches": {
                name: bench.dump() for name, bench in self.benches.items()
            },
            "config": self.config,
        }

    @job("New Bench", priority="low")
    def new_bench(self, name, bench_config, common_site_config, registry, mounts=None):
        self.docker_login(registry)
        self.bench_init(name, bench_config)
        bench = Bench(name, self, mounts=mounts)
        bench.update_config(common_site_config, bench_config)
        if bench.bench_config.get("single_container"):
            bench.generate_supervisor_config()
        bench.deploy()
        bench.setup_nginx()

    def container_exists(self, name: str):
        """
        Throw if container exists
        """
        try:
            self.execute(
                f"""docker ps --filter "name=^{name}$" | grep {name}"""
            )
        except AgentException:
            pass  # container does not exist
        else:
            raise Exception("Container exists")

    @job("Archive Bench", priority="low")
    def archive_bench(self, name):
        bench_directory = os.path.join(self.benches_directory, name)
        if not os.path.exists(bench_directory):
            return
        try:
            bench = Bench(name, self)
        except FileNotFoundError as e:
            if not e.filename.endswith("common_site_config.json"):
                raise
        else:
            if bench.sites:
                raise Exception(f"Bench has sites: {bench.sites}")
            bench.disable_production()
        self.container_exists(name)
        self.move_bench_to_archived_directory(name)

    @job("Cleanup Unused Files", priority="low")
    def cleanup_unused_files(self):
        self.remove_archived_benches()
        self.remove_temporary_files()
        self.remove_unused_docker_artefacts()

    def remove_benches_without_container(self, benches: List[str]):
        for bench in benches:
            try:
                self.execute(f"docker ps -a | grep {bench}")
            except AgentException as e:
                if e.data.returncode:
                    self.move_to_archived_directory(Bench(bench, self))

    @step("Remove Archived Benches")
    def remove_archived_benches(self):
        now = datetime.now().timestamp()
        removed = []
        if os.path.exists(self.archived_directory):
            for bench in os.listdir(self.archived_directory):
                bench_path = os.path.join(self.archived_directory, bench)
                if now - os.stat(bench_path).st_mtime > 86400:
                    removed.append(
                        {
                            "bench": bench,
                            "size": self._get_tree_size(bench_path),
                        }
                    )
                    if os.path.isfile(bench_path):
                        os.remove(bench_path)
                    elif os.path.isdir(bench_path):
                        shutil.rmtree(bench_path)
        return {"benches": removed[:100]}

    @step("Remove Temporary Files")
    def remove_temporary_files(self):
        temp_directory = tempfile.gettempdir()
        now = datetime.now().timestamp()
        removed = []
        patterns = ["frappe-pdf", "snyk-patch", "yarn-", "agent-upload"]
        if os.path.exists(temp_directory):
            for file in os.listdir(temp_directory):
                if not list(filter(lambda x: x in file, patterns)):
                    continue
                file_path = os.path.join(temp_directory, file)
                if now - os.stat(file_path).st_mtime > 7200:
                    removed.append(
                        {"file": file, "size": self._get_tree_size(file_path)}
                    )
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
        return {"files": removed[:100]}

    @step("Remove Unused Docker Artefacts")
    def remove_unused_docker_artefacts(self):
        before = self.execute("docker system df -v")["output"].split("\n")
        prune = self.execute("docker system prune -af")["output"].split("\n")
        after = self.execute("docker system df -v")["output"].split("\n")
        return {
            "before": before,
            "prune": prune,
            "after": after,
        }

    @step("Move Bench to Archived Directory")
    def move_bench_to_archived_directory(self, bench_name):
        if not os.path.exists(self.archived_directory):
            os.mkdir(self.archived_directory)
        target = os.path.join(self.archived_directory, bench_name)
        if os.path.exists(target):
            shutil.rmtree(target)
        bench_directory = os.path.join(self.benches_directory, bench_name)
        self.execute(f"mv {bench_directory} {self.archived_directory}")

    @job("Update Site Pull", priority="low")
    def update_site_pull_job(self, name, source, target, activate):
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

        try:
            site.bench_execute(
                "execute"
                " frappe.website.doctype.website_theme.website_theme"
                ".generate_theme_files_if_not_exist"
            )
        except Exception:
            pass

        if activate:
            site.disable_maintenance_mode()

    @job("Update Site Migrate", priority="low")
    def update_site_migrate_job(
        self,
        name,
        source,
        target,
        activate,
        skip_failing_patches,
        skip_backups,
        before_migrate_scripts: Dict[str, str] = {},
        skip_search_index=True,
    ):
        source = Bench(source, self)
        target = Bench(target, self)
        site = Site(name, source)

        site.enable_maintenance_mode()
        site.wait_till_ready()

        if not skip_backups:
            site.clear_backup_directory()
            site.tablewise_backup()

        self.move_site(site, target)

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)

        if before_migrate_scripts:
            site.run_app_scripts(before_migrate_scripts)

        site.migrate(
            skip_search_index=skip_search_index,
            skip_failing_patches=skip_failing_patches,
        )

        try:
            site.bench_execute(
                "execute"
                " frappe.website.doctype.website_theme.website_theme"
                ".generate_theme_files_if_not_exist"
            )
        except Exception:
            pass

        if activate:
            site.disable_maintenance_mode()
        try:
            site.build_search_index()
        except Exception:
            # Don't fail job on failure
            # v12 does not have build_search_index command
            pass

    @job("Recover Failed Site Migrate", priority="high")
    def update_site_recover_migrate_job(
        self, name, source, target, activate, rollback_scripts
    ):
        source = Bench(source, self)
        target = Bench(target, self)

        site = Site(name, source)
        self.move_site(site, target)

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)
        site.restore_touched_tables()

        if rollback_scripts:
            site.run_app_scripts(rollback_scripts)

        if activate:
            site.disable_maintenance_mode()

    @job("Recover Failed Site Pull", priority="high")
    def update_site_recover_pull_job(self, name, source, target, activate):
        source = Bench(source, self)
        target = Bench(target, self)

        site = Site(name, source)
        self.move_site(site, target)

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)

        if activate:
            site.disable_maintenance_mode()

    @job("Move Site to Bench")
    def move_site_to_bench(
        self, name, source, target, deactivate, activate, skip_failing_patches
    ):
        # Dangerous method (no backup),
        # use update_site_migrate if you don't know what you're doing
        source = Bench(source, self)
        target = Bench(target, self)
        site = Site(name, source)

        if deactivate:  # cases when python is broken in bench
            site.enable_maintenance_mode()
            site.wait_till_ready()

        self.move_site(site, target)

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)

        site.migrate(skip_failing_patches=skip_failing_patches)

        try:
            site.bench_execute(
                "execute"
                " frappe.website.doctype.website_theme.website_theme"
                ".generate_theme_files_if_not_exist"
            )
        except Exception:
            pass

        if activate:
            site.disable_maintenance_mode()

    @job("Recover Failed Site Update", priority="high")
    def update_site_recover_job(self, name, bench):
        site = self.benches[bench].sites[name]
        site.disable_maintenance_mode()

    @step("Move Site")
    def move_site(self, site, target):
        destination = os.path.join(target.sites_directory, site.name)
        destination_site_config = os.path.join(destination, "site_config.json")
        if os.path.exists(destination) and not os.path.exists(
            destination_site_config
        ):
            # If there's already a site directory in the destination bench
            # and it does not have a site_config.json file,
            # then it is an incomplete site directory.
            # Move it to the sites/archived directory
            archived_sites_directory = os.path.join(
                target.sites_directory, "archived"
            )
            os.makedirs(archived_sites_directory, exist_ok=True)
            archived_site_path = os.path.join(
                archived_sites_directory,
                f"{site.name}-{datetime.now().isoformat()}",
            )
            shutil.move(destination, archived_site_path)
        shutil.move(site.directory, target.sites_directory)

    def execute(self, command, directory=None, skip_output_log=False):
        return super().execute(
            command, directory=directory, skip_output_log=skip_output_log
        )

    @job("Reload NGINX")
    def restart_nginx(self):
        return self.reload_nginx()

    @step("Reload NGINX")
    def reload_nginx(self):
        return self._reload_nginx()

    @step("Update Supervisor")
    def update_supervisor(self):
        return self._update_supervisor()

    def setup_authentication(self, password):
        self.update_config({"access_token": pbkdf2.hash(password)})

    def setup_proxysql(self, password):
        self.update_config({"proxysql_admin_password": password})

    def update_config(self, value):
        config = self.config
        config.update(value)
        self.setconfig(config, indent=4)

    def setup_registry(self):
        self.update_config({"registry": True})
        self.setup_nginx()

    def setup_log(self):
        self.update_config({"log": True})
        self.setup_nginx()

    def setup_analytics(self):
        self.update_config({"analytics": True})
        self.setup_nginx()

    def setup_trace(self):
        self.update_config({"trace": True})
        self.setup_nginx()

    def setup_nginx(self):
        self._generate_nginx_config()
        self._generate_agent_nginx_config()
        self._reload_nginx()

    def setup_supervisor(self):
        self._generate_redis_config()
        self._generate_supervisor_config()
        self._update_supervisor()

    def start_all_benches(self):
        for bench in self.benches.values():
            try:
                bench.start()
            except Exception:
                pass

    def stop_all_benches(self):
        for bench in self.benches.values():
            try:
                bench.stop()
            except Exception:
                pass

    @property
    def benches(self) -> Dict[str, Bench]:
        benches = {}
        for directory in os.listdir(self.benches_directory):
            try:
                benches[directory] = Bench(directory, self)
            except Exception:
                pass
        return benches

    def get_bench(self, bench):
        try:
            return self.benches[bench]
        except KeyError:
            raise BenchNotExistsException(bench)

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

    def update_agent_web(self, url=None):
        directory = os.path.join(self.directory, "repo")
        self.execute("git reset --hard", directory=directory)
        self.execute("git clean -fd", directory=directory)
        if url:
            self.execute(
                f"git remote set-url upstream {url}", directory=directory
            )
        self.execute("git fetch upstream", directory=directory)
        self.execute(
            "git merge --ff-only upstream/master", directory=directory
        )

        self.execute("./env/bin/pip install -e repo", directory=self.directory)

        self._generate_redis_config()
        self._generate_supervisor_config()
        self.execute("sudo supervisorctl reread")
        self.execute("sudo supervisorctl restart agent:redis")

        self.setup_nginx()
        for worker in range(self.config["workers"]):
            worker_name = f"agent:worker-{worker}"
            self.execute(f"sudo supervisorctl restart {worker_name}")

        self.execute("sudo supervisorctl restart agent:web")
        run_patches()

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
        self.setup_supervisor()

        self.setup_nginx()
        run_patches()

    def get_agent_version(self):
        directory = os.path.join(self.directory, "repo")
        return {
            "commit": self.execute("git rev-parse HEAD", directory=directory)[
                "output"
            ],
            "status": self.execute("git status --short", directory=directory)[
                "output"
            ],
            "upstream": self.execute(
                "git remote get-url upstream", directory=directory
            )["output"],
            "show": self.execute("git show", directory=directory)["output"],
        }

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
            "nginx/nginx.conf.jinja2",
            {
                "proxy_ip": self.config.get("proxy_ip"),
                "tls_protocols": self.config.get("tls_protocols"),
                "nginx_vts_module_enabled": self.config.get(
                    "nginx_vts_module_enabled", True
                ),
            },
            nginx_config,
        )

    def _generate_agent_nginx_config(self):
        agent_nginx_config = os.path.join(self.directory, "nginx.conf")
        self._render_template(
            "agent/nginx.conf.jinja2",
            {
                "web_port": self.config["web_port"],
                "name": self.name,
                "registry": self.config.get("registry", False),
                "monitor": self.config.get("monitor", False),
                "log": self.config.get("log", False),
                "analytics": self.config.get("analytics", False),
                "trace": self.config.get("trace", False),
                "tls_directory": self.config["tls_directory"],
                "nginx_directory": self.nginx_directory,
                "nginx_vts_module_enabled": self.config.get(
                    "nginx_vts_module_enabled", True
                ),
                "pages_directory": os.path.join(
                    self.directory, "repo", "agent", "pages"
                ),
                "tls_protocols": self.config.get("tls_protocols"),
            },
            agent_nginx_config,
        )

    def _generate_redis_config(self):
        redis_config = os.path.join(self.directory, "redis.conf")
        self._render_template(
            "agent/redis.conf.jinja2",
            {"redis_port": self.config["redis_port"]},
            redis_config,
        )

    def _generate_supervisor_config(self):
        supervisor_config = os.path.join(self.directory, "supervisor.conf")
        self._render_template(
            "agent/supervisor.conf.jinja2",
            {
                "web_port": self.config["web_port"],
                "redis_port": self.config["redis_port"],
                "gunicorn_workers": self.config.get("gunicorn_workers", 2),
                "workers": self.config["workers"],
                "directory": self.directory,
                "user": self.config["user"],
            },
            supervisor_config,
        )

    def _reload_nginx(self):
        return self.execute("sudo systemctl reload nginx")

    def _render_template(self, template, context, outfile, options=None):
        if options is None:
            options = {}
        options.update({"loader": PackageLoader("agent", "templates")})
        environment = Environment(**options)
        template = environment.get_template(template)

        with open(outfile, "w") as f:
            f.write(template.render(**context))

    def _update_supervisor(self):
        self.execute("sudo supervisorctl reread")
        self.execute("sudo supervisorctl update")

    def _get_tree_size(self, path):
        return self.execute(f"du -sh {path}")["output"].split()[0]

    def long_method(
        self,
    ):
        return self.execute("du -h -d 1 /home/aditya/Frappe")["output"]

    @job("Long")
    def long_step(
        self,
    ):
        return self.long_method()

    @job("Long")
    def long_job(
        self,
    ):
        return self.long_step()

    @property
    def wildcards(self) -> List[str]:
        wildcards = []
        for host in os.listdir(self.hosts_directory):
            if "*" in host:
                wildcards.append(host.strip("*."))
        return wildcards
