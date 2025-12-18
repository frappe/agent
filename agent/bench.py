from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import string
import tempfile
import traceback
from contextlib import suppress
from datetime import datetime, timedelta
from functools import partial
from glob import glob
from pathlib import Path, PurePath
from random import choices
from textwrap import indent
from typing import TYPE_CHECKING, TypedDict
from urllib.parse import urlparse

import requests

from agent.app import App
from agent.base import AgentException, Base
from agent.exceptions import InvalidSiteConfigException, SiteNotExistsException
from agent.job import job, step
from agent.site import Site
from agent.utils import download_file, end_execution, get_execution_result, get_size

if TYPE_CHECKING:
    from agent.server import Server

    class BenchUpdateApp(TypedDict):
        app: string
        url: string
        hash: string

    class ShouldRunUpdatePhase(TypedDict):
        setup_requirements_node: bool
        setup_requirements_python: bool
        rebuild_frontend: bool
        migrate_sites: bool


class Bench(Base):
    def __init__(self, name: str, server: Server, mounts=None):
        super().__init__()

        self.name = name
        self.server = server
        self.directory = os.path.join(self.server.benches_directory, name)
        self.sites_directory = os.path.join(self.directory, "sites")
        self.config_directory = os.path.join(self.directory, "config")
        self.logs_directory = os.path.join(self.directory, "logs")
        self.apps_file = os.path.join(self.directory, "sites", "apps.txt")
        self.bench_config_file = os.path.join(self.directory, "config.json")
        self.config_file = os.path.join(self.directory, "sites", "common_site_config.json")
        self.host = self.config.get("db_host", "localhost")
        self.docker_image = self.bench_config.get("docker_image")
        self.mounts = mounts
        if not (
            os.path.isdir(self.directory)
            and os.path.exists(self.sites_directory)
            and os.path.exists(self.config_file)
            and os.path.exists(self.bench_config_file)
        ):
            raise Exception

    @step("Deploy Bench")
    def deploy(self):
        return self.start()

    def dump(self):
        return {
            "name": self.name,
            "apps": {name: app.dump() for name, app in self.apps.items()},
            "config": self.config,
            "sites": {name: site.dump() for name, site in self.sites.items()},
        }

    def _delete_older_usage_files(self, max_retention_time):
        log_files = glob(
            os.path.join(
                self.directory,
                "logs",
                f"{self.server.name}-usage-*.json.log",
            )
        )

        for file in log_files:
            if os.stat(file).st_mtime < max_retention_time:
                print(f"Deleting {file} as it's older than {max_retention_time}")
                os.remove(file)

    def fetch_sites_info(self, since=None):
        max_retention_time = (datetime.utcnow() - timedelta(days=7)).timestamp()
        self._delete_older_usage_files(max_retention_time)

        if not since:
            since = max_retention_time

        info = {}
        usage_data = []
        log_files = glob(
            os.path.join(
                self.server.directory,
                "logs",
                f"{self.server.name}-usage-*.json.log",
            )
        )

        for file in log_files:
            # Only load files that are newer than the since timestamp
            if os.stat(file).st_mtime <= since:
                continue

            with open(file) as f:
                try:
                    usage_data.extend(json.load(f))
                except json.decoder.JSONDecodeError:
                    print(f"Error loading JSON from {file}")

        usage_data.sort(
            key=lambda x: datetime.fromisoformat(x["timestamp"]),
            reverse=True,
        )

        for site in self.sites.values():
            try:
                timezone_data = {d["timestamp"]: d["timezone"] for d in usage_data if d["site"] == site.name}
                timezone = timezone_data[max(timezone_data)]
            except Exception:
                timezone = None

            if not (usage_data and timezone):
                timezone = site.timezone

            info[site.name] = {
                "config": site.config,
                "usage": [
                    {
                        "database": d["database"],
                        "public": d["public"],
                        "private": d["private"],
                        "backups": d["backups"],
                        "timestamp": d["timestamp"],
                    }
                    for d in usage_data
                    if d["site"] == site.name
                ],
                "timezone": timezone,
            }

        return info

    def fetch_sites_analytics(self):
        analytics = {}
        for site in self.sites.values():
            try:
                analytics[site.name] = site.fetch_site_analytics()
            except Exception:
                import traceback

                traceback.print_exc()
        return analytics

    def execute(self, command, input=None, non_zero_throw=True):
        return super().execute(
            command,
            directory=self.directory,
            input=input,
            non_zero_throw=non_zero_throw,
        )

    def docker_execute(self, command, input=None, subdir=None, non_zero_throw=True, as_root: bool = False):
        interactive = "-i" if input else ""
        as_root = "-u root" if as_root else ""
        workdir = "/home/frappe/frappe-bench"
        if subdir:
            workdir = os.path.join(workdir, subdir)

        if self.bench_config.get("single_container"):
            command = f"docker exec {as_root} -w {workdir} {interactive} {self.name} {command}"
        else:
            service = f"{self.name}_worker_default"
            task = self.execute(f"docker service ps -f desired-state=Running -q --no-trunc {service}")[
                "output"
            ].split()[0]
            command = f"docker exec {as_root} -w {workdir} {interactive} {service}.1.{task} {command}"

        return self.execute(command, input=input, non_zero_throw=non_zero_throw)

    @step("New Site")
    def bench_new_site(self, name, mariadb_root_password, admin_password):
        site_database, temp_user, temp_password = self.create_mariadb_user(name, mariadb_root_password)
        try:
            return self.docker_execute(
                f"bench new-site --no-mariadb-socket "
                f"--mariadb-root-username {temp_user} "
                f"--mariadb-root-password {temp_password} "
                f"--admin-password {admin_password} "
                f"--db-name {site_database} {name}"
            )
        finally:
            self.drop_mariadb_user(name, mariadb_root_password, site_database)

    @job("Create User", priority="high")
    def create_user(
        self,
        site: str,
        email: str,
        first_name: str,
        last_name: str,
        password: str | None = None,
    ):
        _site = Site(site, self)
        _site.create_user(email, first_name, last_name, password)

    @job("Complete Setup Wizard")
    def complete_setup_wizard(self, site: str, data: dict):
        _site = Site(site, self)
        return _site.complete_setup_wizard(data)

    @job("Rename Site", priority="high")
    def rename_site_job(
        self,
        site: str,
        new_name: str,
        create_user: dict | None = None,
        config: dict | None = None,
    ):
        site = get_site_from_name(site, new_name, self)
        site.enable_maintenance_mode()
        site.wait_till_ready()
        if config:
            if site.config.get("host_name") == f"https://{site.name}":
                config.update({"host_name": f"https://{new_name}"})
            site.update_config(config)
        else:
            if site.config.get("host_name") == f"https://{site.name}":
                site.update_config({"host_name": f"https://{new_name}"})
        site.rename(new_name)
        self.setup_nginx()
        self.server.reload_nginx()
        site.disable_maintenance_mode()
        site.enable_scheduler()
        if create_user and create_user.get("email"):
            site.create_user(
                create_user.get("email"),
                create_user.get("first_name"),
                create_user.get("last_name"),
                create_user.get("password"),
            )

    def get_database_name(self, site):
        site_directory = os.path.join(self.sites_directory, "sites", site)
        return "_" + hashlib.sha1(site_directory.encode()).hexdigest()[:16]

    def get_random_string(self, length):
        return "".join(choices(string.ascii_letters + string.digits, k=length))

    def create_mariadb_user(self, site, mariadb_root_password, database=None):
        database = database or self.get_database_name(site)
        user = f"{database}_limited"
        password = self.get_random_string(16)
        queries = [
            f"CREATE OR REPLACE USER '{user}'@'%' IDENTIFIED BY '{password}'",
            f"CREATE OR REPLACE DATABASE {user}",
            f"GRANT ALL ON {user}.* TO '{user}'@'%'",
            f"GRANT RELOAD, CREATE USER ON *.* TO '{user}'@'%'",
            f"GRANT ALL ON {database}.* TO '{user}'@'%' WITH GRANT OPTION",
            "FLUSH PRIVILEGES",
        ]
        for query in queries:
            command = f'mysql -h {self.host} -uroot -p{mariadb_root_password} -e "{query}"'
            self.execute(command)
        return database, user, password

    def drop_mariadb_user(self, site, mariadb_root_password, database=None):
        database = database or self.get_database_name(site)
        user = f"{database}_limited"
        queries = [
            f"DROP DATABASE IF EXISTS {user}",
            f"DROP USER IF EXISTS '{user}'@'%'",
            "FLUSH PRIVILEGES",
        ]
        for query in queries:
            command = f'mysql -h {self.host} -uroot -p{mariadb_root_password} -e "{query}"'
            self.execute(command)

    def fetch_monitor_data(self):
        lines = []
        try:
            monitor_log_file = os.path.join(self.directory, "logs", "monitor.json.log")
            time = datetime.utcnow().isoformat()
            logs_directory = os.path.join(
                self.server.directory,
                "logs",
            )
            target_file = os.path.join(
                logs_directory,
                f"{self.name}-{time}-monitor.json.log",
            )
            if os.path.exists(monitor_log_file):
                shutil.move(monitor_log_file, target_file)

                with open(target_file) as f:
                    for line in f.readlines():
                        try:
                            lines.append(json.loads(line))
                        except Exception:
                            traceback.print_exc()

            now = datetime.now().timestamp()
            for file in os.listdir(logs_directory):
                path = os.path.join(logs_directory, file)
                if file.endswith("-monitor.json.log") and (now - os.stat(path).st_mtime) > (7 * 86400):
                    os.remove(path)
        except FileNotFoundError:
            pass
        except Exception:
            traceback.print_exc()
        return lines

    def _parse_pids(self, lines):
        pids = []
        lines = lines.strip().split("\n")

        for line in lines:
            parts = line.strip().split()
            name, pid = parts[0], parts[1]
            pids.append((name, pid))

        return pids

    def get_worker_pids(self):
        """Get all the processes running gunicorn for now"""
        return self._parse_pids(self.execute(f"docker top {self.name} | grep gunicorn")["output"])

    def take_snapshot(self, pid_info: list[tuple[str, str]]):
        snapshots = {}
        pyspy_bin = os.path.join(self.server.directory, "env/bin/py-spy")

        for name, pid in pid_info:
            try:
                snapshots[f"{name}:{pid}"] = json.loads(
                    self.execute(f"sudo {pyspy_bin} dump --pid {pid} --json")["output"]
                )
            except AgentException as e:
                snapshots[f"{name}:{pid}"] = str(e)

        return snapshots

    def status(self):
        status = {
            "sites": {site: {"scheduler": True, "web": True} for site in self.sites},
            "timestamp": str(datetime.now()),
        }

        for site in _inactive_scheduler_sites(self):
            status["sites"][site]["scheduler"] = False

        for site in _inactive_web_sites(self):
            status["sites"][site]["web"] = False

        return status

    @job("New Site", priority="high")
    def new_site(
        self,
        name,
        config,
        apps,
        mariadb_root_password,
        admin_password,
        create_user: dict | None = None,
    ):
        self.bench_new_site(name, mariadb_root_password, admin_password)
        site = Site(name, self)
        site.install_apps(apps)
        site.update_config(config)
        site.enable_scheduler()
        if create_user and create_user.get("email"):
            site.create_user(
                create_user.get("email"),
                create_user.get("first_name"),
                create_user.get("last_name"),
                create_user.get("password"),
            )
        self.setup_nginx()
        self.server.reload_nginx()

    @job("New Site from Backup", priority="high")
    def new_site_from_backup(
        self,
        name,
        default_config,
        apps,
        mariadb_root_password,
        admin_password,
        site_config,
        database,
        public,
        private,
        skip_failing_patches,
    ):
        files = self.download_files(name, database, public, private)
        self.bench_new_site(name, mariadb_root_password, admin_password)
        site = Site(name, self)
        site.update_config(default_config)
        try:
            site.restore_site(
                mariadb_root_password,
                admin_password,
                files["database"],
                files["public"],
                files["private"],
            )
            if site_config:
                site_config = json.loads(site_config)
                site.update_config(site_config)
        finally:
            self.delete_downloaded_files(files["directory"])
        site.uninstall_unavailable_apps(apps)
        site.migrate(skip_failing_patches=skip_failing_patches)
        site.set_admin_password(admin_password)
        site.enable_scheduler()
        self.setup_nginx()
        self.server.reload_nginx()

        return site.bench_execute("list-apps")

    @step("Archive Site")
    def bench_archive_site(self, name, mariadb_root_password, force):
        site_database, temp_user, temp_password = self.create_mariadb_user(
            name, mariadb_root_password, self.valid_sites[name].database
        )
        force_flag = "--force" if force else ""
        try:
            return self.docker_execute(
                f"bench drop-site --no-backup {force_flag} "
                f"--root-login {temp_user} --root-password {temp_password} "
                f"--archived-sites-path archived {name}"
            )
        finally:
            self.drop_mariadb_user(name, mariadb_root_password, site_database)

    @step("Download Backup Files")
    def download_files(self, name, database_url, public_url, private_url):
        download_directory = os.path.join(self.sites_directory, "downloads")
        if not os.path.exists(download_directory):
            os.mkdir(download_directory)
        directory = tempfile.mkdtemp(prefix="agent-upload-", suffix=f"-{name}", dir=download_directory)
        database_file = download_file(database_url, prefix=directory) if database_url else ""
        private_file = download_file(private_url, prefix=directory) if private_url else ""
        public_file = download_file(public_url, prefix=directory) if public_url else ""
        return {
            "directory": directory,
            "database": database_file,
            "private": private_file,
            "public": public_file,
        }

    @step("Delete Downloaded Backup Files")
    def delete_downloaded_files(self, backup_files_directory):
        shutil.rmtree(backup_files_directory)

    @job("Archive Site")
    def archive_site(self, name, mariadb_root_password, force):
        site_directory = os.path.join(self.sites_directory, name)
        if os.path.exists(site_directory):
            self.bench_archive_site(name, mariadb_root_password, force)
        self.setup_nginx()
        self.server._reload_nginx()

    @step("Bench Setup NGINX")
    def setup_nginx(self):
        from filelock import FileLock

        with FileLock(os.path.join(self.directory, "nginx.config.lock")):
            self.generate_nginx_config()
        return self.server._reload_nginx()

    @step("Bench Setup NGINX Target")
    def setup_nginx_target(self):
        from filelock import FileLock

        with FileLock(os.path.join(self.directory, "nginx.config.lock")):
            self.generate_nginx_config()
        return self.server._reload_nginx()

    def _set_sites_host(self, sites: list[Site]):
        for site in sites:
            for wildcard_domain in self.server.wildcards:
                if site.name.endswith("." + wildcard_domain):
                    site.host = "*." + wildcard_domain

    def generate_nginx_config(self):
        sites = [s for s in self.valid_sites.values()]
        domains = _get_domains(sites)

        if standalone := self.server.config.get("standalone"):
            self._set_sites_host(sites)

        codeserver = _get_codeserver_config(self.directory)

        config = {
            "bench_name": self.name,
            "bench_name_slug": self.name.replace("-", "_"),
            "domain": self.server.config.get("domain"),
            "sites": sites,
            "domains": domains,
            "http_timeout": self.bench_config["http_timeout"],
            "web_port": self.bench_config["web_port"],
            "socketio_port": self.bench_config["socketio_port"],
            "sites_directory": self.sites_directory,
            "standalone": standalone,
            "error_pages_directory": self.server.error_pages_directory,
            "nginx_directory": self.server.nginx_directory,
            "tls_protocols": self.server.config.get("tls_protocols"),
            "code_server": codeserver,
        }
        nginx_config = os.path.join(self.directory, "nginx.conf")

        self.server._render_template("bench/nginx.conf.jinja2", config, nginx_config)

    @step("Bench Disable Production")
    def disable_production(self):
        try:
            return self.stop()
        except AgentException as e:
            if "No such container" in e.data["output"]:
                pass
            else:
                raise

    @job("Bench Restart")
    def restart_job(self, web_only=False):
        return self.restart(web_only=web_only)

    @step("Bench Restart")
    def restart(self, web_only=False):
        return self.docker_execute(f"bench restart {'--web' if web_only else ''}")

    @job("Rebuild Bench Assets")
    def rebuild_job(self):
        return self.rebuild()

    @step("Rebuild Bench Assets")
    def rebuild(self, apps: list[str] | None = None, is_inplace: bool = False):
        if not apps:
            return self.docker_execute("bench build")

        if len(apps) == 1 and not is_inplace:
            return self.docker_execute(f"bench build --app {apps[0]}")

        return self.docker_execute(f"bench build --apps {','.join(apps)}")

    @property
    def apps(self):
        with open(self.apps_file, "r") as f:
            apps_list = f.read().split("\n")

        apps = {}
        for directory in apps_list:
            with suppress(Exception):
                apps[directory] = App(directory, self)
        return apps

    @step("Update Bench Configuration")
    def update_config(self, common_site_config, bench_config) -> bool:
        return self._update_config(common_site_config, bench_config)

    def _update_redis_password(self, redis_password: str) -> bool:
        """
        Update redis password if not present once added password will not be changed!
        Release group does not allow changing password anyways
        """
        updated = False

        requirepass_line = f"requirepass {redis_password}\n"
        protected_mode_line = "protected-mode no\n"

        redis_password_pattern = r"^\s*requirepass\s+.*$"
        protected_mode_pattern = r"^\s*protected-mode\s+.*$"

        for conf_file in [
            os.path.join(self.config_directory, "redis-queue.conf"),
            os.path.join(self.config_directory, "redis-cache.conf"),
        ]:
            with open(conf_file, "r") as f:
                content = f.read()

            if not content.endswith("\n"):
                content += "\n"

            if not re.search(protected_mode_pattern, content, flags=re.MULTILINE):
                updated = True
                content += protected_mode_line

            if not re.search(redis_password_pattern, content, flags=re.MULTILINE):
                updated = True
                content += requirepass_line

            with open(conf_file, "w") as f:
                f.write(content)

        return updated

    def _get_redis_passwords(self, common_site_config: dict[str, str | int]) -> str | None:
        """Get redis cache and queue passwords if they exist,
        we assume the redis cache and queue password to be same"""
        redis_cache_url = common_site_config.get("redis_cache")
        return urlparse(redis_cache_url).password

    def _update_config(
        self,
        common_site_config: dict | None = None,
        bench_config: dict | None = None,
    ) -> bool:
        requires_deploy = False
        if common_site_config:
            new_common_site_config = self.get_config(for_update=True)
            new_common_site_config.update(common_site_config)
            redis_password = self._get_redis_passwords(common_site_config)

            if redis_password:
                requires_deploy = self._update_redis_password(redis_password)

            self.set_config(new_common_site_config)

        if bench_config:
            new_bench_config = self.bench_config
            new_bench_config.update(bench_config)
            self.set_bench_config(new_bench_config)

        return requires_deploy

    @job("Update Bench Configuration", priority="high")
    def update_config_job(self, common_site_config, bench_config):
        old_config = self.bench_config
        requires_update = self.update_config(common_site_config, bench_config)
        self.setup_nginx()
        if self.bench_config.get("single_container"):
            self.update_supervisor()
            self.update_runtime_limits()
            if (
                requires_update
                or (old_config["web_port"] != bench_config["web_port"])
                or (old_config["socketio_port"] != bench_config["socketio_port"])
            ):
                self.deploy()
        else:
            self.generate_docker_compose_file()
            self.deploy()

    @step("Update Supervisor Configuration")
    def update_supervisor(self):
        self.generate_supervisor_config()
        self.docker_execute("supervisorctl reread")
        self.docker_execute("supervisorctl update")

    def generate_supervisor_config(self):
        supervisor_config = os.path.join(self.directory, "config", "supervisor.conf")
        self.server._render_template(
            "bench/supervisor.conf",
            {
                "background_workers": self.bench_config["background_workers"],
                "gunicorn_workers": self.bench_config["gunicorn_workers"],
                "http_timeout": self.bench_config["http_timeout"],
                "name": self.name,
                "statsd_host": self.bench_config["statsd_host"],
                "is_ssh_enabled": self.bench_config.get("is_ssh_enabled", False),
                "merge_all_rq_queues": self.bench_config.get("merge_all_rq_queues", False),
                "merge_default_and_short_rq_queues": self.bench_config.get(
                    "merge_default_and_short_rq_queues", False
                ),
                "use_rq_workerpool": self.bench_config.get("use_rq_workerpool", False),
                "environment_variables": self.bench_config.get("environment_variables"),
                "gunicorn_threads_per_worker": self.bench_config.get("gunicorn_threads_per_worker"),
                "is_code_server_enabled": self.bench_config.get("is_code_server_enabled", False),
                "custom_workers": self.common_site_config.get("workers", {}),
                "custom_workers_group": self._get_custom_workers_group(),
            },
            supervisor_config,
        )

    def _get_custom_workers_group(self):
        custom_workers = self.common_site_config.get("workers", {})
        worker_keys = custom_workers.keys()
        if worker_keys:
            return ",".join(f"frappe-bench-{name}-worker" for name in worker_keys)
        return ""

    @step("Generate Docker Compose File")
    def generate_docker_compose_file(self):
        config = self.bench_config
        config.update({"directory": self.directory})
        docker_compose = os.path.join(self.directory, "docker-compose.yml")
        self.server._render_template("bench/docker-compose.yml.jinja2", config, docker_compose)

    @job("Setup Code Server")
    def setup_code_server(self, name, password):
        self.create_code_server_config(name)
        self._start_code_server(password, setup=True)
        self.generate_nginx_config()
        self.server._reload_nginx()

    @step("Create Code Server Config")
    def create_code_server_config(self, name):
        code_server_path = os.path.join(self.directory, "codeserver")
        if not os.path.exists(code_server_path):
            os.mkdir(code_server_path)

        filename = os.path.join(code_server_path, name)
        with open(filename, "w") as file:
            file.write(str(self.bench_config.get("codeserver_port")))

    @step("Start Code Server")
    def _start_code_server(self, password, setup=False):
        if setup:
            self.docker_execute("supervisorctl start code-server:")

        self.docker_execute(
            f"sed -i 's/^password:.*/password: {password}/' /home/frappe/.config/code-server/config.yaml"
        )
        self.docker_execute("supervisorctl restart code-server:")

    @step("Stop Code Server")
    def _stop_code_server(self):
        self.docker_execute("supervisorctl stop code-server:")

    @job("Start Code Server")
    def start_code_server(self, password):
        self._start_code_server(password)

    @job("Stop Code Server")
    def stop_code_server(self):
        self._stop_code_server()

    @job("Archive Code Server")
    def archive_code_server(self):
        if os.path.exists(self.directory):
            self.remove_code_server()
            self.setup_nginx()
            self.server._reload_nginx()

    @step("Remove Code Server")
    def remove_code_server(self):
        code_server_path = os.path.join(self.directory, "codeserver")
        shutil.rmtree(code_server_path)
        self.docker_execute("supervisorctl stop code-server:")

    def prepare_mounts_on_host(self, bench_directory):
        mounts_cmd = ""

        if not self.mounts:
            return mounts_cmd

        def _create_mounts(host_path):
            if not os.path.exists(host_path):
                os.mkdir(host_path)

        for mp in self.mounts:
            host_path = mp["source"]
            destination_path = mp["destination"]

            if not mp["is_absolute_path"]:
                """
                self.server.benches_directory = /home/frappe/benches (Host)
                bench_directory = "/home/frappe/frappe-bench" (container)
                """
                host_path = os.path.join(self.server.benches_directory, mp["source"])
                destination_path = os.path.join(bench_directory, mp["destination"])

                _create_mounts(host_path)

            mounts_cmd += f" -v {host_path}:{destination_path} "

        return mounts_cmd

    def start(self, secondary_server_private_ip: str | None = None):
        if self.bench_config.get("single_container"):
            try:
                self.execute(f"docker stop {self.name}")
                self.execute(f"docker rm {self.name}")
            except Exception:
                pass

            ssh_port = self.bench_config.get("ssh_port", self.bench_config["web_port"] + 4000)
            ssh_ip = secondary_server_private_ip or self.bench_config.get("private_ip", "127.0.0.1")

            rq_port = self.bench_config.get("rq_port")
            rq_cache_port = self.bench_config.get("rq_cache_port")

            if not rq_cache_port:
                # [Auto Scaling] We need to expose this when we restart the container regardless
                offset = 18000 - self.bench_config["web_port"]
                rq_cache_port = 13000 + offset

            rq_port_mapping = f"-p 0.0.0.0:{rq_port}:11000 "  # need to expose to secondary server

            bench_directory = "/home/frappe/frappe-bench"
            mounts = self.prepare_mounts_on_host(bench_directory)

            command = (
                "docker run -d --init -u frappe "
                f"--restart always --hostname {self.name} "
                "--security-opt seccomp=unconfined "
                f"-p 127.0.0.1:{self.bench_config['web_port']}:8000 "
                f"-p 127.0.0.1:{self.bench_config['socketio_port']}:9000 "
                f"-p 127.0.0.1:{self.bench_config['codeserver_port']}:8088 "
                f"-p 0.0.0.0:{rq_cache_port}:13000 "
                f"{rq_port_mapping if rq_port else ''}"
                f"-p {ssh_ip}:{ssh_port}:2200 "
                f"-v {self.sites_directory}:{bench_directory}/sites "
                f"-v {self.logs_directory}:{bench_directory}/logs "
                f"-v {self.config_directory}:{bench_directory}/config "
                f"{mounts} "
                f"--name {self.name} {self.bench_config['docker_image']}"
            )
        else:
            command = (
                "docker stack deploy "
                "--resolve-image=never --with-registry-auth "
                f"--compose-file docker-compose.yml {self.name} "
            )
        return self.execute(command)

    def stop(self):
        if self.bench_config.get("single_container"):
            self.execute(f"docker stop {self.name}")
            return self.execute(f"docker rm {self.name}")
        return self.execute(f"docker stack rm {self.name}")

    @step("Stop Bench")
    def _stop(self):
        return self.execute(f"docker stop {self.name}")

    @step("Start Bench")
    def _start(self):
        return self.execute(f"docker start {self.name}")

    @job("Force Update Bench Limits")
    def force_update_limits(self, memory_high, memory_max, memory_swap, vcpu):
        self._stop()
        self._update_runtime_limits(memory_high, memory_max, memory_swap, vcpu)
        self._start()

    def update_runtime_limits(self, multiplier=1.0):
        memory_high = self.bench_config.get("memory_high") * multiplier
        memory_max = self.bench_config.get("memory_max") * multiplier
        memory_swap = self.bench_config.get("memory_swap") * multiplier
        vcpu = self.bench_config.get("vcpu")
        if not any([memory_high, memory_max, memory_swap, vcpu]):
            return
        self._update_runtime_limits(memory_high, memory_max, memory_swap, vcpu)

    @step("Update Bench Memory Limits")
    def _update_runtime_limits(self, memory_high, memory_max, memory_swap, vcpu):
        cmd = f"docker update {self.name}"
        if memory_high:
            cmd += f" --memory-reservation={memory_high}M"
        if memory_max:
            cmd += f" --memory={memory_max}M"
        if memory_swap:
            cmd += f" --memory-swap={memory_swap}M"
        if vcpu:
            cmd += f" --cpus={vcpu}"
        return self.execute(cmd)

    def _update_database_host(self, db_host: str):
        self._update_config({"db_host": db_host})

    @property
    def job_record(self):
        return self.server.job_record

    def readable_jde_err(self, title: str, jde: json.decoder.JSONDecodeError) -> str:
        output = f"{title}:\n{jde.doc}\n{jde}\n"
        import re

        output = re.sub(r'("db_name":.* ")(\w*)(")', r"\1********\3", output)
        return re.sub(r'("db_password":.* ")(\w*)(")', r"\1********\3", output)

    @property
    def sites(self):
        return self._sites()

    @property
    def valid_sites(self):
        return self._sites(validate_configs=True)

    def _sites(self, validate_configs=False) -> dict[str, Site]:
        sites = {}
        for directory in os.listdir(self.sites_directory):
            try:
                sites[directory] = Site(directory, self)
            except json.decoder.JSONDecodeError as jde:
                output = self.readable_jde_err(f"Error parsing JSON in {directory}", jde)
                try:
                    self.execute(
                        f"echo '{output}';exit {int(validate_configs)}",
                    )  # exit 1 to make sure the job fails and shows output
                except AgentException as e:
                    raise InvalidSiteConfigException(e.data, directory) from e
            except Exception:
                pass
        return sites

    def get_site(self, site):
        try:
            return self.valid_sites[site]
        except KeyError as e:
            raise SiteNotExistsException(site, self.name) from e
        except InvalidSiteConfigException as e:
            if e.site == site:
                raise

    @property
    def step_record(self):
        return self.server.step_record

    @step_record.setter
    def step_record(self, value):
        self.server.step_record = value

    def get_usage(self):
        return {
            "storage": get_size(self.directory),
            "database": sum([site.get_database_size() for site in self.sites.values()]),
        }

    @property
    def bench_config(self) -> dict:
        with open(self.bench_config_file, "r") as f:
            return json.load(f)

    @property
    def common_site_config(self):
        with open(self.config_file, "r") as f:
            return json.load(f)

    def set_bench_config(self, value, indent=1):
        """
        To avoid partial writes, we need to first write the config to a temporary file,
        then rename it to the original file.
        """
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            json.dump(value, temp_file, indent=indent, sort_keys=True)
            temp_file.flush()
            os.fsync(temp_file.fileno())
            temp_file.close()

        os.rename(self.bench_config_file, self.bench_config_file + ".bak")

        try:
            shutil.copy2(temp_file.name, self.bench_config_file)
            os.remove(temp_file.name)
        except Exception as e:
            os.rename(self.bench_config_file + ".bak", self.bench_config_file)
            raise e

    @job("Patch App")
    def patch_app(
        self,
        app: str,
        patch: str,
        filename: str,
        build_assets: bool,
        revert: bool,
    ):
        patch_container_path = self.prepare_app_patch(app, patch, filename)
        self.git_apply(app, revert, patch_container_path)

        if build_assets:
            self.rebuild()

        self.restart()

    def prepare_app_patch(self, app: str, patch: str, filename: str) -> str:
        """
        Function returns path inside the container, the sites is
        mounted in the container at a different path from that of
        the bench outside it.
        """
        relative = ["sites", "patches", app]
        patch_dir = Path(os.path.join(self.directory, *relative))
        patch_dir.mkdir(parents=True, exist_ok=True)

        bench_container_dir = "/home/frappe/frappe-bench"
        patch_container_dir = os.path.join(bench_container_dir, *relative, filename)

        patch_path = patch_dir / filename
        if patch_path.is_file():
            return patch_container_dir

        with patch_path.open("w") as f:
            f.write(patch)

        return patch_container_dir

    @step("Git Apply")
    def git_apply(self, app: str, revert: bool, patch_container_path: str):
        command = "git apply "
        if revert:
            command += "--reverse "
        command += patch_container_path

        app_path = os.path.join("apps", app)
        self.docker_execute(command, subdir=app_path)

    @job("Call Bench Supervisorctl")
    def call_supervisorctl(self, command: str, programs: list[str]):
        self.run_supervisorctl_command(command, programs)

    @step("Run Supervisorctl Command")
    def run_supervisorctl_command(self, command: str, programs: list[str]):
        target = "all"
        if len(programs) > 0:
            target = " ".join(programs)
        self.docker_execute(f"supervisorctl {command} {target}")

    @job("Update Bench In Place")
    def update_inplace(
        self,
        sites: list[str],
        image: str,
        apps: list[BenchUpdateApp],
    ):
        if not (diff_dict := self.pull_app_changes(apps).get("diff")):
            return

        should_run = get_should_run_update_phase(diff_dict)

        node = should_run["setup_requirements_node"]
        python = should_run["setup_requirements_python"]
        if node or python:
            self.setup_requirements(node, python)

        if should_run["migrate_sites"]:
            self.migrate_sites(sites)

        if should_run["rebuild_frontend"]:
            self.rebuild(apps=[app["app"] for app in apps], is_inplace=True)

        # commit container changes
        self.commit_container_changes(image)

        # restart site
        self.restart(web_only=False)

    @step("Pull App Changes")
    def pull_app_changes(self, apps: list[BenchUpdateApp]):
        res = get_execution_result()

        diff: dict[str, list[str]] = {}
        outputs: list[str] = []
        for app in apps:
            if not (files := self._pull_app_change(app)):
                continue

            app_name = app["app"]
            diff[app_name] = files

            output = "\n".join(
                [
                    app_name,
                    indent("\n".join(files), "    "),
                ]
            )
            outputs.append(output)

        res = end_execution(res, "\n\n".join(outputs))
        res["diff"] = diff
        return res

    def _pull_app_change(self, app: BenchUpdateApp) -> list[str]:
        remote = "inplace"
        app_path = os.path.join("apps", app["app"])
        exec = partial(self.docker_execute, subdir=app_path)

        self.set_git_remote(app["app"], app["url"], remote)

        app_path: str = os.path.join("apps", app["app"])
        new_hash: str = app["hash"]
        old_hash: str = exec("git rev-parse HEAD")["output"]

        if old_hash == new_hash:
            # Remove remote, url might be private
            exec(f"git remote remove {remote}")
            return []

        # Fetch new hash and get changed files
        exec(f"git fetch --depth 1 {remote} {new_hash}")
        diff: str = exec(f"git diff --name-only {old_hash} {new_hash}")["output"]

        # Ensure repo is not dirty and checkout next_hash
        exec(f"git reset --hard {old_hash}")
        exec("git clean -fd")
        exec(f"git checkout {new_hash}")

        # Remove remote, url might be private
        exec(f"git remote remove {remote}")
        return [s for s in diff.split("\n") if s]

    def set_git_remote(
        self,
        app: str,
        url: str,
        remote: str,
    ):
        app_path = os.path.join("apps", app)
        res = self.docker_execute(
            f"git remote get-url {remote}",
            subdir=app_path,
            non_zero_throw=False,
        )

        if res["output"] == url:
            return

        if res["returncode"] == 0:
            self.docker_execute(
                f"git remote remove {remote}",
                subdir=app_path,
            )

        self.docker_execute(
            f"git remote add {remote} {url}",
            subdir=app_path,
        )

    @step("Setup Requirements")
    def setup_requirements(self, node: bool = True, python: bool = True):
        flag = ""

        if node and not python:
            flag = " --node"

        if not node and python:
            flag = " --python"

        return self.docker_execute("bench setup requirements" + flag)

    @step("Migrate Sites")
    def migrate_sites(
        self,
        sites: list[str],
        skip_search_index: bool = False,
        skip_failing_patches: bool = False,
    ):
        res = get_execution_result()
        outputs: list[str] = []

        for site_name in sites:
            migrate_res = self.migrate_site(
                self.sites[site_name],
                skip_search_index,
                skip_failing_patches,
            )
            output = "\n".join(
                [
                    site_name,
                    indent(migrate_res["output"], "    "),
                ]
            )
            outputs.append(output)

        return end_execution(
            res,
            "\n\n".join(outputs),
        )

    def migrate_site(
        self,
        site: Site,
        skip_search_index: bool = False,
        skip_failing_patches: bool = False,
    ):
        site._enable_maintenance_mode()
        res = site._migrate(
            skip_search_index,
            skip_failing_patches,
        )
        site._disable_maintenance_mode()
        return res

    @step("Commit Container Changes")
    def commit_container_changes(self, image: str):
        container_id = self.execute(f'docker ps -aqf "name={self.name}"')["output"]
        res = self.execute(f"docker commit {container_id} {image}")
        self._update_config(bench_config={"docker_image": image})
        return res

    @job("Recover Update In Place")
    def recover_update_inplace(
        self,
        site_names: list[str],
        image: str,
    ):
        self._update_config(bench_config={"docker_image": image})
        sites = [Site(name, self) for name in site_names]

        # Enable maintenance mode on sites if possible
        self.enable_maintenance_mode(sites)

        """
        Will stop and remove failed inplace updated container and
        start last running container pointed to by image.
        """
        self.deploy()

        self.setup_nginx()
        self.recover_sites(sites)

    @step("Enable Maintenance Mode")
    def enable_maintenance_mode(self, sites: list[Site]):
        for site in sites:
            with suppress(Exception):
                site._enable_maintenance_mode()

    @step("Recover Sites")
    def recover_sites(self, sites: list[Site]):
        for site in sites:
            site._restore_touched_tables()
            with suppress(Exception):
                site.generate_theme_files()
            site._disable_maintenance_mode()


def get_should_run_update_phase(
    diff_dict: dict[str, list[str]],
) -> ShouldRunUpdatePhase:
    diff = []
    for dl in diff_dict.values():
        diff.extend(dl)

    setup_node = False
    setup_python = False
    rebuild = False
    migrate = False

    for file in diff:
        if all([setup_node, setup_python, rebuild, migrate]):
            break

        if not setup_node:
            setup_node = should_setup_requirements_node(file)

        if not setup_python:
            setup_python = should_setup_requirements_py(file)

        if not rebuild:
            rebuild = should_rebuild_frontend(file)

        if not migrate:
            migrate = should_migrate_sites(file)

    return dict(
        setup_requirements_node=setup_node,
        setup_requirements_python=setup_python,
        rebuild_frontend=rebuild,
        migrate_sites=migrate,
    )


def should_setup_requirements_node(file: str) -> bool:
    return _should_run_phase(
        file,
        [
            "package.json",
            "package-lock.json",
            "yarn.lock",
            ".lockb",
            "pnpm-lock.yaml",
        ],
    )


def should_setup_requirements_py(file: str) -> bool:
    return _should_run_phase(
        file,
        ["pyproject.toml", "setup.py", "requirements.txt"],
    )


def should_rebuild_frontend(file: str) -> bool:
    return _should_run_phase(
        file,
        [
            ".js",
            ".ts",
            ".html",
            ".vue",
            ".jsx",
            ".tsx",
            ".css",
            ".scss",
            ".sass",
        ],
        ["www", "public", "frontend", "dashboard"],
    )


def should_migrate_sites(file: str) -> bool:
    return _should_run_phase(
        file,
        ["hooks.py", ".json"],
        ["patches"],
    )


def _should_run_phase(
    file: str,
    ends: list[str] | None = None,
    subs: list[str] | None = None,
    globs: list[str] | None = None,
) -> bool:
    ends = ends or []
    subs = subs or []
    globs = globs or []

    if any([file.endswith(e) for e in ends]):
        return True

    if any([s in file for s in subs]):
        return True

    return any([PurePath(file).match(s) for s in globs])


def get_site_from_name(name: str, new_name: str, bench: Bench):
    try:
        return Site(name, bench)
    except OSError:
        pass

    try:
        return Site(new_name, bench)
    except OSError:
        raise Exception(f"Neither {name} nor {new_name} exists") from None


def _touch_currentsite_file(bench: Bench):
    file = os.path.join(bench.sites_directory, "currentsite.txt")
    open(file, "w").close()


def _inactive_scheduler_sites(bench: Bench):
    inactive = []
    _touch_currentsite_file(bench)
    try:
        doctor = bench.docker_execute("bench doctor")["output"].split("\n")
    except AgentException as e:
        doctor = e.data["output"]

    for line in doctor:
        if "inactive" in line:
            site = line.split(" ")[-1]
            inactive.append(site)
    return inactive


def _inactive_web_sites(bench: Bench):
    inactive = []
    session = requests.Session()
    for site in bench.sites:
        url = f"https://{site}/api/method/ping"
        try:
            result = session.get(url)
        except Exception as e:
            result = None
            print("Ping Failed", url, e)
        if not result or result.status_code != 200:
            inactive.append(site)
    return inactive


def _get_domains(sites: list[Site]):
    domains: dict[str, str] = {}
    for site in sites:
        for domain in site.config.get("domains", []):
            domains[domain] = site.name
    return domains


def _get_codeserver_config(bench_directory: str):
    codeserver_directory = os.path.join(bench_directory, "codeserver")

    if not os.path.exists(codeserver_directory):
        return {}

    codeservers = os.listdir(codeserver_directory)
    if not codeservers:
        return {}

    with open(os.path.join(codeserver_directory, codeservers[0])) as file:
        port = file.read()

    return {"name": codeservers[0], "port": port}
