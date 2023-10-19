import hashlib
import os
import json
import shutil
from typing import Dict
import requests
import string
import tempfile
import traceback

from random import choices
from glob import glob
from datetime import datetime, timedelta

from agent.app import App
from agent.base import AgentException, Base
from agent.job import job, step
from agent.site import Site
from agent.utils import download_file, get_size
from agent.exceptions import SiteNotExistsException



class Bench(Base):
    def __init__(self, name, server):
        self.name = name
        self.server = server
        self.directory = os.path.join(self.server.benches_directory, name)
        self.sites_directory = os.path.join(self.directory, "sites")
        self.config_directory = os.path.join(self.directory, "config")
        self.logs_directory = os.path.join(self.directory, "logs")
        self.apps_file = os.path.join(self.directory, "sites", "apps.txt")
        self.bench_config_file = os.path.join(self.directory, "config.json")
        self.config_file = os.path.join(
            self.directory, "sites", "common_site_config.json"
        )
        self.host = self.config.get("db_host", "localhost")
        self.docker_image = self.bench_config.get("docker_image")
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

    def fetch_sites_info(self, since=None):
        max_retention_time = (
            datetime.utcnow() - timedelta(days=30)
        ).timestamp()

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
        valid_files = [
            file for file in log_files if os.stat(file).st_mtime > since
        ]

        for file in log_files:
            if (file not in valid_files) and (
                os.stat(file).st_mtime > max_retention_time
            ):
                print(f"Deleting {file}")
                os.remove(file)
            else:
                try:
                    usage_data.extend(json.load(open(file)))
                except json.decoder.JSONDecodeError:
                    print(f"Error loading JSON from {file}")

        for site in self.sites.values():
            try:
                timezone_data = {
                    d["timestamp"]: d["timezone"]
                    for d in usage_data
                    if d["site"] == site.name
                }
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

    def execute(self, command, input=None):
        return super().execute(command, directory=self.directory, input=input)

    def docker_execute(self, command, input=None):
        interactive = "-i" if input else ""
        if self.bench_config.get("single_container"):
            command = (
                "docker exec -w /home/frappe/frappe-bench "
                f"{interactive} {self.name} {command}"
            )
        else:
            service = f"{self.name}_worker_default"
            task = self.execute(
                "docker service ps -f desired-state=Running -q --no-trunc "
                f"{service}"
            )["output"].split()[0]
            command = (
                "docker exec -w /home/frappe/frappe-bench "
                f"{interactive} {service}.1.{task} {command}"
            )
        return self.execute(command, input=input)

    @step("New Site")
    def bench_new_site(self, name, mariadb_root_password, admin_password):
        site_database, temp_user, temp_password = self.create_mariadb_user(
            name, mariadb_root_password
        )
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

    @job("Rename Site", priority="high")
    def rename_site_job(self, site: str, new_name: str):
        try:
            site = Site(site, self)
        except OSError:
            site = Site(new_name, self)
            return
        except OSError:
            raise Exception(f"Neither {site} nor {new_name} exists")
        site.enable_maintenance_mode()
        site.wait_till_ready()
        if site.config.get("host_name") == f"https://{site.name}":
            site.update_config({"host_name": f"https://{new_name}"})
        site.rename(new_name)
        self.setup_nginx()
        self.server.reload_nginx()
        site.disable_maintenance_mode()
        site.enable_scheduler()

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
            command = (
                f"mysql -h {self.host} -uroot -p{mariadb_root_password}"
                f' -e "{query}"'
            )
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
            command = (
                f"mysql -h {self.host} -uroot -p{mariadb_root_password}"
                f' -e "{query}"'
            )
            self.execute(command)

    def fetch_monitor_data(self):
        lines = []
        try:
            monitor_log_file = os.path.join(
                self.directory, "logs", "monitor.json.log"
            )
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
                if file.endswith("-monitor.json.log") and (
                    now - os.stat(path).st_mtime
                ) > (7 * 86400):
                    os.remove(path)
        except FileNotFoundError:
            pass
        except Exception:
            traceback.print_exc()
        return lines

    def status(self):
        def _touch_currentsite_file(bench):
            file = os.path.join(bench.sites_directory, "currentsite.txt")
            open(file, "w").close()

        def _inactive_scheduler_sites(bench):
            inactive = []
            _touch_currentsite_file(bench)
            try:
                doctor = bench.docker_execute("bench doctor")["output"].split(
                    "\n"
                )
            except AgentException as e:
                doctor = e.data["output"]

            for line in doctor:
                if "inactive" in line:
                    site = line.split(" ")[-1]
                    inactive.append(site)
            return inactive

        def _inactive_web_sites(bench):
            inactive = []
            session = requests.Session()
            for site in bench.sites.keys():
                url = f"https://{site}/api/method/ping"
                try:
                    result = session.get(url)
                except Exception as e:
                    result = None
                    print("Ping Failed", url, e)
                if not result or result.status_code != 200:
                    inactive.append(site)
            return inactive

        status = {
            "sites": {
                site: {"scheduler": True, "web": True}
                for site in self.sites.keys()
            },
            "timestamp": str(datetime.now()),
        }

        for site in _inactive_scheduler_sites(self):
            status["sites"][site]["scheduler"] = False

        for site in _inactive_web_sites(self):
            status["sites"][site]["web"] = False

        return status

    @job("New Site", priority="high")
    def new_site(
        self, name, config, apps, mariadb_root_password, admin_password
    ):
        self.bench_new_site(name, mariadb_root_password, admin_password)
        site = Site(name, self)
        site.install_apps(apps)
        site.update_config(config)
        site.enable_scheduler()
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
            site.restore(
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
        directory = tempfile.mkdtemp(
            prefix="agent-upload-", suffix=f"-{name}", dir=download_directory
        )
        database_file = download_file(database_url, prefix=directory)
        private_file = (
            download_file(private_url, prefix=directory) if private_url else ""
        )
        public_file = (
            download_file(public_url, prefix=directory) if public_url else ""
        )
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
        self.generate_nginx_config()
        self.server._reload_nginx()

    @step("Bench Setup NGINX Target")
    def setup_nginx_target(self):
        self.generate_nginx_config()
        self.server._reload_nginx()

    def generate_nginx_config(self):
        domains = {}
        sites = []
        for site in self.valid_sites.values():
            sites.append(site)
            for domain in site.config.get("domains", []):
                domains[domain] = site.name

        standalone = self.server.config.get("standalone")
        if standalone:
            for site in sites:
                for wildcard_domain in self.server.wildcards:
                    if site.name.endswith("." + wildcard_domain):
                        site.host = "*." + wildcard_domain

        codeserver_directory = os.path.join(self.directory, "codeserver")
        if os.path.exists(codeserver_directory):
            codeservers = os.listdir(codeserver_directory)
            if codeservers:
                with open(
                    os.path.join(codeserver_directory, codeservers[0])
                ) as file:
                    port = file.read()
                codeserver = {"name": codeservers[0], "port": port}
            else:
                codeserver = {}
        else:
            codeserver = {}

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
        self.server._render_template(
            "bench/nginx.conf.jinja2", config, nginx_config
        )

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
        return self.docker_execute(
            f"bench restart {'--web' if web_only else ''}"
        )

    @property
    def apps(self):
        with open(self.apps_file, "r") as f:
            apps_list = f.read().split("\n")

        apps = {}
        for directory in apps_list:
            try:
                apps[directory] = App(directory, self)
            except Exception:
                pass
        return apps

    @step("Update Bench Configuration")
    def update_config(self, common_site_config, bench_config):
        new_common_site_config = self.config
        new_common_site_config.update(common_site_config)
        self.setconfig(new_common_site_config)

        new_bench_config = self.bench_config
        new_bench_config.update(bench_config)
        self.set_bench_config(new_bench_config)

    @job("Update Bench Configuration", priority="high")
    def update_config_job(self, common_site_config, bench_config):
        old_config = self.bench_config
        self.update_config(common_site_config, bench_config)
        self.setup_nginx()
        if self.bench_config.get("single_container"):
            self.update_supervisor()
            if (old_config["web_port"] != bench_config["web_port"]) or (
                old_config["socketio_port"] != bench_config["socketio_port"]
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
        supervisor_config = os.path.join(
            self.directory, "config", "supervisor.conf"
        )
        self.server._render_template(
            "bench/supervisor.conf",
            {
                "background_workers": self.bench_config["background_workers"],
                "gunicorn_workers": self.bench_config["gunicorn_workers"],
                "http_timeout": self.bench_config["http_timeout"],
                "name": self.name,
                "statsd_host": self.bench_config["statsd_host"],
                "is_ssh_enabled": self.bench_config.get(
                    "is_ssh_enabled", False
                ),
                "merge_all_rq_queues": self.bench_config.get(
                    "merge_all_rq_queues", False
                ),
                "merge_default_and_short_rq_queues": self.bench_config.get(
                    "merge_default_and_short_rq_queues", False
                ),
                "environment_variables": self.bench_config.get(
                    "environment_variables"
                ),
            },
            supervisor_config,
        )

    @step("Generate Docker Compose File")
    def generate_docker_compose_file(self):
        config = self.bench_config
        config.update({"directory": self.directory})
        docker_compose = os.path.join(self.directory, "docker-compose.yml")
        self.server._render_template(
            "bench/docker-compose.yml.jinja2", config, docker_compose
        )

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

    def start(self):
        if self.bench_config.get("single_container"):
            try:
                self.execute(f"docker stop {self.name}")
                self.execute(f"docker rm {self.name}")
            except Exception:
                pass

            ssh_port = self.bench_config.get(
                "ssh_port", self.bench_config["web_port"] + 4000
            )
            ssh_ip = self.bench_config.get("private_ip", "127.0.0.1")

            bench_directory = "/home/frappe/frappe-bench"
            command = (
                "docker run -d --init -u frappe "
                f"--restart always --hostname {self.name} "
                f"-p 127.0.0.1:{self.bench_config['web_port']}:8000 "
                f"-p 127.0.0.1:{self.bench_config['socketio_port']}:9000 "
                f"-p 127.0.0.1:{self.bench_config['codeserver_port']}:8088 "
                f"-p {ssh_ip}:{ssh_port}:2200 "
                f"-v {self.sites_directory}:{bench_directory}/sites "
                f"-v {self.logs_directory}:{bench_directory}/logs "
                f"-v {self.config_directory}:{bench_directory}/config "
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
        else:
            return self.execute(f"docker stack rm {self.name}")

    @property
    def job_record(self):
        return self.server.job_record

    def readable_jde_err(
        self, title: str, jde: json.decoder.JSONDecodeError
    ) -> str:
        output = f"{title}:\n" f"{jde.doc}\n" f"{jde}\n"
        import re

        output = re.sub(r'("db_name":.* ")(\w*)(")', r"\1********\3", output)
        output = re.sub(
            r'("db_password":.* ")(\w*)(")', r"\1********\3", output
        )
        return output

    @property
    def sites(self):
        return self._sites()

    @property
    def valid_sites(self):
        return self._sites(validate_configs=True)

    def _sites(self, validate_configs=False) -> Dict[str, Site]:
        sites = {}
        for directory in os.listdir(self.sites_directory):
            try:
                sites[directory] = Site(directory, self)
            except json.decoder.JSONDecodeError as jde:
                output = self.readable_jde_err(
                    f"Error parsing JSON in {directory}", jde
                )
                self.execute(
                    f"echo '{output}';exit {int(validate_configs)}",
                )  # exit 1 to make sure the job fails and shows output
            except Exception:
                pass
        return sites

    def get_site(self, site):
        if site in self.sites:
            return self.benches[site]

        raise SiteNotExistsException(site, self.name)

    @property
    def step_record(self):
        return self.server.step_record

    def get_usage(self):
        return {
            "storage": get_size(self.directory),
            "database": sum(
                [site.get_database_size() for site in self.sites.values()]
            ),
        }

    @property
    def bench_config(self):
        with open(self.bench_config_file, "r") as f:
            return json.load(f)

    def set_bench_config(self, value, indent=1):
        with open(self.bench_config_file, "w") as f:
            json.dump(value, f, indent=indent, sort_keys=True)
