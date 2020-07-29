import json
import os
import shutil
import traceback
from agent.app import App
from agent.base import Base, AgentException
from agent.job import job, step
from agent.site import Site
from datetime import datetime
import requests
import tempfile
from agent.utils import download_file


class Bench(Base):
    def __init__(self, name, server):
        self.name = name
        self.server = server
        self.directory = os.path.join(self.server.benches_directory, name)
        self.apps_directory = os.path.join(self.directory, "apps")
        self.sites_directory = os.path.join(self.directory, "sites")
        self.apps_file = os.path.join(self.directory, "sites", "apps.txt")
        self.config_file = os.path.join(
            self.directory, "sites", "common_site_config.json"
        )
        if not (
            os.path.isdir(self.directory)
            and os.path.exists(self.apps_directory)
            and os.path.exists(self.sites_directory)
            and os.path.exists(self.config_file)
        ):
            raise Exception

    @step("Bench Build")
    def build(self):
        return self.execute("bench build")

    def dump(self):
        return {
            "name": self.name,
            "apps": {name: app.dump() for name, app in self.apps.items()},
            "config": self.config,
            "sites": {name: site.dump() for name, site in self.sites.items()},
        }

    def execute(self, command, input=None):
        return super().execute(command, directory=self.directory, input=input)

    @step("New Site")
    def bench_new_site(self, name, mariadb_root_password, admin_password):
        return self.execute(
            f"bench new-site "
            f"--admin-password {admin_password} "
            f"--mariadb-root-password {mariadb_root_password} "
            f"{name}"
        )

    def fetch_monitor_data(self):
        lines = []
        try:
            monitor_log_file = os.path.join(
                self.directory, "logs", "monitor.json.log"
            )
            time = datetime.utcnow().isoformat()
            logs_directory = os.path.join(self.server.directory, "logs",)
            target_file = os.path.join(
                logs_directory, f"{self.name}-{time}-monitor.json.log",
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
                doctor = bench.execute("bench doctor")["output"].split("\n")
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

    @job("New Site")
    def new_site(
        self, name, config, apps, mariadb_root_password, admin_password
    ):
        self.bench_new_site(name, mariadb_root_password, admin_password)
        site = Site(name, self)
        site.install_apps(apps)
        site.update_config(config)
        self.setup_nginx()
        self.server.reload_nginx()

    @job("New Site from Backup")
    def new_site_from_backup(
        self,
        name,
        config,
        apps,
        mariadb_root_password,
        admin_password,
        database,
        public,
        private,
    ):

        files = self.download_files(name, database, public, private)
        self.bench_new_site(name, mariadb_root_password, admin_password)
        site = Site(name, self)
        site.update_config(config)
        site.restore(
            mariadb_root_password,
            admin_password,
            files["database"],
            files["public"],
            files["private"],
        )
        site.uninstall_unavailable_apps(apps)
        site.migrate()
        site.set_admin_password(admin_password)
        site.enable_scheduler()
        self.setup_nginx()
        self.server.reload_nginx()

        shutil.rmtree(os.path.dirname(files["database"]))
        return site.bench_execute("list-apps")

    @step("Archive Site")
    def bench_archive_site(self, name, mariadb_root_password):
        return self.execute(
            f"bench drop-site {name} "
            f"--root-password {mariadb_root_password} --no-backup"
        )

    @step("Download Backup Files")
    def download_files(self, name, database_url, public_url, private_url):
        folder = tempfile.mkdtemp(prefix="agent-upload-", suffix=f"-{name}")
        database_file = download_file(database_url, prefix=folder)
        private_file = download_file(private_url, prefix=folder)
        public_file = download_file(public_url, prefix=folder)
        return {
            "database": database_file,
            "private": private_file,
            "public": public_file,
        }

    @job("Archive Site")
    def archive_site(self, name, mariadb_root_password):
        self.bench_archive_site(name, mariadb_root_password)
        self.setup_nginx()
        self.server.reload_nginx()

    @step("Bench Reset Frappe App")
    def reset_frappe(self, apps):
        data = {}
        output = []

        hash = list(filter(lambda x: x["name"] == "frappe", apps))[0]["hash"]
        data["fetch"] = self.apps["frappe"].fetch_ref(hash)
        data["checkout"] = self.apps["frappe"].checkout(hash)

        output.append(data["fetch"]["output"])
        output.append(data["checkout"]["output"])

        data["output"] = "\n".join(output)
        return data

    @step("Bench Get Apps")
    def get_apps(self, apps):
        data = {"apps": {}}
        output = []

        for app in apps:
            name, repo, url, hash = (
                app["name"],
                app["repo"],
                app["url"],
                app["hash"],
            )
            if name in self.apps:  # Skip frappe
                continue

            app_directory = os.path.join(self.apps_directory, repo)
            os.mkdir(app_directory)

            data["apps"][name] = {}
            log = data["apps"][name]
            log["clone"] = self.clone_app(url, hash, app_directory)
            log["get"] = self.get_app(url)

            output.append(log["clone"])
            output.append(log["get"])

        data["output"] = "\n".join(output)
        return data

    def clone_app(self, url, hash, dir):
        commands = []
        commands.append(self.server.execute("git init", dir))
        commands.append(
            self.server.execute(f"git remote add upstream {url}", dir)
        )
        commands.append(
            self.server.execute(
                f"git fetch --progress --depth 1 upstream {hash}", dir
            )
        )
        commands.append(self.server.execute(f"git checkout {hash}", dir))
        return "".join(c["output"] for c in commands)

    def get_app(self, url):
        return self.execute(
            f"bench get-app {url} --skip-assets", input="N\ny\n"
        )["output"]

    @step("Bench Setup NGINX")
    def setup_nginx(self):
        return self.execute("bench setup nginx --yes")

    @step("Bench Setup NGINX Target")
    def setup_nginx_target(self):
        return self.execute("bench setup nginx --yes")

    @step("Bench Setup Supervisor")
    def setup_supervisor(self):
        user = self.config["frappe_user"]
        return self.execute(f"sudo bench setup supervisor --user {user} --yes")

    @step("Bench Setup Production")
    def setup_production(self):
        processes = [
            "web",
            "schedule",
            "worker",
            "redis-queue",
            "redis-socketio",
            "redis-cache",
            "node-socketio",
        ]
        logs_directory = os.path.join(self.directory, "logs")
        for process in processes:
            stdout_log = os.path.join(logs_directory, f"{process}.log")
            stderr_log = os.path.join(logs_directory, f"{process}.error.log")
            open(stdout_log, "a").close()
            open(stderr_log, "a").close()

        user = self.config["frappe_user"]
        return self.execute(f"sudo bench setup production {user} --yes")

    @step("Bench Disable Production")
    def disable_production(self):
        return self.execute("sudo bench disable-production")

    @step("Bench Setup Redis")
    def setup_redis(self):
        return self.execute("bench setup redis")

    @step("Bench Setup Requirements")
    def setup_requirements(self):
        return self.execute("bench setup requirements")

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
    def update_config(self, value):
        new_config = self.config
        new_config.update(value)
        self.setconfig(new_config)

    @job("Update Bench Configuration")
    def update_config_job(self, value):
        self.update_config(value)
        self.setup_supervisor()
        self.server.update_supervisor()
        self.setup_nginx()
        self.server.reload_nginx()

    @property
    def job_record(self):
        return self.server.job_record

    @property
    def sites(self):
        sites = {}
        for directory in os.listdir(self.sites_directory):
            try:
                sites[directory] = Site(directory, self)
            except Exception:
                pass
        return sites

    @property
    def step_record(self):
        return self.server.step_record
