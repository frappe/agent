import json
import os

from agent.app import App
from agent.base import Base
from agent.job import job, step
from agent.site import Site


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

    def execute(self, command):
        return super().execute(command, directory=self.directory)

    @step("New Site")
    def bench_new_site(self, name, mariadb_root_password, admin_password):
        return self.execute(
            f"bench new-site "
            f"--admin-password {admin_password} "
            f"--mariadb-root-password {mariadb_root_password} "
            f"{name}"
        )

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

    @step("Archive Site")
    def bench_archive_site(self, name, mariadb_root_password):
        return self.execute(
            f"bench drop-site {name}"
            f"--root-password {mariadb_root_password} --no-backup"
        )

    @job("Archive Site")
    def archive_site(self, name, mariadb_root_password):
        self.bench_archive_site(name, mariadb_root_password)
        self.setup_nginx()
        self.server.reload_nginx()

    @step("Bench Reset Apps")
    def reset_apps(self, apps):
        data = {"apps": {}}
        output = []

        for app in apps:
            name, hash = app["name"], app["hash"]
            data["apps"][name] = {}
            log = data["apps"][name]
            log["fetch"] = self.apps[name].fetch()
            log["reset"] = self.apps[name].reset(hash)

            output.append(log["fetch"]["output"])
            output.append(log["reset"]["output"])

        data["output"] = "\n".join(output)
        return data

    @step("Bench Get Apps")
    def get_apps(self, apps):
        data = {"apps": {}}
        output = []

        for app in apps:
            name, branch, repo = app["name"], app["branch"], app["repo"]
            data["apps"][name] = {}
            log = data["apps"][name]
            if name not in self.apps:
                log["get"] = self.execute(
                    f"bench get-app --branch {branch} {repo} {name}"
                )

                output.append(log["get"]["output"])

        data["output"] = "\n".join(output)
        return data

    @step("Bench Setup NGINX")
    def setup_nginx(self):
        return self.execute(f"bench setup nginx --yes")

    @step("Bench Setup Production")
    def setup_production(self):
        user = self.config["frappe_user"]
        return self.execute(f"sudo bench setup production {user} --yes")

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

    @step("Bench Set Configuration")
    def setconfig(self, value):
        with open(self.config_file, "w") as f:
            json.dump(value, f, indent=1, sort_keys=True)

    @job("Bench Set Configuration")
    def setconfig_job(self, value):
        self.setconfig(value)

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
