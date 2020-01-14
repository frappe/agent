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

    def execute(self, command):
        return super().execute(command, directory=self.directory)

    @step("Bench Reset Apps")
    def reset_apps(self, apps):
        for app in apps:
            name, branch, repo, hash = (
                app["name"],
                app["branch"],
                app["repo"],
                app["hash"],
            )
            if name != "frappe":
                self.execute(f"get-app --branch {branch} {repo} {name}")
            self.apps[name].reset(hash)
        return

    @step("Bench Setup Production")
    def setup_production(self):
        user = self.config["frappe_user"]
        return self.execute(f"bench setup production --yes {user}")

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
