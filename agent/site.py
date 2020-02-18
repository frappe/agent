from agent.base import Base
from agent.job import step, job
import os
import json


class Site(Base):
    def __init__(self, name, bench):
        self.name = name
        self.bench = bench
        self.directory = os.path.join(self.bench.sites_directory, name)
        self.config_file = os.path.join(self.directory, "site_config.json")
        if not (
            os.path.isdir(self.directory) and os.path.exists(self.config_file)
        ):
            raise Exception

    def bench_execute(self, command):
        return self.bench.execute(f"bench --site {self.name} {command}")

    def dump(self):
        return {"name": self.name}

    @step("Install Apps")
    def install_apps(self, apps):
        data = {"apps": {}}
        output = []
        for app in apps:
            data["apps"][app] = {}
            log = data["apps"][app]
            if app != "frappe":
                log["install"] = self.bench_execute(
                    f"install-app {app}"
                )
                output.append(log["install"]["output"])
        return data

    @step("Site Update Configuration")
    def update_config(self, value):
        new_config = self.config
        new_config.update(value)
        self.setconfig(new_config)

    @step("Backup Site")
    def backup(self):
        return self.bench.execute(f"bench --site {self.name} backup")

    @job("Backup Site")
    def backup_job(self):
        self.backup()

    def setconfig(self, value):
        with open(self.config_file, "w") as f:
            json.dump(value, f, indent=1, sort_keys=True)

    @property
    def job_record(self):
        return self.bench.server.job_record

    @property
    def step_record(self):
        return self.bench.server.step_record
