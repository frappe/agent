from agent.base import Base
from agent.job import step, job
import os
import json
import shutil
import time


class Site(Base):
    def __init__(self, name, bench):
        self.name = name
        self.bench = bench
        self.directory = os.path.join(self.bench.sites_directory, name)
        self.backup_directory = os.path.join(self.directory, ".migrate")
        self.config_file = os.path.join(self.directory, "site_config.json")
        self.touched_tables_file = os.path.join(
            self.directory, "touched_tables.json"
        )
        if not (
            os.path.isdir(self.directory) and os.path.exists(self.config_file)
        ):
            raise Exception
        self.database = self.config["db_name"]
        self.user = self.config["db_name"]
        self.password = self.config["db_password"]

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
                log["install"] = self.bench_execute(f"install-app {app}")
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

    @step("Enable Maintenance Mode")
    def enable_maintenance_mode(self):
        return self.bench.execute(
            f"bench --site {self.name} set-maintenance-mode on"
        )

    @step("Wait for Enqueued Jobs")
    def wait_till_ready(self):
        WAIT_TIMEOUT = 120
        data = {"tries": []}
        start = time.time()
        while (time.time() - start) < WAIT_TIMEOUT:
            try:
                output = self.bench.execute(
                    f"bench --site {self.name} ready-for-migration"
                )
                data["tries"].append(output)
                break
            except Exception as e:
                data["tries"].append(e.data)
                time.sleep(1)
        return data

    @step("Clear Backup Directory")
    def clear_backup_directory(self):
        if os.path.exists(self.backup_directory):
            shutil.rmtree(self.backup_directory)
        os.mkdir(self.backup_directory)

    @step("Backup Site Tables")
    def tablewise_backup(self):
        data = {"tables": {}}
        for table in self.tables:
            backup_file = os.path.join(self.backup_directory, f"{table}.sql")
            output = self.execute(
                f"mysqldump --single-transaction --quick --lock-tables=false "
                f"-u {self.user} -p{self.password} {self.database} '{table}' "
                f"> '{backup_file}'"
            )
            data["tables"][table] = output
        return data

    @step("Migrate Site")
    def migrate(self):
        return self.bench.execute(f"bench --site {self.name} migrate")

    @step("Disable Maintenance Mode")
    def disable_maintenance_mode(self):
        return self.bench.execute(
            f"bench --site {self.name} set-maintenance-mode off"
        )

    @step("Restore Touched Tables")
    def restore_touched_tables(self):
        data = {"tables": {}}
        for table in self.touched_tables:
            backup_file = os.path.join(self.backup_directory, f"{table}.sql")
            if os.path.exists(backup_file):
                output = self.execute(
                    f"mysql -u {self.user} -p{self.password} {self.database} "
                    f"< '{backup_file}'"
                )
                data["tables"][table] = output
        return data

    @step("Pause Scheduler")
    def pause_scheduler(self):
        self.bench.execute(f"bench --site {self.name} scheduler pause")

    @step("Resume Scheduler")
    def resume_scheduler(self):
        self.bench.execute(f"bench --site {self.name} scheduler resume")

    @property
    def tables(self):
        return self.execute(
            f"mysql --disable-column-names -B -e 'SHOW TABLES' "
            f"-u {self.user} -p{self.password} {self.database}"
        )["output"].split("\n")

    @property
    def touched_tables(self):
        with open(self.touched_tables_file, "r") as f:
            return json.load(f)

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
