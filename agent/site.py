from agent.base import Base
from agent.job import step, job
import os
import json
import requests
import shutil
import time
from datetime import datetime


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

    @step("Install App on Site")
    def install_app(self, app):
        return self.bench_execute(f"install-app {app}")

    @step("Restore Site")
    def restore(
        self,
        mariadb_root_password,
        admin_password,
        database_file,
        public_file,
        private_file,
    ):
        return self.bench_execute(
            f"--force restore "
            f"--mariadb-root-password {mariadb_root_password} "
            f"--admin-password {admin_password} "
            f"--with-public-files {public_file} "
            f"--with-private-files {private_file} {database_file}"
        )

    @step("Reinstall Site")
    def reinstall(
        self, mariadb_root_password, admin_password,
    ):
        return self.bench_execute(
            f"reinstall --yes "
            f"--mariadb-root-password {mariadb_root_password} "
            f"--admin-password {admin_password}"
        )

    @job("Reinstall Site")
    def reinstall_job(
        self, mariadb_root_password, admin_password,
    ):
        return self.reinstall(mariadb_root_password, admin_password)

    @job("Install App on Site")
    def install_app_job(self, app):
        self.install_app(app)

    @step("Update Site Configuration")
    def update_config(self, value):
        new_config = self.config
        new_config.update(value)
        self.setconfig(new_config)

    @job("Update Site Configuration")
    def update_config_job(self, value):
        self.update_config(value)

    @step("Backup Site")
    def backup(self):
        return self.bench.execute(f"bench --verbose --site {self.name} backup")

    @step("Enable Maintenance Mode")
    def enable_maintenance_mode(self):
        return self.bench.execute(
            f"bench --site {self.name} set-maintenance-mode on"
        )

    @step("Set Administrator Password")
    def set_admin_password(self, password):
        return self.bench_execute(f"set-admin-password {password}")

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

    @step("Uninstall Unavailable Apps")
    def uninstall_unavailable_apps(self, apps_to_keep):
        installed_apps = self.bench_execute("list-apps")["output"].split("\n")
        for app in installed_apps:
            if app not in apps_to_keep:
                self.bench_execute(f"remove-from-installed-apps '{app}'")

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
        return self.bench.execute(f"bench --site {self.name} scheduler pause")

    @step("Enable Scheduler")
    def enable_scheduler(self):
        return self.bench.execute(f"bench --site {self.name} scheduler enable")

    @step("Resume Scheduler")
    def resume_scheduler(self):
        return self.bench.execute(f"bench --site {self.name} scheduler resume")

    def fetch_site_status(self):
        data = {
            "scheduler": True,
            "web": True,
            "timestamp": str(datetime.now()),
        }
        try:
            ping_url = f"https://{self.name}/api/method/ping"
            data["web"] = requests.get(ping_url).status_code == 200
        except Exception:
            data["web"] = False

        doctor = self.bench.execute(f"bench --site {self.name} doctor")
        if "inactive" in doctor["output"]:
            data["scheduler"] = False

        return data

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
        backup = self.backup()
        database = backup["output"].split(" - ")[1].split("/")[-1]
        file = os.path.join(self.directory, "private", "backups", database)
        return {
            "database": database,
            "size": os.stat(file).st_size,
            "url": f"https://{self.name}/backups/{database}",
        }

    def setconfig(self, value):
        with open(self.config_file, "w") as f:
            json.dump(value, f, indent=1, sort_keys=True)

    @property
    def job_record(self):
        return self.bench.server.job_record

    @property
    def step_record(self):
        return self.bench.server.step_record
