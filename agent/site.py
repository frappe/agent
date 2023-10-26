import json
import os
import re
import shutil
import time
from datetime import datetime
from typing import Dict

import requests

from agent.base import Base
from agent.job import job, step
from agent.utils import b2mb, get_size


class Site(Base):
    def __init__(self, name, bench):
        self.name = name
        self.bench = bench
        self.directory = os.path.join(self.bench.sites_directory, name)
        self.backup_directory = os.path.join(self.directory, ".migrate")
        self.logs_directory = os.path.join(self.directory, "logs")
        self.config_file = os.path.join(self.directory, "site_config.json")
        self.touched_tables_file = os.path.join(
            self.directory, "touched_tables.json"
        )
        self.previous_tables_file = os.path.join(
            self.directory, "previous_tables.json"
        )
        self.analytics_file = os.path.join(
            self.directory,
            "analytics.json",
        )

        if not os.path.isdir(self.directory):
            raise OSError(f"Path {self.directory} is not a directory")

        if not os.path.exists(self.config_file):
            raise OSError(f"Path {self.config_file} does not exist")

        self.database = self.config["db_name"]
        self.user = self.config["db_name"]
        self.password = self.config["db_password"]
        self.host = self.config.get("db_host", self.bench.host)

    def bench_execute(self, command, input=None):
        return self.bench.docker_execute(
            f"bench --site {self.name} {command}", input=input
        )

    def dump(self):
        return {"name": self.name}

    @step("Rename Site")
    def rename(self, new_name):
        os.rename(
            self.directory, os.path.join(self.bench.sites_directory, new_name)
        )
        self.name = new_name

    @job("Run After Migrate Steps")
    def run_after_migrate_steps_job(self, admin_password):
        """
        Run after migrate steps

        Used to run after-migrate steps for when migrations break.
        """
        self.set_admin_password(admin_password)
        self.bench.setup_nginx()
        self.bench.server.reload_nginx()
        self.disable_maintenance_mode()
        self.enable_scheduler()

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
        data["output"] = "\n".join(output)
        return data

    @step("Install App on Site")
    def install_app(self, app):
        return self.bench_execute(f"install-app {app}")

    @step("Uninstall App from Site")
    def uninstall_app(self, app):
        return self.bench_execute(f"uninstall-app {app} --yes --force")

    @step("Restore Site")
    def restore(
        self,
        mariadb_root_password,
        admin_password,
        database_file,
        public_file,
        private_file,
    ):
        sites_directory = self.bench.sites_directory
        database_file = database_file.replace(
            sites_directory, "/home/frappe/frappe-bench/sites"
        )
        public_file = public_file.replace(
            sites_directory, "/home/frappe/frappe-bench/sites"
        )
        private_file = private_file.replace(
            sites_directory, "/home/frappe/frappe-bench/sites"
        )

        public_file_option = (
            f"--with-public-files {public_file}" if public_file else ""
        )
        private_file_option = (
            f"--with-private-files {private_file} " if private_file else ""
        )

        _, temp_user, temp_password = self.bench.create_mariadb_user(
            self.name, mariadb_root_password, self.database
        )
        try:
            return self.bench_execute(
                "--force restore "
                f"--mariadb-root-username {temp_user} "
                f"--mariadb-root-password {temp_password} "
                f"--admin-password {admin_password} "
                f"{public_file_option} "
                f"{private_file_option} "
                f"{database_file}"
            )
        finally:
            self.bench.drop_mariadb_user(
                self.name, mariadb_root_password, self.database
            )

    @job("Restore Site")
    def restore_job(
        self,
        apps,
        mariadb_root_password,
        admin_password,
        database,
        public,
        private,
        skip_failing_patches,
    ):
        files = self.bench.download_files(self.name, database, public, private)
        try:
            self.restore(
                mariadb_root_password,
                admin_password,
                files["database"],
                files["public"],
                files["private"],
            )
        finally:
            self.bench.delete_downloaded_files(files["directory"])
        self.uninstall_unavailable_apps(apps)
        self.migrate(skip_failing_patches=skip_failing_patches)
        self.set_admin_password(admin_password)
        self.enable_scheduler()

        self.bench.setup_nginx()
        self.bench.server.reload_nginx()

        return self.bench_execute("list-apps")

    @job("Migrate Site")
    def migrate_job(self, skip_failing_patches=False, activate=True):
        self.migrate(skip_failing_patches=skip_failing_patches)
        if activate:
            self.disable_maintenance_mode()

    @step("Reinstall Site")
    def reinstall(
        self,
        mariadb_root_password,
        admin_password,
    ):
        _, temp_user, temp_password = self.bench.create_mariadb_user(
            self.name, mariadb_root_password, self.database
        )
        try:
            return self.bench_execute(
                f"reinstall --yes "
                f"--mariadb-root-username {temp_user} "
                f"--mariadb-root-password {temp_password} "
                f"--admin-password {admin_password}"
            )
        finally:
            self.bench.drop_mariadb_user(
                self.name, mariadb_root_password, self.database
            )

    @job("Reinstall Site")
    def reinstall_job(
        self,
        mariadb_root_password,
        admin_password,
    ):
        return self.reinstall(mariadb_root_password, admin_password)

    @job("Install App on Site")
    def install_app_job(self, app):
        self.install_app(app)

    @job("Uninstall App on Site")
    def uninstall_app_job(self, app):
        self.uninstall_app(app)

    @step("Update Site Configuration")
    def update_config(self, value, remove=None):
        """Pass Site Config value to update or replace existing site config.

        Args:
            value (dict): Site Config
            remove (list, optional): Keys sent in the form of a list will be
                popped from the existing site config. Defaults to None.
        """
        new_config = self.config
        new_config.update(value)

        if remove:
            for key in remove:
                new_config.pop(key, None)

        self.setconfig(new_config)

    @job("Add Domain", priority="high")
    def add_domain(self, domain):
        domains = set(self.config.get("domains", []))
        domains.add(domain)
        self.update_config({"domains": list(domains)})
        self.bench.setup_nginx()
        self.bench.server.reload_nginx()

    @job("Remove Domain", priority="high")
    def remove_domain(self, domain):
        domains = set(self.config.get("domains", []))
        domains.discard(domain)
        self.update_config({"domains": list(domains)})
        self.bench.setup_nginx()
        self.bench.server.reload_nginx()

    def create_database_access_credentials(self, mode, mariadb_root_password):
        database = self.database
        user = f"{self.user}_{mode}"
        password = self.bench.get_random_string(16)
        privileges = {
            "read_only": "SELECT",
            "read_write": "ALL",
        }.get(mode, "SELECT")
        queries = [
            f"CREATE OR REPLACE USER '{user}'@'%' IDENTIFIED BY '{password}'",
            f"GRANT {privileges} ON {database}.* TO '{user}'@'%'",
            "FLUSH PRIVILEGES",
        ]
        for query in queries:
            command = (
                f"mysql -h {self.host} -uroot -p{mariadb_root_password}"
                f' -e "{query}"'
            )
            self.execute(command)
        return {"database": database, "user": user, "password": password}

    def revoke_database_access_credentials(self, user, mariadb_root_password):
        if user == self.user:
            # Do not revoke access for the main user
            return {}
        queries = [
            f"DROP USER IF EXISTS '{user}'@'%'",
            "FLUSH PRIVILEGES",
        ]
        for query in queries:
            command = (
                f"mysql -h {self.host} -uroot -p{mariadb_root_password}"
                f' -e "{query}"'
            )
            self.execute(command)
        return {}

    @job("Setup ERPNext", priority="high")
    def setup_erpnext(self, user, config):
        self.create_user(
            user["email"],
            user["first_name"],
            user["last_name"],
        )
        self.update_erpnext_config(config)
        return {"sid": self.sid(user["email"])}

    @job("Restore Site Tables", priority="high")
    def restore_site_tables_job(self, activate):
        self.restore_site_tables()
        if activate:
            self.disable_maintenance_mode()

    @step("Restore Site Tables")
    def restore_site_tables(self):
        data = {"tables": {}}
        for backup_file in os.listdir(self.backup_directory):
            backup_file_path = os.path.join(self.backup_directory, backup_file)
            output = self.execute(
                "set -o pipefail && "
                f"gunzip -c '{backup_file_path}' | "
                f"mysql -h {self.host} -u {self.user} -p{self.password} "
                f"{self.database}",
                executable="/bin/bash",
            )
            data["tables"][backup_file] = output
        return data

    @step("Update ERPNext Configuration")
    def update_erpnext_config(self, value):
        config_file = os.path.join(self.directory, "journeys_config.json")
        with open(config_file, "r") as f:
            config = json.load(f)

        config.update(value)

        with open(config_file, "w") as f:
            json.dump(config, f, indent=1, sort_keys=True)

    @step("Create User")
    def create_user(self, email, first_name, last_name):
        return self.bench_execute(
            f"add-system-manager {email} "
            f"--first-name {first_name} --last-name {last_name}"
        )

    @job("Update Site Configuration", priority="high")
    def update_config_job(self, value, remove):
        self.update_config(value, remove)

    @job("Reset Site Usage", priority="high")
    def reset_site_usage_job(self):
        return self.reset_site_usage()

    @step("Reset Site Usage")
    def reset_site_usage(self):
        pattern = f"{self.database}|rate-limit-counter-[0-9]*"
        keys_command = f"redis-cli --raw -p 13000 KEYS '{pattern}'"
        keys = self.bench.docker_execute(keys_command)
        data = {"keys": keys, "get": [], "delete": []}
        for key in keys["output"].splitlines():
            get = self.bench.docker_execute(f"redis-cli -p 13000 GET '{key}'")
            delete = self.bench.docker_execute(
                f"redis-cli -p 13000 DEL '{key}'"
            )
            data["get"].append(get)
            data["delete"].append(delete)
        return data

    @job("Update Saas Plan")
    def update_saas_plan(self, plan):
        self.update_plan(plan)

    @step("Update Saas Plan")
    def update_plan(self, plan):
        self.bench_execute(f"update-site-plan {plan}")

    @step("Backup Site")
    def backup(self, with_files=False):
        with_files = "--with-files" if with_files else ""
        self.bench_execute(f"backup {with_files}")
        return self.fetch_latest_backup(with_files=with_files)

    @step("Upload Site Backup to S3")
    def upload_offsite_backup(self, backup_files, offsite):
        import boto3

        offsite_files = {}
        bucket, auth, prefix = (
            offsite["bucket"],
            offsite["auth"],
            offsite["path"],
        )
        region = auth.get("REGION")

        if region:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=auth["ACCESS_KEY"],
                aws_secret_access_key=auth["SECRET_KEY"],
                region_name=region,
            )
        else:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=auth["ACCESS_KEY"],
                aws_secret_access_key=auth["SECRET_KEY"],
            )

        for backup_file in backup_files.values():
            file_name = backup_file["file"].split(os.sep)[-1]
            offsite_path = os.path.join(prefix, file_name)
            offsite_files[file_name] = offsite_path

            with open(backup_file["path"], "rb") as data:
                s3.upload_fileobj(data, bucket, offsite_path)

        return offsite_files

    @step("Enable Maintenance Mode")
    def enable_maintenance_mode(self):
        return self.bench_execute("set-maintenance-mode on")

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
                output = self.bench_execute("ready-for-migration")
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
        tables = self.tables
        json.dump(
            tables,
            open(self.previous_tables_file, "w"),
            indent=4,
            sort_keys=True,
        )
        data = {"tables": {}}
        for table in tables:
            backup_file = os.path.join(
                self.backup_directory, f"{table}.sql.gz"
            )
            output = self.execute(
                "set -o pipefail && "
                "mysqldump --single-transaction --quick --lock-tables=false "
                f"-h {self.host} -u {self.user} -p{self.password} "
                f"{self.database} '{table}' "
                f" | gzip > '{backup_file}'",
                executable="/bin/bash",
            )
            data["tables"][table] = output
        return data

    @step("Run App Specific Scripts")
    def run_app_scripts(self, scripts: Dict[str, str]):
        for app_name in scripts:
            script = scripts[app_name]
            self.bench_execute("console", input=script)

    @step("Migrate Site")
    def migrate(self, skip_search_index=False, skip_failing_patches=False):
        cmd = "migrate"
        if skip_search_index:
            cmd += " --skip-search-index"
        if skip_failing_patches:
            cmd += " --skip-failing"
        return self.bench_execute(cmd)

    @step("Build Search Index")
    def build_search_index(self):
        return self.bench_execute("build-search-index")

    @job("Clear Cache")
    def clear_cache_job(self):
        self.clear_cache()
        self.clear_website_cache()

    @step("Clear Cache")
    def clear_cache(self):
        return self.bench_execute("clear-cache")

    @step("Clear Website Cache")
    def clear_website_cache(self):
        return self.bench_execute("clear-website-cache")

    @step("Uninstall Unavailable Apps")
    def uninstall_unavailable_apps(self, apps_to_keep):
        installed_apps = json.loads(
            self.bench_execute("execute frappe.get_installed_apps")["output"]
        )
        for app in installed_apps:
            if app not in apps_to_keep:
                self.bench_execute(f"remove-from-installed-apps '{app}'")
                self.bench_execute("clear-cache")

    @step("Disable Maintenance Mode")
    def disable_maintenance_mode(self):
        return self.bench_execute("set-maintenance-mode off")

    @step("Restore Touched Tables")
    def restore_touched_tables(self):
        data = {"restored": {}}
        try:
            tables_to_restore = self.touched_tables
        except Exception:
            tables_to_restore = self.previous_tables
        for table in tables_to_restore:
            backup_file = os.path.join(
                self.backup_directory, f"{table}.sql.gz"
            )
            if os.path.exists(backup_file):
                output = self.execute(
                    "set -o pipefail && "
                    f"gunzip -c '{backup_file}' | "
                    f"mysql -h {self.host} -u {self.user} -p{self.password} "
                    f"{self.database}",
                    executable="/bin/bash",
                )
                data["restored"][table] = output

        dropped_tables = self.drop_new_tables()
        data.update(dropped_tables)
        return data

    def drop_new_tables(self):
        new_tables = set(self.tables) - set(self.previous_tables)
        data = {"dropped": {}}
        for table in new_tables:
            output = self.execute(
                f"mysql -h {self.host} -u {self.user} -p{self.password} "
                f"{self.database} -e 'DROP TABLE `{table}`'"
            )
            data["dropped"][table] = output
        return data

    @step("Pause Scheduler")
    def pause_scheduler(self):
        return self.bench_execute("scheduler pause")

    @step("Enable Scheduler")
    def enable_scheduler(self):
        return self.bench_execute("scheduler enable")

    @step("Resume Scheduler")
    def resume_scheduler(self):
        return self.bench_execute("scheduler resume")

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

        doctor = self.bench_execute("doctor")
        if "inactive" in doctor["output"]:
            data["scheduler"] = False

        return data

    def get_timezone(self):
        return self.timezone

    def fetch_site_info(self):
        data = {
            "config": self.config,
            "timezone": self.get_timezone(),
            "usage": self.get_usage(),
        }
        return data

    def fetch_site_analytics(self):
        return json.load(open(self.analytics_file))

    def sid(self, user="Administrator"):
        code = f"""import frappe
from frappe.app import init_request
try:
    from frappe.utils import set_request
except ImportError:
    from frappe.tests import set_request
set_request()
frappe.app.init_request(frappe.local.request)
frappe.local.login_manager.login_as("{user}")
print(">>>" + frappe.session.sid + "<<<")

"""

        output = self.bench_execute("console", input=code)["output"]
        sid = re.search(r">>>(.*)<<<", output).group(1)
        if not sid or sid == user:  # case when it fails
            output = self.bench_execute(f"browse --user {user}")["output"]
            sid = re.search(r"\?sid=([a-z0-9]*)", output).group(1)
        return sid

    @property
    def timezone(self):
        query = (
            f"select defvalue from {self.database}.tabDefaultValue where"
            " defkey = 'time_zone' and parent = '__default'"
        )
        try:
            timezone = self.execute(
                f"mysql -h {self.host} -u{self.database} -p{self.password} "
                f'-sN -e "{query}"'
            )["output"].strip()
        except Exception:
            timezone = ""
        return timezone

    @property
    def tables(self):
        return self.execute(
            "mysql --disable-column-names -B -e 'SHOW TABLES' "
            f"-h {self.host} -u {self.user} -p{self.password} {self.database}",
            remove_crs=False,
        )["output"].split("\n")

    @property
    def touched_tables(self):
        with open(self.touched_tables_file, "r") as f:
            return json.load(f)

    @property
    def previous_tables(self):
        with open(self.previous_tables_file, "r") as f:
            return json.load(f)

    @job("Backup Site", priority="low")
    def backup_job(self, with_files=False, offsite=None):
        backup_files = self.backup(with_files)
        uploaded_files = (
            self.upload_offsite_backup(backup_files, offsite)
            if (offsite and backup_files)
            else {}
        )
        return {"backups": backup_files, "offsite": uploaded_files}

    @job("Optimize Tables")
    def optimize_tables_job(self):
        return self.optimize_tables()

    @step("Optimize Tables")
    def optimize_tables(self):
        tables = [row[0] for row in self.get_database_free_tables()]
        for table in tables:
            query = f"OPTIMIZE TABLE `{table}`"
            self.execute(
                f"mysql -sN -h {self.host} -u{self.user} -p{self.password}"
                f" {self.database} -e '{query}'"
            )

    def fetch_latest_backup(self, with_files=True):
        databases, publics, privates, site_configs = [], [], [], []
        backup_directory = os.path.join(self.directory, "private", "backups")

        for file in os.listdir(backup_directory):
            path = os.path.join(backup_directory, file)
            if file.endswith("database.sql.gz") or file.endswith(
                "database-enc.sql.gz"
            ):
                databases.append(path)
            elif file.endswith("private-files.tar") or file.endswith(
                "private-files-enc.tar"
            ):
                privates.append(path)
            elif file.endswith("files.tar") or file.endswith("files-enc.tar"):
                publics.append(path)
            elif file.endswith("site_config_backup.json") or file.endswith(
                "site_config_backup-enc.json"
            ):
                site_configs.append(path)

        backups = {
            "database": {"path": max(databases, key=os.path.getmtime)},
            "site_config": {"path": max(site_configs, key=os.path.getmtime)},
        }

        if with_files:
            backups["private"] = {"path": max(privates, key=os.path.getmtime)}
            backups["public"] = {"path": max(publics, key=os.path.getmtime)}

        for backup in backups.values():
            file = os.path.basename(backup["path"])
            backup["file"] = file
            backup["size"] = os.stat(backup["path"]).st_size
            backup["url"] = f"https://{self.name}/backups/{file}"

        return backups

    def get_usage(self):
        """Returns Usage in bytes"""
        backup_directory = os.path.join(self.directory, "private", "backups")
        public_directory = os.path.join(self.directory, "public")
        private_directory = os.path.join(self.directory, "private")
        backup_directory_size = get_size(backup_directory)

        return {
            "database": b2mb(self.get_database_size()),
            "database_free_tables": self.get_database_free_tables(),
            "database_free": b2mb(self.get_database_free_size()),
            "public": b2mb(get_size(public_directory)),
            "private": b2mb(
                get_size(private_directory) - backup_directory_size
            ),
            "backups": b2mb(backup_directory_size),
        }

    def get_analytics(self):
        analytics = self.bench_execute("execute frappe.utils.get_site_info")[
            "output"
        ]
        return json.loads(analytics)

    def get_database_size(self):
        # only specific to mysql/mariaDB. use a different query for postgres.
        # or try using frappe.db.get_database_size if possible
        query = (
            "SELECT SUM(`data_length` + `index_length`)"
            " FROM information_schema.tables"
            f' WHERE `table_schema` = "{self.database}"'
            " GROUP BY `table_schema`"
        )
        command = (
            f"mysql -sN -h {self.host} -u{self.user} -p{self.password}"
            f" -e '{query}'"
        )
        database_size = self.execute(command).get("output")

        try:
            return int(database_size)
        except Exception:
            return 0

    def get_database_free_size(self):
        query = (
            "SELECT SUM(`data_free`)"
            " FROM information_schema.tables"
            f' WHERE `table_schema` = "{self.database}"'
            " GROUP BY `table_schema`"
        )
        command = (
            f"mysql -sN -h {self.host} -u{self.user} -p{self.password}"
            f" -e '{query}'"
        )
        database_size = self.execute(command).get("output")

        try:
            return int(database_size)
        except Exception:
            return 0

    def get_database_free_tables(self):
        try:
            query = (
                "SELECT `table_name`,"
                " round((`data_free` / 1024 / 1024), 2)"
                " FROM information_schema.tables"
                f' WHERE `table_schema` = "{self.database}"'
                " AND ((`data_free / (`data_length` + `index_length`)) > 0.2"
                " OR `data_free` > 100 * 1024 * 1024)"
            )
            command = (
                f"mysql -sN -h {self.host} -u{self.user} -p{self.password}"
                f" -e '{query}'"
            )
            output = self.execute(command).get("output")
            return [line.split("\t") for line in output.splitlines()]
        except Exception:
            return []

    @property
    def job_record(self):
        return self.bench.server.job_record

    @property
    def step_record(self):
        return self.bench.server.step_record
