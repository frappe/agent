from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import time
from datetime import datetime
from shlex import quote
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import requests

from agent.base import AgentException, Base
from agent.database import Database
from agent.job import job, step
from agent.utils import b2mb, compute_file_hash, get_size

if TYPE_CHECKING:
    from agent.bench import Bench


class Site(Base):
    def __init__(self, name: str, bench: Bench):
        super().__init__()

        self.name = name
        self.bench = bench
        self.directory = os.path.join(self.bench.sites_directory, name)
        self.backup_directory = os.path.join(self.directory, ".migrate")
        self.logs_directory = os.path.join(self.directory, "logs")
        self.config_file = os.path.join(self.directory, "site_config.json")
        self.touched_tables_file = os.path.join(self.directory, "touched_tables.json")
        self.previous_tables_file = os.path.join(self.directory, "previous_tables.json")
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
        self.db_port = self.config.get("db_port", self.bench.db_port)

    def bench_execute(self, command, input=None):
        return self.bench.docker_execute(f"bench --site {self.name} {command}", input=input)

    def dump(self):
        return {"name": self.name}

    @step("Rename Site")
    def rename(self, new_name):
        os.rename(self.directory, os.path.join(self.bench.sites_directory, new_name))
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
        try:
            return self.bench_execute(f"install-app {app} --force")
        except AgentException as e:
            if "Error: no such option: --force" in e.data["output"]:
                return self.bench_execute(f"install-app {app}")  # not available in < v14
            raise

    @step("Uninstall App from Site")
    def uninstall_app(self, app):
        return self.bench_execute(f"uninstall-app {app} --yes --force")

    @step("Restore Site")
    def restore_site(
        self,
        mariadb_root_password,
        admin_password,
        database_file,
        public_file,
        private_file,
    ):
        sites_directory = self.bench.sites_directory
        database_file = database_file.replace(sites_directory, "/home/frappe/frappe-bench/sites")
        public_file = public_file.replace(sites_directory, "/home/frappe/frappe-bench/sites")
        private_file = private_file.replace(sites_directory, "/home/frappe/frappe-bench/sites")

        public_file_option = f"--with-public-files {public_file}" if public_file else ""
        private_file_option = f"--with-private-files {private_file} " if private_file else ""

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
            self.bench.drop_mariadb_user(self.name, mariadb_root_password, self.database)

    @step("Restore Files")
    def restore_files(
        self,
        public_file=None,
        private_file=None,
    ):
        """Restore files from the given paths."""
        sites_directory = self.bench.sites_directory

        if public_file:
            dir_path = os.path.join(sites_directory, self.name, "public", "files")
            try:
                shutil.rmtree(dir_path, ignore_errors=True)
            finally:
                os.makedirs(dir_path, exist_ok=True)

            self.execute(
                f"tar {'z' if public_file.endswith('.tgz') else ''}xvf {public_file} --strip 2",
                directory=os.path.join(sites_directory, self.name),
            )

        if private_file:
            dir_path = os.path.join(sites_directory, self.name, "private", "files")
            try:
                shutil.rmtree(dir_path, ignore_errors=True)
            finally:
                os.makedirs(dir_path, exist_ok=True)

            self.execute(
                f"tar {'z' if private_file.endswith('.tgz') else ''}xvf {private_file} --strip 2",
                directory=os.path.join(sites_directory, self.name),
            )

    @step("Checksum of Downloaded Backup Files")
    def calculate_checksum_of_backup_files(self, database_file, public_file, private_file):
        database_file_sha256 = compute_file_hash(database_file, algorithm="sha256", raise_exception=False)

        data = f"""Database File
> File Name - {os.path.basename(database_file)}
> SHA256 Checksum - {database_file_sha256}\n"""
        if public_file:
            public_file_sha256 = compute_file_hash(public_file, algorithm="sha256", raise_exception=False)
            data += f"""\nPublic File
> File Name - {os.path.basename(public_file)}
> SHA256 Checksum - {public_file_sha256}\n"""
        if private_file:
            private_file_sha256 = compute_file_hash(private_file, algorithm="sha256", raise_exception=False)
            data += f"""\nPrivate File
> File Name - {os.path.basename(private_file)}
> SHA256 Checksum - {private_file_sha256}\n"""

        return {"output": data}

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
        is_database_restoration_required = False
        try:
            if files["database"]:
                is_database_restoration_required = True
                self.restore_site(
                    mariadb_root_password,
                    admin_password,
                    files["database"],
                    files["public"],
                    files["private"],
                )
            else:
                self.restore_files(
                    public_file=files["public"],
                    private_file=files["private"],
                )
        except Exception:
            self.calculate_checksum_of_backup_files(files["database"], files["public"], files["private"])
            raise
        finally:
            self.bench.delete_downloaded_files(files["directory"])

        if is_database_restoration_required:
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
            self.bench.drop_mariadb_user(self.name, mariadb_root_password, self.database)

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
        new_config = self.get_config(for_update=True)
        new_config.update(value)

        if remove:
            for key in remove:
                new_config.pop(key, None)

        self.set_config(new_config)

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
            command = f'mysql -h {self.host} -P {self.db_port} -uroot -p{mariadb_root_password} -e "{query}"'
            self.execute(command)
        return {"database": database, "user": user, "password": password}

    def revoke_database_access_credentials(self, user, mariadb_root_password):
        if user == self.user:
            # Do not revoke access for the main user
            return {}
        self.db_instance("root", mariadb_root_password).remove_user(user)
        return {}

    @job("Create Database User", priority="high")
    def create_database_user_job(self, user, password, mariadb_root_password):
        return self.create_database_user(user, password, mariadb_root_password)

    @step("Create Database User")
    def create_database_user(self, user, password, mariadb_root_password):
        if user == self.user:
            # Do not perform any operation for the main user
            return {}
        self.db_instance("root", mariadb_root_password).create_user(user, password)
        return {
            "database": self.database,
        }

    @job("Remove Database User", priority="high")
    def remove_database_user_job(self, user, mariadb_root_password):
        return self.remove_database_user(user, mariadb_root_password)

    @step("Remove Database User")
    def remove_database_user(self, user, mariadb_root_password):
        if user == self.user:
            # Do not perform any operation for the main user
            return {}
        self.db_instance("root", mariadb_root_password).remove_user(user)
        return {}

    @job("Modify Database User Permissions", priority="high")
    def modify_database_user_permissions_job(self, user, mode, permissions, mariadb_root_password):
        return self.modify_database_user_permissions(user, mode, permissions, mariadb_root_password)

    @step("Modify Database User Permissions")
    def modify_database_user_permissions(self, user, mode, permissions, mariadb_root_password):
        if user == self.user:
            # Do not perform any operation for the main user
            return {}
        self.db_instance("root", mariadb_root_password).modify_user_permissions(user, mode, permissions)
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
                f"mysql -h {self.host} -P {self.db_port} -u {self.user} -p{self.password} "
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
    def create_user(self, email, first_name, last_name, password=None):
        first_name = quote(first_name)
        last_name = quote(last_name)
        if password:
            password = quote(password)
        command = f"add-system-manager {email} --first-name {first_name} --last-name {last_name}"
        if password:
            command += f" --password {password}"
        return self.bench_execute(command)

    @step("Complete Setup Wizard")
    def complete_setup_wizard(self, data):
        payload = {"args": data}
        payload = quote(json.dumps(payload))
        command = f"execute frappe.desk.page.setup_wizard.setup_wizard.setup_complete --kwargs {payload}"
        return self.bench_execute(command)

    @job("Update Site Configuration", priority="high")
    def update_config_job(self, value, remove):
        self.update_config(value, remove)

    @job("Reset Site Usage", priority="high")
    def reset_site_usage_job(self):
        return self.reset_site_usage()

    @step("Reset Site Usage")
    def reset_site_usage(self):
        pattern = f"{self.database}|rate-limit-counter-[0-9]*"
        password = urlparse(self.bench.config.get("redis_cache")).password
        password_arg = f"-a '{password}'" if password else ""
        keys_command = f"redis-cli --raw -p 13000 {password_arg} KEYS '{pattern}'"
        keys = self.bench.docker_execute(keys_command)
        data = {"keys": keys, "get": [], "delete": []}
        for key in keys["output"].splitlines():
            get = self.bench.docker_execute(f"redis-cli -p 13000 {password_arg} GET '{key}'")
            delete = self.bench.docker_execute(f"redis-cli -p 13000 {password_arg} DEL '{key}'")
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
    def upload_offsite_backup(self, backup_files, offsite, keep_files_locally_after_offsite_backup: bool):
        import boto3

        offsite_files = {}
        try:
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
        finally:
            if not keep_files_locally_after_offsite_backup:
                for backup_file in backup_files.values():
                    with contextlib.suppress(FileNotFoundError):
                        os.remove(backup_file["path"])

        return offsite_files

    @step("Enable Maintenance Mode")
    def enable_maintenance_mode(self):
        return self._enable_maintenance_mode()

    def _enable_maintenance_mode(self):
        return self.bench_execute("set-maintenance-mode on")

    @step("Set Administrator Password")
    def set_admin_password(self, password):
        return self.bench_execute(f"set-admin-password {password}")

    @step("Wait for Enqueued Jobs")
    def wait_till_ready(self):
        WAIT_TIMEOUT = 300
        data = {"tries": []}
        start = time.time()
        is_ready = False
        while (time.time() - start) < WAIT_TIMEOUT:
            try:
                output = self.bench_execute("ready-for-migration")
                data["tries"].append(output)
                is_ready = True
                break
            except Exception as e:
                data["tries"].append(e.data)
                time.sleep(1)

        if not is_ready:
            raise Exception(
                f"Site not ready for migration after {WAIT_TIMEOUT}s."
                f" Site might have lot of jobs in queue. Try again later."
            )

        return data

    @step("Clear Backup Directory")
    def clear_backup_directory(self):
        if os.path.exists(self.backup_directory):
            shutil.rmtree(self.backup_directory)
        os.mkdir(self.backup_directory)

    @step("Backup Site Tables")
    def tablewise_backup(self):
        tables = self.tables
        with open(self.previous_tables_file, "w") as ptf:
            json.dump(tables, ptf, indent=4, sort_keys=True)

        data = {"tables": {}}
        for table in tables:
            backup_file = os.path.join(self.backup_directory, f"{table}.sql.gz")
            output = self.execute(
                "set -o pipefail && "
                "mysqldump --single-transaction --quick --lock-tables=false "
                f"-h {self.host} -P {self.db_port} -u {self.user} -p{self.password} "
                f"{self.database} '{table}' "
                f" | gzip > '{backup_file}'",
                executable="/bin/bash",
            )
            data["tables"][table] = output
        return data

    @step("Run App Specific Scripts")
    def run_app_scripts(self, scripts: dict[str, str]):
        for app_name in scripts:
            script = scripts[app_name]
            self.bench_execute("console", input=script)

    @step("Migrate Site")
    def migrate(self, skip_search_index=False, skip_failing_patches=False):
        return self._migrate(
            skip_search_index,
            skip_failing_patches,
        )

    def _migrate(
        self,
        skip_search_index: bool = False,
        skip_failing_patches: bool = False,
    ):
        cmd = "migrate"
        if skip_search_index:
            cmd += " --skip-search-index"
        if skip_failing_patches:
            cmd += " --skip-failing"
        return self.bench_execute(cmd)

    @step("Log Touched Tables")
    def log_touched_tables(self):
        try:
            # It will either return the touched tables
            # or try to return the previous tables
            return self.tables_to_restore
        except Exception:
            # If both file is not there, assume no tables are touched
            return []

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
        installed_apps = json.loads(self.bench_execute("execute frappe.get_installed_apps")["output"])
        for app in installed_apps:
            if app not in apps_to_keep:
                self.bench_execute(f"remove-from-installed-apps '{app}'")
                self.bench_execute("clear-cache")

    @step("Disable Maintenance Mode")
    def disable_maintenance_mode(self):
        self._disable_maintenance_mode()

    def _disable_maintenance_mode(self):
        return self.bench_execute("set-maintenance-mode off")

    @step("Restore Touched Tables")
    def restore_touched_tables(self):
        return self._restore_touched_tables()

    def _restore_touched_tables(self):
        data = {"restored": {}}
        for table in self.tables_to_restore:
            backup_file = os.path.join(self.backup_directory, f"{table}.sql.gz")
            if os.path.exists(backup_file):
                output = self.execute(
                    "set -o pipefail && "
                    f"gunzip -c '{backup_file}' | "
                    f"mysql -h {self.host} -P {self.db_port} -u {self.user} -p{self.password} "
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
                f"mysql -h {self.host} -P {self.db_port} -u {self.user} -p{self.password} "
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
        return {
            "config": self.config,
            "timezone": self.get_timezone(),
            "usage": self.get_usage(),
        }

    def fetch_site_analytics(self):
        if not os.path.exists(self.analytics_file):
            return {}
        with open(self.analytics_file) as af:
            return json.load(af)

    def sid(self, user="Administrator"):
        code = f"""from frappe.auth import CookieManager, LoginManager
try:
    from frappe.utils import set_request
except ImportError:
    from frappe.tests import set_request

user = '{user}'
set_request(path="/")
frappe.local.cookie_manager = CookieManager()
frappe.local.login_manager = LoginManager()
frappe.local.request_ip = "127.0.0.1"
frappe.local.login_manager.login_as(user)
print(">>>" + frappe.session.sid + "<<<")
"""

        sid = None
        if (output := self.bench_execute("console", input=code)["output"]) and (
            res := re.search(r">>>(.*)<<<", output)
        ):
            sid = res.group(1)
        if (
            (not sid or sid == user or sid == "Guest")
            and (output := self.bench_execute(f"browse --user {user}")["output"])
            and (res := re.search(r"\?sid=([a-z0-9]*)", output))
        ):
            sid = res.group(1)
        return sid

    @property
    def timezone(self):
        query = (
            f"select defvalue from {self.database}.tabDefaultValue where"
            " defkey = 'time_zone' and parent = '__default'"
        )
        try:
            timezone = self.execute(
                f"mysql -h {self.host} -P {self.db_port} -u{self.database} -p{self.password} "
                f'--connect-timeout 3 -sN -e "{query}"'
            )["output"].strip()
        except Exception:
            timezone = ""
        return timezone

    @property
    def tables(self):
        return self.execute(
            "mysql --disable-column-names -B -e 'SHOW TABLES' "
            f"-h {self.host} -P {self.db_port} -u {self.user} -p{self.password} {self.database}"
        )["output"].split("\n")

    @property
    def touched_tables(self):
        with open(self.touched_tables_file, "r") as f:
            return json.load(f)

    @property
    def previous_tables(self):
        with open(self.previous_tables_file, "r") as f:
            return json.load(f)

    @property
    def tables_to_restore(self):
        try:
            return self.touched_tables
        except Exception:
            return self.previous_tables

    @job("Backup Site", priority="low")
    def backup_job(
        self, with_files=False, offsite=None, keep_files_locally_after_offsite_backup: bool = False
    ):
        backup_files = self.backup(with_files)
        uploaded_files = (
            self.upload_offsite_backup(backup_files, offsite, keep_files_locally_after_offsite_backup)
            if (offsite and backup_files)
            else {}
        )
        return {"backups": backup_files, "offsite": uploaded_files}

    @job("Optimize Tables")
    def optimize_tables_job(self, tables: list[str] | None):
        return self.optimize_tables(tables)

    @step("Optimize Tables")
    def optimize_tables(self, tables: list[str] | None = None):
        if not tables:
            tables = [row[0] for row in self.get_database_free_tables()]

        optimized_tables = []
        failed_optimizations = []

        for table in tables:
            query = f"OPTIMIZE TABLE `{table}`"
            try:
                self.execute(
                    f"mysql -sN -h {self.host} -P {self.db_port} "
                    f"-u{self.user} -p{self.password} {self.database} -e '{query}'"
                )
                optimized_tables.append(table)
            except:  # noqa # pylint: disable=bare-except
                failed_optimizations.append(table)
                continue

        if not tables:
            return {"output": "No tables require optimization."}

        message_parts = []

        if optimized_tables:
            message_parts.append(
                f"Successfully optimized {len(optimized_tables)} table(s):\n- "
                + "\n- ".join(optimized_tables)
            )

        if failed_optimizations:
            message_parts.append(
                f"Failed to optimize {len(failed_optimizations)} table(s):\n- "
                + "\n- ".join(failed_optimizations)
            )

        return {"output": "\n\n".join(message_parts)}

    def fetch_latest_backup(self, with_files=True):
        databases, publics, privates, site_configs = [], [], [], []
        backup_directory = os.path.join(self.directory, "private", "backups")

        for file in os.listdir(backup_directory):
            path = os.path.join(backup_directory, file)
            if file.endswith("database.sql.gz") or file.endswith("database-enc.sql.gz"):
                databases.append(path)
            elif file.endswith("private-files.tar") or file.endswith("private-files-enc.tar"):
                privates.append(path)
            elif file.endswith("files.tar") or file.endswith("files-enc.tar"):
                publics.append(path)
            elif file.endswith("site_config_backup.json") or file.endswith("site_config_backup-enc.json"):
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

        return {
            "database": b2mb(self.get_database_size()),
            "database_free_tables": self.get_database_free_tables(),
            "database_free": b2mb(self.get_database_free_size()),
            "public": b2mb(get_size(public_directory)),
            "private": b2mb(get_size(private_directory, ignore_dirs=["backups"])),
            "backups": b2mb(get_size(backup_directory)),
        }

    def get_analytics(self):
        analytics = self.bench_execute("execute frappe.utils.get_site_info")["output"]
        return json.loads(analytics)

    def get_database_size(self):
        try:
            query = f'SELECT size FROM press_meta.schema_sizes WHERE `schema` = "{self.database}"'
            command = f"mysql -sN -h {self.host} -P {self.db_port} \
                -u{self.user} -p{self.password} -e '{query}'"
            database_size = self.execute(command).get("output")
        except Exception:
            # Fallback to old way if press_meta is not available
            try:
                # only specific to mysql/mariaDB. use a different query for postgres.
                # or try using frappe.db.get_database_size if possible
                query = (
                    "SELECT SUM(`data_length` + `index_length` + `data_free`)"
                    " FROM information_schema.tables"
                    f' WHERE `table_schema` = "{self.database}"'
                    " GROUP BY `table_schema`"
                )
                command = f"mysql -sN -h {self.host} -P {self.db_port} \
                    -u{self.user} -p{self.password} -e '{query}'"
                database_size = self.execute(command).get("output")
            except Exception as e:
                raise e
        try:
            assert database_size is not None, "Could not fetch database size"
            return int(database_size)
        except Exception:
            return 0

    def describe_database_table(self, doctype, columns=None):
        if not columns:
            columns = []

        command = f"describe-database-table --doctype '{doctype}' "
        for column in columns:
            command += f"--column {column} "
        try:
            output = self.bench_execute(command)["output"]
            return json.loads(output)
        except Exception:
            return {}

    @property
    def apps(self):
        return self.bench_execute("execute frappe.get_installed_apps")["output"]

    @property
    def apps_as_json(self):
        return json.loads(self.bench_execute("list-apps -f json")["output"])[self.name]

    @job("Add Database Index")
    def add_database_index(self, doctype, columns=None):
        if not columns:
            return
        self._add_database_index(doctype, columns)

    @step("Add Database Index With Bench Command")
    def _add_database_index(self, doctype, columns):
        command = f"add-database-index --doctype '{doctype}' "
        for column in columns:
            command += f"--column {column} "

        return self.bench_execute(command)

    def get_database_free_size(self):
        query = (
            "SELECT SUM(`data_free`)"
            " FROM information_schema.tables"
            f' WHERE `table_schema` = "{self.database}"'
            " GROUP BY `table_schema`"
        )
        command = f"mysql -sN -h {self.host} -P {self.db_port} -u{self.user} -p{self.password} -e '{query}'"
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
                " AND ((`data_free` / (`data_length` + `index_length`)) > 0.2"
                " OR `data_free` > 100 * 1024 * 1024)"
            )
            command = (
                f"mysql -sN -h {self.host} -P {self.db_port} -u{self.user} -p{self.password} -e '{query}'"
            )
            output = self.execute(command).get("output")
            return [line.split("\t") for line in output.splitlines()]
        except Exception:
            return []

    @job("Fetch Database Table Schema")
    def fetch_database_table_schema(self, include_table_size: bool = True, include_index_info: bool = True):
        database = self.db_instance()
        tables = {}
        table_schemas = self._fetch_database_table_schema(database, include_index_info=include_index_info)
        for table_name in table_schemas:
            tables[table_name] = {
                "columns": table_schemas[table_name],
            }

        if include_table_size:
            table_sizes = self._fetch_database_table_sizes(database)
            for table_name in table_sizes:
                if table_name not in tables:
                    continue
                tables[table_name]["size"] = table_sizes[table_name]

        return tables

    @step("Fetch Database Table Schema")
    def _fetch_database_table_schema(self, database: Database, include_index_info: bool = True):
        return database.fetch_database_table_schema(include_index_info=include_index_info)

    @step("Fetch Database Table Sizes")
    def _fetch_database_table_sizes(self, database: Database):
        return database.fetch_database_table_sizes()

    def run_sql_query(self, query: str, commit: bool = False, as_dict: bool = False):
        db = self.db_instance()
        success, output = db.execute_query(query, commit=commit, as_dict=as_dict)
        response = {"success": success, "data": output}
        if not success and hasattr(db, "last_executed_query"):
            response["failed_query"] = db.last_executed_query
        return response

    @job("Analyze Slow Queries")
    def analyze_slow_queries_job(self, queries: list[dict], database_root_password: str) -> list[dict]:
        return self.analyze_slow_queries(queries, database_root_password)

    @step("Analyze Slow Queries")
    def analyze_slow_queries(self, queries: list[dict], database_root_password: str) -> list[dict]:
        from agent.database_optimizer import OptimizeDatabaseQueries

        """
        Args:
            queries (list[dict]): List of queries to analyze
                {
                    "example": "<complete query>",
                    "normalized": "<normalized query>",
                }
        """
        example_queries = [query["example"] for query in queries]
        optimizer = OptimizeDatabaseQueries(self, example_queries, database_root_password)
        analysis = optimizer.analyze()
        analysis_summary = {}  # map[query -> list[index_info_dict]
        for query, indexes in analysis.items():
            analysis_summary[query] = [index.to_dict() for index in indexes]

        result = []  # list[{example, normalized, suggested_indexes}]
        for query in queries:
            query["suggested_indexes"] = analysis_summary.get(query["example"], [])
            result.append(query)
        return {
            "result": result,
        }

    def fetch_summarized_database_performance_report(self, mariadb_root_password: str):
        database = self.db_instance(username="root", password=mariadb_root_password)
        return database.fetch_summarized_performance_report()

    def fetch_database_process_list(self, mariadb_root_password: str):
        return self.db_instance(username="root", password=mariadb_root_password).fetch_process_list()

    def kill_database_process(self, pid: str, mariadb_root_password: str):
        return self.db_instance(username="root", password=mariadb_root_password).kill_process(pid)

    def db_instance(self, username: str | None = None, password: str | None = None) -> Database:
        if not username:
            username = self.user
        if not password:
            password = self.password
        return Database(self.host, self.db_port, username, password, self.database)

    @property
    def job_record(self):
        return self.bench.server.job_record

    @property
    def step_record(self):
        return self.bench.server.step_record

    @step_record.setter
    def step_record(self, value):
        self.bench.server.step_record = value

    def generate_theme_files(self):
        self.bench_execute(
            "execute frappe.website.doctype.website_theme.website_theme.generate_theme_files_if_not_exist"
        )
