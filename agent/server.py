from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import tempfile
import time
from contextlib import suppress
from datetime import datetime

from jinja2 import Environment, PackageLoader
from passlib.hash import pbkdf2_sha256 as pbkdf2
from peewee import MySQLDatabase

from agent.application_storage_analyzer import (
    analyze_benches_structure,
    format_size,
    parse_docker_df_output,
    parse_total_disk_usage_output,
    to_bytes,
)
from agent.base import AgentException, Base
from agent.bench import Bench
from agent.exceptions import BenchNotExistsException, RegistryDownException
from agent.job import Job, Step, job, step
from agent.nfs_handler import NFSHandler
from agent.patch_handler import run_patches
from agent.site import Site
from agent.utils import get_supervisor_processes_status, is_registry_healthy


class Server(Base):
    def __init__(self, directory=None):
        super().__init__()

        self.directory = directory or os.getcwd()
        self.set_config_attributes()

        self.job = None
        self.step = None

    def set_config_attributes(self):
        """Setting config attributes here to enable easy config reloads"""
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]
        self.benches_directory = self.config["benches_directory"]
        self.archived_directory = os.path.join(os.path.dirname(self.benches_directory), "archived")
        self.nginx_directory = self.config["nginx_directory"]
        self.hosts_directory = os.path.join(self.nginx_directory, "hosts")
        self.error_pages_directory = os.path.join(self.directory, "repo", "agent", "pages")

    @property
    def press_url(self):
        return self.config.get("press_url", "https://frappecloud.com")

    def docker_login(self, registry):
        url = registry["url"]
        username = registry["username"]
        password = registry["password"]
        return self.execute(f"docker login -u {username} -p {password} {url}")

    def establish_connection_with_registry(self, max_retries: int, registry: dict[str, str]):
        """Given the attempt count try and establish connection with the registry else Raise"""
        for attempt in range(max_retries):
            try:
                if not is_registry_healthy(registry["url"], registry["username"], registry["password"]):
                    raise RegistryDownException("Registry is not available")
                break
            except RegistryDownException as e:
                if attempt == max_retries - 1:
                    raise Exception("Failed to pull image") from e

                time.sleep(60)

    @step("Initialize Bench")
    def bench_init(self, name, config, registry: dict[str, str]):
        self.establish_connection_with_registry(max_retries=3, registry=registry)
        bench_directory = os.path.join(self.benches_directory, name)
        os.mkdir(bench_directory)
        directories = ["logs", "sites", "config"]
        for directory in directories:
            os.mkdir(os.path.join(bench_directory, directory))

        bench_config_file = os.path.join(bench_directory, "config.json")
        with open(bench_config_file, "w") as f:
            json.dump(config, f, indent=1, sort_keys=True)

        config.update({"directory": bench_directory, "name": name})
        docker_compose = os.path.join(bench_directory, "docker-compose.yml")
        self._render_template("bench/docker-compose.yml.jinja2", config, docker_compose)

        config_directory = os.path.join(bench_directory, "config")
        command = (
            "docker run --rm --net none "
            f"-v {config_directory}:/home/frappe/frappe-bench/configmount "
            f"{config['docker_image']} cp -LR config/. configmount"
        )
        self.execute(command, directory=bench_directory)

        sites_directory = os.path.join(bench_directory, "sites")
        # Copy sites directory from image to host system
        command = (
            "docker run --rm --net none "
            f"-v {sites_directory}:/home/frappe/frappe-bench/sitesmount "
            f"{config['docker_image']} cp -LR sites/. sitesmount"
        )
        return self.execute(command, directory=bench_directory)

    def dump(self):
        return {
            "name": self.name,
            "benches": {name: bench.dump() for name, bench in self.benches.items()},
            "config": self.config,
        }

    @job("New Bench", priority="low")
    def new_bench(self, name, bench_config, common_site_config, registry, mounts=None):
        self.docker_login(registry)
        self.bench_init(name, bench_config, registry)
        bench = Bench(name, self, mounts=mounts)
        bench.update_config(common_site_config, bench_config)
        if bench.bench_config.get("single_container"):
            bench.generate_supervisor_config()
        bench.deploy()
        bench.setup_nginx()

    def container_exists(self, name: str, max_retries: int = 3):
        """
        Throw if container exists; after 5 retries with backoff of 5 seconds
        """
        for attempt in range(max_retries):
            try:
                self.execute(f"""docker ps --filter "name=^{name}$" | grep {name}""")
            except AgentException:
                break  # container does not exist
            else:
                if attempt == max_retries - 1:
                    raise Exception("Container exists")
                time.sleep(5)

    def get_image_size(self, image_tag: str):
        try:
            return (
                to_bytes(
                    self.execute(
                        f'docker image ls --format "{{{{.Tag}}}} {{{{.Size}}}}" | grep -E "^{image_tag} "'
                    )["output"].split()[-1]
                )
                / 1024**3
            )
        except AgentException:
            pass

    def unused_image_size(self) -> list[float]:
        """Get the sizes of all the images that are not in use in bytes"""
        images_present = self.execute("docker image ls --format '{{.Repository}}:{{.Tag}} {{.Size}}'")[
            "output"
        ].split("\n")
        images_present = [image.split() for image in images_present]
        images_in_use = self.execute("docker container ls --format {{.Image}}")["output"].split("\n")

        return [to_bytes(size) for image_name, size in images_present if image_name not in images_in_use]

    def get_reclaimable_size(self) -> dict[str, dict[str, float] | float]:
        """Checks archived (bench and site) and unused docker artefacts size"""
        archived_sites_directory = os.path.join(self.benches_directory, "*", "sites", "archived")
        archived_folder_size = self.execute(
            "du -sB1 /home/frappe/archived/ --exclude assets | awk '{print $1}'"
        ).get("output")
        archived_folder_size = float(archived_folder_size)

        try:
            site_archived_folder_size = (
                self.execute(f"du -sB1 {archived_sites_directory} --exclude assets | awk '{{print $1}}'")
                .get("output")
                .split("\n")
            )
            site_archived_folder_size = sum(map(float, site_archived_folder_size))
        except Exception:
            site_archived_folder_size = 0

        unused_images_size = sum(self.unused_image_size())
        total_archived_folder_size = archived_folder_size + site_archived_folder_size

        formatted_archived_folder_size = f"{round(total_archived_folder_size / 1024**3, 2)}GB"
        formatted_unused_image_size = format_size(unused_images_size)

        return {
            "archived": formatted_archived_folder_size,
            "images": formatted_unused_image_size,
            "total": round((unused_images_size + total_archived_folder_size) / 1024**3, 2),
        }

    def _check_site_on_bench(self, bench_name: str):
        """Check if sites are present on the benches"""
        sites_directory = f"/home/frappe/benches/{bench_name}/sites"
        for possible_site in os.listdir(sites_directory):
            if os.path.exists(os.path.join(sites_directory, possible_site, "site_config.json")):
                raise Exception(f"Bench {bench_name} has sites")

    def disable_production_on_bench(self, name: str):
        """In case of corrupted bench / site config don't stall archive"""
        self._check_site_on_bench(name)
        self.execute(f"docker rm {name} --force")

    @job("Run Benches on Shared FS")
    def change_bench_directory(
        self,
        directory: str,
        is_primary: bool,
        secondary_server_private_ip: str,
        redis_connection_string_ip: str | None = None,
        restart_benches: bool = True,
        registry_settings: dict | None = None,
    ):
        self._change_bench_directory(directory)
        self.set_config_attributes()
        self.update_agent_nginx_config()
        self.update_bench_nginx_config()
        self._reload_nginx()

        if redis_connection_string_ip:
            self._configure_site_with_redis_private_ip(redis_connection_string_ip)

        if restart_benches:
            # We will only start with secondary server private IP if this is a secondary server
            self.restart_benches(
                is_primary=is_primary,
                registry_settings=registry_settings,
                secondary_server_private_ip=secondary_server_private_ip if not is_primary else None,
            )

    @step("Configure Site with Redis Private IP")
    def _configure_site_with_redis_private_ip(self, private_ip: str):
        for _, bench in self.benches.items():
            common_site_config = bench.get_config(for_update=True)

            for key in ("redis_cache", "redis_queue", "redis_socketio"):
                if private_ip != "localhost":
                    port = (
                        bench.bench_config["rq_port"]
                        if key == "redis_queue"
                        else bench.bench_config["rq_cache_port"]
                    )
                else:
                    port = 11000 if key == "redis_queue" else 13000

                updated_connection_string = f"redis://{private_ip}:{port}"
                common_site_config.update({key: updated_connection_string})

            bench.set_config(common_site_config)

    @step("Change Bench Directory")
    def _change_bench_directory(self, directory: str):
        self.update_config({"benches_directory": directory})

    @step("Update Agent Nginx Conf File")
    def update_agent_nginx_config(self):
        self._generate_nginx_config()

    @step("Update Bench Nginx Conf File")
    def update_bench_nginx_config(self):
        from filelock import FileLock

        for _, bench in self.benches.items():
            with FileLock(os.path.join(bench.directory, "nginx.config.lock")):
                # Don't want to use setup_nginx as it reloads everytime
                bench.generate_nginx_config()

    @step("Restart Benches")
    def restart_benches(
        self, is_primary: bool, secondary_server_private_ip: str, registry_settings: dict[str, str]
    ):
        if not is_primary:
            # Don't need to pull images on primary server
            self.docker_login(registry_settings)
        for _, bench in self.benches.items():
            bench.start(secondary_server_private_ip=secondary_server_private_ip)

    @job("Stop Bench Workers")
    def stop_bench_workers(self):
        self._stop_bench_workers()

    @step("Stop Bench Workers")
    def _stop_bench_workers(self):
        """Stop all workers except redis"""
        for _, bench in self.benches.items():
            bench.docker_execute("supervisorctl stop frappe-bench-web: frappe-bench-workers:", as_root=True)

    @job("Start Bench Workers")
    def start_bench_workers(self):
        self._start_bench_workers()

    @step("Start Bench Workers")
    def _start_bench_workers(self):
        """Start all workers"""
        for _, bench in self.benches.items():
            bench.docker_execute("supervisorctl start frappe-bench-web: frappe-bench-workers:", as_root=True)

    @job("Force Remove All Benches")
    def force_remove_all_benches(self):
        self._force_remove_all_benches()

    @step("Force Remove All Benches")
    def _force_remove_all_benches(self):
        for _, bench in self.benches.items():
            bench.execute(f"docker rm {bench.name} --force")

    @job("Archive Bench", priority="low")
    def archive_bench(self, name):
        bench_directory = os.path.join(self.benches_directory, name)
        if not os.path.exists(bench_directory):
            return
        try:
            bench = Bench(name, self)
        except json.JSONDecodeError:
            self.disable_production_on_bench(name)
        except FileNotFoundError as e:
            if not e.filename.endswith("common_site_config.json"):
                raise
        else:
            if bench.sites:
                raise Exception(f"Bench has sites: {bench.sites}")
            bench.disable_production()

        self.container_exists(name)
        self.move_bench_to_archived_directory(name)

    @job("Cleanup Unused Files", priority="low")
    def cleanup_unused_files(self, force: bool = False):
        self.remove_archived_benches(force)
        self.remove_temporary_files(force)
        self.remove_unused_docker_artefacts()
        if force:
            self.remove_archived_sites()

    def remove_benches_without_container(self, benches: list[str]):
        for bench in benches:
            try:
                self.execute(f"docker ps -a | grep {bench}")
            except AgentException as e:
                if e.data.returncode:
                    self.move_to_archived_directory(Bench(bench, self))

    @step("Remove Archived Sites")
    def remove_archived_sites(self):
        for bench in self.benches:
            archived_sites_path = os.path.join(self.benches_directory, bench, "sites", "archived")
            if os.path.exists(archived_sites_path) and os.path.isdir(archived_sites_path):
                shutil.rmtree(archived_sites_path)

    @step("Remove Archived Benches")
    def remove_archived_benches(self, force: bool = False):
        now = datetime.now().timestamp()
        removed = []
        if os.path.exists(self.archived_directory):
            for bench in os.listdir(self.archived_directory):
                bench_path = os.path.join(self.archived_directory, bench)
                if force or (now - os.stat(bench_path).st_mtime > 86400):
                    removed.append(
                        {
                            "bench": bench,
                            "size": self._get_tree_size(bench_path),
                        }
                    )
                    if os.path.isfile(bench_path):
                        os.remove(bench_path)
                    elif os.path.isdir(bench_path):
                        shutil.rmtree(bench_path)
        return {"benches": removed[:100]}

    @step("Remove Temporary Files")
    def remove_temporary_files(self, force: bool = False):
        temp_directory = tempfile.gettempdir()
        now = datetime.now().timestamp()
        removed = []
        patterns = ["frappe-pdf", "snyk-patch", "yarn-", "agent-upload"]
        if os.path.exists(temp_directory):
            for file in os.listdir(temp_directory):
                if not list(filter(lambda x: x in file, patterns)):
                    continue
                file_path = os.path.join(temp_directory, file)
                if force or (now - os.stat(file_path).st_mtime > 7200):
                    removed.append({"file": file, "size": self._get_tree_size(file_path)})
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
        return {"files": removed[:100]}

    @step("Remove Unused Docker Artefacts")
    def remove_unused_docker_artefacts(self):
        before = self.execute("docker system df -v")["output"].split("\n")
        prune = self.execute("docker system prune -af")["output"].split("\n")
        after = self.execute("docker system df -v")["output"].split("\n")
        return {
            "before": before,
            "prune": prune,
            "after": after,
        }

    @step("Move Bench to Archived Directory")
    def move_bench_to_archived_directory(self, bench_name):
        if not os.path.exists(self.archived_directory):
            os.mkdir(self.archived_directory)
        target = os.path.join(self.archived_directory, bench_name)
        if os.path.exists(target):
            shutil.rmtree(target)
        bench_directory = os.path.join(self.benches_directory, bench_name)
        assets_directory = os.path.join(bench_directory, "sites", "assets")

        # Dropping assets we don't restore that anyways
        if os.path.exists(assets_directory):
            shutil.rmtree(assets_directory)

        self.execute(f"mv {bench_directory} {self.archived_directory}")

    @job("Update Site Pull", priority="low")
    def update_site_pull_job(self, name, source, target, activate):
        source = Bench(source, self)
        site = Site(name, source)

        site.enable_maintenance_mode()
        site.wait_till_ready()

        target = Bench(target, self)
        self.move_site(site, target)
        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)
        with suppress(Exception):
            site.generate_theme_files()

        if activate:
            site.disable_maintenance_mode()

    @job("Update Site Migrate", priority="low")
    def update_site_migrate_job(
        self,
        name,
        source,
        target,
        activate,
        skip_failing_patches,
        skip_backups,
        before_migrate_scripts: dict[str, str] | None = None,
        skip_search_index: bool = True,
    ):
        if before_migrate_scripts is None:
            before_migrate_scripts = {}

        source = Bench(source, self)
        site = Site(name, source)

        site.enable_maintenance_mode()
        site.wait_till_ready()

        if not skip_backups:
            site.clear_backup_directory()
            site.tablewise_backup()

        target = Bench(target, self)
        self.move_site(site, target)

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)

        if before_migrate_scripts:
            site.run_app_scripts(before_migrate_scripts)

        try:
            site.migrate(
                skip_search_index=skip_search_index,
                skip_failing_patches=skip_failing_patches,
            )
        finally:
            site.log_touched_tables()

        with suppress(Exception):
            site.bench_execute(
                "execute frappe.website.doctype.website_theme.website_theme.generate_theme_files_if_not_exist"
            )

        if activate:
            site.disable_maintenance_mode()

        with suppress(Exception):
            # Don't fail job on failure
            # v12 does not have build_search_index command
            site.build_search_index()

    @job("Deactivate Site", priority="high")
    def deactivate_site_job(self, name, bench):
        source = Bench(bench, self)
        site = Site(name, source)

        site.enable_maintenance_mode()
        site.wait_till_ready()

    @job("Activate Site", priority="high")
    def activate_site_job(self, name, bench):
        source = Bench(bench, self)
        site = Site(name, source)

        site.disable_maintenance_mode()
        with suppress(Exception):
            # Don't fail job on failure
            # v12 does not have build_search_index command
            site.build_search_index()

    @job("Recover Failed Site Migrate", priority="high")
    def update_site_recover_migrate_job(
        self, name, source, target, activate, rollback_scripts, restore_touched_tables
    ):
        source = Bench(source, self)
        target = Bench(target, self)

        site = Site(name, source)
        self.move_site(site, target)

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)
        if restore_touched_tables:
            site.restore_touched_tables()

        if rollback_scripts:
            site.run_app_scripts(rollback_scripts)

        if activate:
            site.disable_maintenance_mode()

    @job("Recover Failed Site Pull", priority="high")
    def update_site_recover_pull_job(self, name, source, target, activate):
        source = Bench(source, self)
        target = Bench(target, self)

        site = Site(name, source)
        self.move_site(site, target)

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)

        if activate:
            site.disable_maintenance_mode()

    @job("Move Site to Bench")
    def move_site_to_bench(self, name, source, target, deactivate, activate, skip_failing_patches):
        # Dangerous method (no backup),
        # use update_site_migrate if you don't know what you're doing
        source = Bench(source, self)
        site = Site(name, source)

        if deactivate:  # cases when python is broken in bench
            site.enable_maintenance_mode()
            site.wait_till_ready()

        target = Bench(target, self)
        self.move_site(site, target)

        source.setup_nginx()
        target.setup_nginx_target()
        self.reload_nginx()

        site = Site(name, target)

        site.migrate(skip_failing_patches=skip_failing_patches)

        with suppress(Exception):
            site.bench_execute(
                "execute frappe.website.doctype.website_theme.website_theme.generate_theme_files_if_not_exist"
            )

        if activate:
            site.disable_maintenance_mode()

    @job("Recover Failed Site Update", priority="high")
    def update_site_recover_job(self, name, bench):
        site = self.benches[bench].sites[name]
        site.disable_maintenance_mode()

    @step("Move Site")
    def move_site(self, site, target):
        destination = os.path.join(target.sites_directory, site.name)
        destination_site_config = os.path.join(destination, "site_config.json")
        if os.path.exists(destination) and not os.path.exists(destination_site_config):
            # If there's already a site directory in the destination bench
            # and it does not have a site_config.json file,
            # then it is an incomplete site directory.
            # Move it to the sites/archived directory
            archived_sites_directory = os.path.join(target.sites_directory, "archived")
            os.makedirs(archived_sites_directory, exist_ok=True)
            archived_site_path = os.path.join(
                archived_sites_directory,
                f"{site.name}-{datetime.now().isoformat()}",
            )
            shutil.move(destination, archived_site_path)
        shutil.move(site.directory, target.sites_directory)

    def execute(self, command, directory=None, skip_output_log=False, non_zero_throw=True):
        return super().execute(
            command, directory=directory, skip_output_log=skip_output_log, non_zero_throw=non_zero_throw
        )

    @job("Pull Docker Images", priority="high")
    def pull_docker_images(self, image_tags: list[str], registry: dict[str, str]) -> None:
        self._pull_docker_images(image_tags, registry)

    @step("Pull Docker Images")
    def _pull_docker_images(self, image_tags: list[str], registry: dict[str, str]) -> None:
        self.docker_login(registry)

        for image_tag in image_tags:
            command = f"docker pull {image_tag}"
            self.execute(command, directory=self.directory)

    @job("Reload NGINX")
    def restart_nginx(self):
        return self.reload_nginx()

    @step("Reload NGINX")
    def reload_nginx(self):
        return self._reload_nginx()

    @step("Update Supervisor")
    def update_supervisor(self):
        return self._update_supervisor()

    @job("Update Database Host", priority="high")
    def update_database_host_job(self, db_host: str):
        self.update_database_host_step(db_host)

    @step("Update Database Host")
    def update_database_host_step(self, db_host: str):
        for b in self.benches.values():
            b._update_database_host(db_host)

    def setup_authentication(self, password):
        self.update_config({"access_token": pbkdf2.hash(password)})

    def setup_proxysql(self, password):
        self.update_config({"proxysql_admin_password": password})

    @job("Add Servers to ACL")
    def add_to_acl(
        self,
        primary_server_private_ip: str,
        secondary_server_private_ip: str,
        shared_directory: bool,
    ) -> None:
        return self._add_to_acl(
            primary_server_private_ip,
            secondary_server_private_ip,
            shared_directory,
        )

    @step("Add Servers to ACL")
    def _add_to_acl(
        self,
        primary_server_private_ip: str,
        secondary_server_private_ip: str,
        shared_directory: str,
    ):
        nfs_handler = NFSHandler(self)
        return nfs_handler.add_to_acl(
            primary_server_private_ip=primary_server_private_ip,
            secondary_server_private_ip=secondary_server_private_ip,
            shared_directory=shared_directory,
        )

    @job("Remove Server from ACL")
    def remove_from_acl(
        self, shared_directory: str, primary_server_private_ip: str, secondary_server_private_ip: str
    ) -> None:
        return self._remove_from_acl(shared_directory, primary_server_private_ip, secondary_server_private_ip)

    @step("Remove Server from ACL")
    def _remove_from_acl(
        self, shared_directory: str, primary_server_private_ip: str, secondary_server_private_ip: str
    ):
        nfs_handler = NFSHandler(self)
        return nfs_handler.remove_from_acl(
            shared_directory=shared_directory,
            primary_server_private_ip=primary_server_private_ip,
            secondary_server_private_ip=secondary_server_private_ip,
        )

    def update_config(self, value):
        config = self.get_config(for_update=True)
        config.update(value)
        self.set_config(config, indent=4)

    def setup_registry(self):
        self.update_config({"registry": True})
        self.setup_nginx()

    def setup_log(self):
        self.update_config({"log": True})
        self.setup_nginx()

    def setup_analytics(self):
        self.update_config({"analytics": True})
        self.setup_nginx()

    def setup_trace(self):
        self.update_config({"trace": True})
        self.setup_nginx()

    def setup_sentry(self, sentry_dsn):
        self.update_config({"sentry_dsn": sentry_dsn})
        self.setup_supervisor()

    def setup_nginx(self):
        self._generate_nginx_config()
        self._generate_agent_nginx_config()
        self._reload_nginx()

    def setup_supervisor(self):
        self._generate_redis_config()
        self._generate_supervisor_config()
        self._update_supervisor()

    def start_all_benches(self):
        for bench in self.benches.values():
            with suppress(Exception):
                bench.start()

    def stop_all_benches(self):
        for bench in self.benches.values():
            with suppress(Exception):
                bench.stop()

    @property
    def benches(self) -> dict[str, Bench]:
        benches = {}
        for directory in os.listdir(self.benches_directory):
            with suppress(Exception):
                benches[directory] = Bench(directory, self)
        return benches

    def get_bench(self, bench):
        try:
            return self.benches[bench]
        except KeyError as exc:
            raise BenchNotExistsException(bench) from exc

    @property
    def job_record(self):
        if self.job is None:
            self.job = Job()
        return self.job

    @property
    def step_record(self):
        if self.step is None:
            self.step = Step()
        return self.step

    @step_record.setter
    def step_record(self, value):
        self.step = value

    def update_agent_web(self, url=None, branch="master"):
        directory = os.path.join(self.directory, "repo")
        self.execute("git reset --hard", directory=directory)
        self.execute("git clean -fd", directory=directory)
        if url:
            self.execute(f"git remote set-url upstream {url}", directory=directory)
        self.execute("git fetch upstream", directory=directory)
        self.execute(f"git checkout {branch}", directory=directory)
        self.execute(f"git merge --ff-only upstream/{branch}", directory=directory)
        self.execute("./env/bin/pip install -e repo", directory=self.directory)

        self._generate_redis_config()
        self._generate_supervisor_config()
        self.execute("sudo supervisorctl reread")
        self.execute("sudo supervisorctl restart agent:redis")

        self.setup_nginx()
        for worker in range(self.config["workers"]):
            worker_name = f"agent:worker-{worker}"
            self.execute(f"sudo supervisorctl restart {worker_name}")

        self.execute("sudo supervisorctl restart agent:web")
        run_patches()

    def update_agent_cli(  # noqa: C901
        self,
        restart_redis=True,
        restart_rq_workers=True,
        restart_web_workers=True,
        skip_repo_setup=False,
        skip_patches=False,
    ):
        directory = os.path.join(self.directory, "repo")
        if skip_repo_setup:
            self.execute("git reset --hard", directory=directory)
            self.execute("git clean -fd", directory=directory)
            self.execute("git fetch upstream", directory=directory)
            self.execute("git merge --ff-only upstream/master", directory=directory)
            self.execute("./env/bin/pip install -e repo", directory=self.directory)

        supervisor_status = get_supervisor_processes_status()

        # Stop web service
        if restart_web_workers and supervisor_status.get("web") == "RUNNING":
            self.execute("sudo supervisorctl stop agent:web", non_zero_throw=False)

        # Stop required services
        if restart_rq_workers:
            for worker_id in supervisor_status.get("worker", {}):
                self.execute(f"sudo supervisorctl stop agent:worker-{worker_id}", non_zero_throw=False)

        # Stop NGINX Reload Manager if it's a proxy server
        is_proxy_server = self.config.get("domain") and self.config.get("name").startswith("n")
        if is_proxy_server:
            self.execute("sudo supervisorctl stop agent:nginx_reload_manager", non_zero_throw=False)

        # Stop redis
        if restart_redis and supervisor_status.get("redis") == "RUNNING":
            self.execute("sudo supervisorctl stop agent:redis", non_zero_throw=False)

        self.setup_supervisor()

        # Start back services in same order
        supervisor_status = get_supervisor_processes_status()
        if restart_redis or supervisor_status.get("redis") != "RUNNING":
            self.execute("sudo supervisorctl start agent:redis")

        # Start NGINX Reload Manager if it's a proxy server
        if is_proxy_server:
            self.execute("sudo supervisorctl start agent:nginx_reload_manager")

        if restart_rq_workers:
            for i in range(self.config["workers"]):
                self.execute(f"sudo supervisorctl start agent:worker-{i}")

        if restart_web_workers:
            self.execute("sudo supervisorctl start agent:web")

        self.setup_nginx()

        if not skip_patches:
            run_patches()

    @staticmethod
    def run_ncdu_command(path: str, excludes: list | None = None) -> str | None:
        cmd = ["ncdu", path, "-x", "-o", "/dev/stdout"]
        if excludes:
            for item in excludes:
                cmd.extend(["--exclude", item])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
            return result.stdout if result.returncode == 0 else None
        except subprocess.TimeoutExpired:
            return None

    def get_storage_breakdown(self) -> dict:
        app_storage_analysis = {}
        failed_message = "Failed to analyze storage"

        benches_output = self.run_ncdu_command(
            "/home/frappe/benches/", excludes=["node_modules", "env", "assets"]
        )
        docker_output = self.execute("docker system df --format '{{.Size}}'").get("output")
        total_output = self.execute(
            f"df -B1 {'/opt/volumes/benches' if os.path.exists('/opt/volumes/benches') else '/'}"
        ).get("output")

        if total_output:
            total_data = parse_total_disk_usage_output(total_output)
            app_storage_analysis["total"] = total_data

        if benches_output:
            benches_data = analyze_benches_structure(benches_output)
            if benches_data:
                app_storage_analysis["benches"] = benches_data

        if docker_output:
            docker_data = parse_docker_df_output(docker_output)
            app_storage_analysis["docker"] = docker_data

        return app_storage_analysis or {"error": failed_message}

    def get_agent_version(self):
        directory = os.path.join(self.directory, "repo")
        return {
            "commit": self.execute("git rev-parse HEAD", directory=directory)["output"],
            "status": self.execute("git status --short", directory=directory)["output"],
            "upstream": self.execute("git remote get-url upstream", directory=directory)["output"],
            "show": self.execute("git show", directory=directory)["output"],
            "python": platform.python_version(),
            "services": get_supervisor_processes_status(),
        }

    def status(self, mariadb_root_password):
        return {
            "mariadb": self.mariadb_processlist(mariadb_root_password=mariadb_root_password),
            "supervisor": self.supervisor_status(),
            "nginx": self.nginx_status(),
            "stats": self.stats(),
            "processes": self.processes(),
            "timestamp": str(datetime.now()),
        }

    def _memory_stats(self):
        free = self.execute("free -t -m")["output"].split("\n")
        memory = {}
        headers = free[0].split()
        for line in free[1:]:
            type, line = line.split(None, 1)
            memory[type.lower()[:-1]] = dict(zip(headers, list(map(int, line.split()))))
        return memory

    def _cpu_stats(self):
        prev_proc = self.execute("cat /proc/stat")["output"].split("\n")
        time.sleep(0.5)
        now_proc = self.execute("cat /proc/stat")["output"].split("\n")

        # 0   user            Time spent in user mode.
        # 1   nice            Time spent in user mode with low priority
        # 2   system          Time spent in system mode.
        # 3   idle            Time spent in the idle task.
        # 4   iowait          Time waiting for I/O to complete.  This
        # 5   irq             Time servicing interrupts.
        # 6   softirq         Time servicing softirqs.
        # 7   steal           Stolen time
        # 8   guest           Time spent running a virtual CPU for guest OS
        # 9   guest_nice      Time spent running a niced guest

        # IDLE = idle + iowait
        # NONIDLE = user + nice + system + irq + softirq + steal + guest
        #           + guest_nice
        # TOTAL = IDLE + NONIDLE
        # USAGE = TOTAL - IDLE / TOTAL
        cpu = {}
        for prev, now in zip(prev_proc, now_proc):
            if prev.startswith("cpu"):
                type = prev.split()[0]
                prev = list(map(int, prev.split()[1:]))
                now = list(map(int, now.split()[1:]))

                idle = (now[3] + now[4]) - (prev[3] + prev[4])
                total = sum(now) - sum(prev)
                cpu[type] = int(1000 * (total - idle) / total) / 10
        return cpu

    def stats(self):
        load_average = os.getloadavg()
        return {
            "cpu": {
                "usage": self._cpu_stats(),
                "count": os.cpu_count(),
                "load_average": {
                    1: load_average[0],
                    5: load_average[1],
                    15: load_average[2],
                },
            },
            "memory": self._memory_stats(),
        }

    def processes(self):
        processes = []
        try:
            output = self.execute("ps --pid 2 --ppid 2 --deselect u")["output"].split("\n")
            headers = list(filter(None, output[0].split()))
            rows = map(lambda s: s.strip().split(None, len(headers) - 1), output[1:])
            processes = [dict(zip(headers, row)) for row in rows]
        except Exception:
            import traceback

            traceback.print_exc()
        return processes

    def mariadb_processlist(self, mariadb_root_password):
        processes = []
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host="localhost",
                port=3306,
            )
            cursor = mariadb.execute_sql("SHOW PROCESSLIST")
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            processes = list(map(lambda x: dict(zip(columns, x)), rows))
        except Exception:
            import traceback

            traceback.print_exc()
        return processes

    def supervisor_status(self, name="all"):
        status = []
        try:
            try:
                supervisor = self.execute(f"sudo supervisorctl status {name}")
            except AgentException as e:
                supervisor = e.data

            for process in supervisor["output"].split("\n"):
                name, description = process.split(None, 1)

                name, *group = name.strip().split(":")
                group = group[0] if group else ""

                state, *description = description.strip().split(None, 1)
                state = state.strip()
                description = description[0].strip() if description else ""

                status.append(
                    {
                        "name": name,
                        "group": group,
                        "state": state,
                        "description": description,
                        "online": state == "RUNNING",
                    }
                )
        except Exception:
            import traceback

            traceback.print_exc()
        return status

    def nginx_status(self):
        try:
            systemd = self.execute("sudo systemctl status nginx")
        except AgentException as e:
            systemd = e.data
        return systemd["output"]

    def _generate_nginx_config(self):
        nginx_config = os.path.join(self.nginx_directory, "nginx.conf")
        self._render_template(
            "nginx/nginx.conf.jinja2",
            {
                "proxy_ip": self.config.get("proxy_ip"),
                "tls_protocols": self.config.get("tls_protocols"),
                "nginx_vts_module_enabled": self.config.get("nginx_vts_module_enabled", True),
                "ip_whitelist": self.config.get("ip_whitelist", []),
                "use_shared": self.config.get("benches_directory") == "/shared",
            },
            nginx_config,
        )

    def _generate_agent_nginx_config(self):
        agent_nginx_config = os.path.join(self.directory, "nginx.conf")
        self._render_template(
            "agent/nginx.conf.jinja2",
            {
                "web_port": self.config["web_port"],
                "name": self.name,
                "registry": self.config.get("registry", False),
                "monitor": self.config.get("monitor", False),
                "log": self.config.get("log", False),
                "analytics": self.config.get("analytics", False),
                "trace": self.config.get("trace", False),
                "tls_directory": self.config["tls_directory"],
                "nginx_directory": self.nginx_directory,
                "nginx_vts_module_enabled": self.config.get("nginx_vts_module_enabled", True),
                "pages_directory": os.path.join(self.directory, "repo", "agent", "pages"),
                "tls_protocols": self.config.get("tls_protocols"),
                "press_url": self.config.get("press_url"),
            },
            agent_nginx_config,
        )

    def _generate_redis_config(self):
        redis_config = os.path.join(self.directory, "redis.conf")
        self._render_template(
            "agent/redis.conf.jinja2",
            {"redis_port": self.config["redis_port"]},
            redis_config,
        )

    def _generate_supervisor_config(self):
        supervisor_config = os.path.join(self.directory, "supervisor.conf")
        data = {
            "web_port": self.config["web_port"],
            "redis_port": self.config["redis_port"],
            "gunicorn_workers": self.config.get("gunicorn_workers", 2),
            "workers": self.config["workers"],
            "directory": self.directory,
            "user": self.config["user"],
            "sentry_dsn": self.config.get("sentry_dsn"),
            "is_standalone": self.config.get("standalone", False),
        }
        if self.config.get("name").startswith("n"):
            data["is_proxy_server"] = True

        self._render_template(
            "agent/supervisor.conf.jinja2",
            data,
            supervisor_config,
        )

    def _reload_nginx(self):
        try:
            return self.execute("sudo systemctl reload nginx")
        except AgentException as e:
            try:
                self.execute("sudo nginx -t")
            except AgentException as e2:
                raise e2 from e
            else:
                raise e

    def _render_template(self, template, context, outfile, options=None):
        if options is None:
            options = {}
        options.update({"loader": PackageLoader("agent", "templates")})
        environment = Environment(**options)
        template = environment.get_template(template)

        with open(outfile, "w") as f:
            f.write(template.render(**context))

    def _update_supervisor(self):
        self.execute("sudo supervisorctl reread")
        self.execute("sudo supervisorctl update")

    def _get_tree_size(self, path):
        return self.execute(f"du -sh {path}")["output"].split()[0]

    def long_method(
        self,
    ):
        return self.execute("du -h -d 1 /home/aditya/Frappe")["output"]

    @job("Long")
    def long_step(
        self,
    ):
        return self.long_method()

    @job("Long")
    def long_job(
        self,
    ):
        return self.long_step()

    @job("Ping Job")
    def ping_job(self):
        return self.ping_step()

    @step("Ping Step")
    def ping_step(self):
        return {"message": "pong"}

    @property
    def wildcards(self) -> list[str]:
        wildcards = []
        for host in os.listdir(self.hosts_directory):
            if "*" in host:
                wildcards.append(host.strip("*."))
        return wildcards
