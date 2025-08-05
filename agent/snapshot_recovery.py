from __future__ import annotations

import contextlib
import json
import os
import subprocess
from pathlib import Path

from agent.job import job, step
from agent.server import Server


def read_json(file_path: str) -> dict | None:
    """Read a JSON file and return its content as a dictionary."""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


class SnapshotRecovery(Server):
    def __init__(self):
        super().__init__()

    @job("Search Sites in Snapshot")
    def search_sites_in_snapshot(self, sites: list[str]) -> dict[str, dict[str, str]]:
        return self._search_sites(sites)

    @step("Search Sites")
    def _search_sites(self, sites: list[str]) -> dict[str, dict[str, str]]:  # noqa: C901
        benches_directory = Path(self.benches_directory)
        if not benches_directory.exists():
            return {}

        site_info = {}
        for bench in benches_directory.iterdir():
            if not bench.is_dir():
                continue

            sites_directory = bench / "sites"
            if not sites_directory.exists():
                continue

            for site in sites_directory.iterdir():
                if not site.is_dir():
                    continue

                site_name = site.name
                if site_name not in sites:
                    continue

                site_config_path = site / "site_config.json"
                if not site_config_path.exists():
                    continue

                site_config_json = read_json(site_config_path)
                if not site_config_json:
                    continue

                db_name = site_config_json.get("db_name", "")

                site_info[site_name] = {
                    "bench": str(bench),
                    "db_name": db_name,
                }

        return site_info

    @job("Backup Files From Snapshot")
    def backup_files(self, site: str, bench: str, offsite: dict) -> str:
        public_file_name = f"{site.replace('.', '_')}_public_files.tar.gz"
        private_file_name = f"{site.replace('.', '_')}_private_files.tar.gz"
        backup_files = {
            public_file_name: {
                "file": public_file_name,
                "path": os.path.join(self.benches_directory, public_file_name),
                "tar_chdir": os.path.join(self.benches_directory, bench, "sites"),
                "tar_include": f"{site}/public/files",
            },
            private_file_name: {
                "file": private_file_name,
                "path": os.path.join(self.benches_directory, private_file_name),
                "tar_chdir": os.path.join(self.benches_directory, bench, "sites"),
                "tar_include": f"{site}/private/files",
            },
        }
        try:
            self._backup_files(backup_files)
            for file in backup_files.values():
                stat = os.stat(file["path"])
                file["size"] = stat.st_size

            offsite_files = self.upload_backup_files_to_s3(backup_files, offsite)
        finally:
            for backup_file in backup_files.values():
                with contextlib.suppress(Exception):
                    os.remove(backup_file["path"])

        return {
            "backup_files": backup_files,
            "offsite_files": offsite_files,
            "site": site,
            "bench": bench,
        }

    @step("Backup Files")
    def _backup_files(self, files: dict[str]) -> str:
        for file in files.values():
            tar_chdir = file.get("tar_chdir")
            tar_include = file.get("tar_include")
            file_path = file.get("path")

            if not os.path.exists(os.path.join(tar_chdir, tar_include)):
                os.makedirs(os.path.join(tar_chdir, tar_include), exist_ok=True)

            subprocess.run(
                ["tar", "-czf", file_path, "-C", tar_chdir, tar_include],
                check=True,
            )

    @job("Backup Database From Snapshot")
    def backup_db(
        self, site: str, database_ip: str, database_name: str, mariadb_root_password: str, offsite: dict
    ) -> str:
        backup_file = f"{site.replace('.', '_')}.sql.gz"
        backup_file_path = os.path.join(self.benches_directory, backup_file)

        self._backup_db(site, database_ip, database_name, mariadb_root_password, backup_file_path)

        backup_file_size = os.stat(backup_file_path).st_size
        try:
            offsite_files = self.upload_backup_files_to_s3(
                {database_name: {"file": backup_file, "path": backup_file_path}}, offsite
            )
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.remove(backup_file_path)

        return {
            "backup_file_path": backup_file_path,
            "backup_file": backup_file,
            "backup_file_size": backup_file_size,
            "offsite_files": offsite_files,
            "database_name": database_name,
        }

    @step("Backup Database")
    def _backup_db(
        self, site: str, database_ip: str, database_name: str, mariadb_root_password: str, file_path: str
    ) -> str:
        command = [
            "mysqldump",
            "-h",
            database_ip,
            "-u",
            "root",
            f"-p{mariadb_root_password}",
            database_name,
            "|",
            "gzip",
            ">",
            file_path,
        ]

        subprocess.run(" ".join(command), shell=True, check=True)

    @step("Upload Backup Files to S3")
    def upload_backup_files_to_s3(self, backup_files, offsite) -> str:
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
