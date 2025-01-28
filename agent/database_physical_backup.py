from __future__ import annotations

import os
import subprocess
import time

import peewee
import requests

from agent.database_server import DatabaseServer
from agent.job import job, step
from agent.utils import compute_file_hash, decode_mariadb_filename


class DatabasePhysicalBackup(DatabaseServer):
    def __init__(
        self,
        databases: list[str],
        db_user: str,
        db_password: str,
        site_backup_name: str,
        snapshot_trigger_url: str,
        snapshot_request_key: str,
        db_host: str = "localhost",
        db_port: int = 3306,
        db_base_path: str = "/var/lib/mysql",
    ):
        if not databases:
            raise ValueError("At least one database is required")
        # Instance variable for internal use
        self._db_instances: dict[str, peewee.MySQLDatabase] = {}
        self._db_tables_locked: dict[str, bool] = {db: False for db in databases}

        # variables
        self.site_backup_name = site_backup_name
        self.snapshot_trigger_url = snapshot_trigger_url
        self.snapshot_request_key = snapshot_request_key
        self.databases = databases
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_base_path = db_base_path
        self.db_directories: dict[str, str] = {
            db: os.path.join(self.db_base_path, db) for db in self.databases
        }

        self.innodb_tables: dict[str, list[str]] = {db: [] for db in self.databases}
        self.myisam_tables: dict[str, list[str]] = {db: [] for db in self.databases}
        self.files_metadata: dict[str, dict[str, str]] = {db: {} for db in self.databases}
        self.table_schemas: dict[str, str] = {}

        super().__init__()

    @job("Physical Backup Database", priority="low")
    def backup_job(self):
        self.fetch_table_info()
        self.flush_tables()
        self.flush_changes_to_disk()
        self.validate_exportable_files()
        self.export_table_schemas()
        self.collect_files_metadata()
        self.create_snapshot()  # Blocking call
        self.unlock_all_tables()
        # Return the data [Required for restoring the backup]
        data = {}
        for db_name in self.databases:
            data[db_name] = {
                "innodb_tables": self.innodb_tables[db_name],
                "myisam_tables": self.myisam_tables[db_name],
                "table_schema": self.table_schemas[db_name],
                "files_metadata": self.files_metadata[db_name],
            }
        return data

    @step("Fetch Database Tables Information")
    def fetch_table_info(self):
        """
        Store the table names and their engines in the respective dictionaries
        """
        for db_name in self.databases:
            db_instance = self.get_db(db_name)
            query = (
                "SELECT table_name, ENGINE FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_type != 'VIEW' "
                "ORDER BY table_name"
            )
            data = db_instance.execute_sql(query).fetchall()
            for row in data:
                table = row[0]
                engine = row[1]
                if engine == "InnoDB":
                    self.innodb_tables[db_name].append(table)
                elif engine == "MyISAM":
                    self.myisam_tables[db_name].append(table)

    @step("Flush Database Tables")
    def flush_tables(self):
        for db_name in self.databases:
            """
            InnoDB and MyISAM tables
            Flush the tables and take read lock

            Ref : https://mariadb.com/kb/en/flush-tables-for-export/#:~:text=If%20FLUSH%20TABLES%20...%20FOR%20EXPORT%20is%20in%20effect%20in%20the%20session%2C%20the%20following%20statements%20will%20produce%20an%20error%20if%20attempted%3A

            FLUSH TABLES ... FOR EXPORT
            This will
                - Take READ lock on the tables
                - Flush the tables
                - Will not allow to change table structure (ALTER TABLE, DROP TABLE nothing will work)
            """
            tables = self.innodb_tables[db_name] + self.myisam_tables[db_name]
            tables = [f"`{table}`" for table in tables]
            flush_table_export_query = "FLUSH TABLES {} FOR EXPORT;".format(", ".join(tables))
            self.get_db(db_name).execute_sql(flush_table_export_query)
            self._db_tables_locked[db_name] = True

    @step("Flush Changes to Disk")
    def flush_changes_to_disk(self):
        """
        It's important to flush all the disk buffer of files to disk before snapshot.
        This will ensure that the snapshot is consistent.
        """
        for db_name in self.databases:
            files = os.listdir(self.db_directories[db_name])
            for file in files:
                file_path = os.path.join(self.db_directories[db_name], file)
                """
                Open the file in binary mode and keep buffering disabled(allowed only for binary mode)
                https://docs.python.org/3/library/functions.html#open:~:text=buffering%20is%20an%20optional%20integer

                With this change, we don't need to call f.flush()
                https://docs.python.org/3/library/os.html#os.fsync
                """
                with open(file_path, "rb", buffering=0) as f:
                    os.fsync(f.fileno())

    @step("Validate Exportable Files")
    def validate_exportable_files(self):
        for db_name in self.databases:
            # list all the files in the database directory
            db_files = os.listdir(self.db_directories[db_name])
            db_files = [decode_mariadb_filename(file) for file in db_files]
            """
            InnoDB tables should have the .cfg files to be able to restore it back

            https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#exporting-transportable-tablespaces-for-non-partitioned-tables

            Additionally, ensure .frm files should be present as well.
            If Import tablespace failed for some reason and we need to reconstruct the tables,
            we need .frm files to start the database server.
            """
            for table in self.innodb_tables[db_name]:
                file = table + ".cfg"
                if file not in db_files:
                    raise DatabaseExportFileNotFoundError(f"CFG file for table {table} not found")
                file = table + ".frm"
                if file not in db_files:
                    raise DatabaseExportFileNotFoundError(f"FRM file for table {table} not found")

            """
            MyISAM tables should have .MYD and .MYI files at-least to be able to restore it back
            """
            for table in self.myisam_tables[db_name]:
                table_files = [table + ".MYD", table + ".MYI"]
                for table_file in table_files:
                    if table_file not in db_files:
                        raise DatabaseExportFileNotFoundError(f"MYD or MYI file for table {table} not found")

    @step("Export Table Schema")
    def export_table_schemas(self):
        for db_name in self.databases:
            """
            Export the database schema
            It's important to export the schema only after taking the read lock.
            """
            self.table_schemas[db_name] = self.export_table_schema(db_name)

    @step("Collect Files Metadata")
    def collect_files_metadata(self):
        for db_name in self.databases:
            files = os.listdir(self.db_directories[db_name])
            for file in files:
                file_extension = os.path.splitext(file)[-1].lower()
                if file_extension not in [".cfg", ".frm", ".myd", ".myi", ".ibd"]:
                    continue
                file_path = os.path.join(self.db_directories[db_name], file)
                """
                cfg, frm files are too important to restore/reconstruct the database.
                This files are small also, so we can take the checksum of these files.

                For IBD, MYD files we will just take the size of the file as a validation during restore,
                whether the fsync has happened successfully or not.

                For MariaDB > 10.6.0, it use O_DIRECT as innodb_flush_method,
                So, data will be written directly to the disk without buffering.
                Only the metadata will be updated via fsync.

                https://mariadb.com/kb/en/innodb-system-variables/#innodb_flush_method
                """
                self.files_metadata[db_name][file] = {
                    "size": os.path.getsize(file_path),
                    "checksum": compute_file_hash(file_path, raise_exception=True)
                    if file_extension in [".cfg", ".frm", ".myi"]
                    else None,
                }

    @step("Create Database Snapshot")
    def create_snapshot(self):
        """
        Trigger the snapshot creation
        """
        retries = 0

        while True:
            response = requests.post(
                self.snapshot_trigger_url,
                json={
                    "name": self.site_backup_name,
                    "key": self.snapshot_request_key,
                },
            )
            if response.status_code in [500, 502, 503, 504] and retries <= 10:
                retries += 1
                time.sleep(10)
                continue

            response.raise_for_status()
            break

    @step("Unlock Tables")
    def unlock_all_tables(self):
        for db_name in self.databases:
            self._unlock_tables(db_name)

    def export_table_schema(self, db_name) -> str:
        command = [
            "mariadb-dump",
            "-u",
            self.db_user,
            "-p" + self.db_password,
            "--no-data",
            db_name,
        ]
        try:
            output = subprocess.check_output(command)
        except subprocess.CalledProcessError as e:
            raise DatabaseSchemaExportError(e.output)  # noqa: B904

        return output.decode("utf-8")

    def _unlock_tables(self, db_name):
        self.get_db(db_name).execute_sql("UNLOCK TABLES;")
        self._db_tables_locked[db_name] = False
        """
        Anyway, if the db connection gets closed or db thread dies,
        the tables will be unlocked automatically
        """

    def get_db(self, db_name: str) -> peewee.MySQLDatabase:
        instance = self._db_instances.get(db_name, None)
        if instance is not None:
            if not instance.is_connection_usable():
                raise DatabaseConnectionClosedWithDatabase(
                    f"Database connection closed with database {db_name}"
                )
            return instance
        if db_name not in self.databases:
            raise ValueError(f"Database {db_name} not found")
        self._db_instances[db_name] = peewee.MySQLDatabase(
            db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
        )
        self._db_instances[db_name].connect()
        # Set session wait timeout to 4 hours [EXPERIMENTAL]
        self._db_instances[db_name].execute_sql("SET SESSION wait_timeout = 14400;")
        return self._db_instances[db_name]

    def __del__(self):
        for db_name in self.databases:
            if self._db_tables_locked[db_name]:
                self._unlock_tables(db_name)

        for db_name in self.databases:
            self.get_db(db_name).close()


class DatabaseSchemaExportError(Exception):
    pass


class DatabaseExportFileNotFoundError(Exception):
    pass


class DatabaseConnectionClosedWithDatabase(Exception):
    pass
