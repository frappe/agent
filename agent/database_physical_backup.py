from __future__ import annotations

import contextlib
import json
import os
import subprocess
import time
from random import randint

import requests

from agent.database import CustomPeeweeDB
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
        self._db_instances: dict[str, CustomPeeweeDB] = {}
        self._db_instances_connection_id: dict[str, int] = {}
        self._db_tables_locked: dict[str, bool] = {db: False for db in databases}

        # variables
        self.site_backup_name = site_backup_name
        self.snapshot_trigger_url = snapshot_trigger_url
        self.snapshot_request_key = snapshot_request_key
        self.databases = databases
        self._db_user = db_user
        self._db_password = db_password
        self._db_host = db_host
        self._db_port = db_port
        self._db_base_path = db_base_path
        self._db_directories: dict[str, str] = {
            db: os.path.join(self._db_base_path, db) for db in self.databases
        }

        self.innodb_tables: dict[str, list[str]] = {db: [] for db in self.databases}
        self.myisam_tables: dict[str, list[str]] = {db: [] for db in self.databases}
        self.sequence_tables: dict[str, list[str]] = {db: [] for db in self.databases}
        self.files_metadata: dict[str, dict[str, dict[str, str]]] = {db: {} for db in self.databases}
        self.table_schemas: dict[str, str] = {}

        super().__init__()

    @job("Physical Backup Database", priority="low")
    def create_backup_job(self):
        self.remove_backups_metadata()
        self.fetch_table_info()
        self.flush_tables()
        self.flush_changes_to_disk()
        self.validate_exportable_files()
        self.export_table_schemas()
        self.collect_files_metadata()
        self.store_backup_metadata()
        self.create_snapshot()  # Blocking call
        self.unlock_all_tables()
        self.remove_backups_metadata()

    def remove_backups_metadata(self):
        with contextlib.suppress(Exception):
            for db_name in self.databases:
                os.remove(get_path_of_physical_backup_metadata(self._db_base_path, db_name))

    @step("Fetch Database Tables Information")
    def fetch_table_info(self):
        """
        Store the table names and their engines in the respective dictionaries
        """
        for db_name in self.databases:
            db_instance = self.get_db(db_name)
            query = (
                "SELECT table_name, engine, table_type FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_type != 'VIEW' "
                "ORDER BY table_name"
            )
            data = db_instance.execute_sql(query).fetchall()
            for row in data:
                table = row[0]
                engine = row[1]
                table_type = row[2]
                if engine == "InnoDB":
                    self.innodb_tables[db_name].append(table)
                elif engine == "MyISAM":
                    self.myisam_tables[db_name].append(table)

                if table_type == "SEQUENCE":
                    """
                    Sequence table can use any engine

                    In mariadb-dump result, sequence table will have specific SQL for creating/dropping
                    https://mariadb.com/kb/en/create-sequence/
                    https://mariadb.com/kb/en/drop-sequence/

                    Store the table references, so that we can handle this in a different manner
                    during physical restore
                    """
                    self.sequence_tables[db_name].append(table)

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
            self._kill_other_db_connections(db_name)
            self.get_db(db_name).execute_sql(flush_table_export_query)
            self._db_tables_locked[db_name] = True

    @step("Flush Changes to Disk")
    def flush_changes_to_disk(self):
        """
        It's important to flush all the disk buffer of files to disk before snapshot.
        This will ensure that the snapshot is consistent.
        """
        for db_name in self.databases:
            files = os.listdir(self._db_directories[db_name])
            for file in files:
                file_path = os.path.join(self._db_directories[db_name], file)
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
            db_files = os.listdir(self._db_directories[db_name])
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
            files = os.listdir(self._db_directories[db_name])
            for file in files:
                file_extension = os.path.splitext(file)[-1].lower()
                if file_extension not in [".cfg", ".frm", ".myd", ".myi", ".ibd"]:
                    continue
                file_path = os.path.join(self._db_directories[db_name], file)
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

    @step("Store Backup Metadata")
    def store_backup_metadata(self):
        """
        Store the backup metadata in the database
        """
        data = {}
        for db_name in self.databases:
            data = {
                "innodb_tables": self.innodb_tables[db_name],
                "myisam_tables": self.myisam_tables[db_name],
                "sequence_tables": self.sequence_tables[db_name],
                "table_schema": self.table_schemas[db_name],
                "files_metadata": self.files_metadata[db_name],
            }
            file_path = get_path_of_physical_backup_metadata(self._db_base_path, db_name)
            with open(file_path, "w") as f:
                json.dump(data, f)

            os.chmod(file_path, 0o777)

            with open(file_path, "rb", buffering=0) as f:
                os.fsync(f.fileno())

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
            if response.status_code in [417, 500, 502, 503, 504] and retries <= 10:
                retries += 1
                time.sleep(15 + randint(2, 8))
                continue

            response.raise_for_status()
            break

    @step("Unlock Tables")
    def unlock_all_tables(self):
        for db_name in self.databases:
            self._unlock_tables(db_name)

    def export_table_schema(self, db_name: str) -> str:
        self._kill_other_db_connections(db_name)
        command = [
            "mariadb-dump",
            "-u",
            self._db_user,
            "-p" + self._db_password,
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

    def get_db(self, db_name: str) -> CustomPeeweeDB:
        instance = self._db_instances.get(db_name, None)
        if instance is not None:
            if not instance.is_connection_usable():
                raise DatabaseConnectionClosedWithDatabase(
                    f"Database connection closed with database {db_name}"
                )
            return instance
        if db_name not in self.databases:
            raise ValueError(f"Database {db_name} not found")
        self._db_instances[db_name] = CustomPeeweeDB(
            db_name,
            user=self._db_user,
            password=self._db_password,
            host=self._db_host,
            port=self._db_port,
        )
        self._db_instances[db_name].connect()
        # Set session wait timeout to 4 hours [EXPERIMENTAL]
        self._db_instances[db_name].execute_sql("SET SESSION wait_timeout = 14400;")
        # Fetch the connection id
        self._db_instances_connection_id[db_name] = int(
            self._db_instances[db_name].execute_sql("SELECT CONNECTION_ID();").fetchone()[0]
        )
        return self._db_instances[db_name]

    def _kill_other_db_connections(self, db_name: str):
        kill_other_db_connections(self.get_db(db_name), [self._db_instances_connection_id[db_name]])

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


def get_path_of_physical_backup_metadata(db_base_path: str, database_name: str) -> str:
    return os.path.join(db_base_path, database_name, "physical_backup_meta.json")


def is_db_connection_usable(db: CustomPeeweeDB) -> bool:
    try:
        if not db.is_connection_usable():
            return False
        db.execute_sql("SELECT 1;")
        return True
    except Exception:
        return False


def run_sql_query(db: CustomPeeweeDB, query: str) -> list[str]:
    """
    Return the result of the query as a list of rows
    """
    cursor = db.execute_sql(query)
    if not cursor.description:
        return []
    rows = cursor.fetchall()
    return [row for row in rows]


def kill_other_db_connections(db: CustomPeeweeDB, thread_ids: list[int]):
    """
    We deactivate site before backup/restore and activate site after backup/restore.
    But, connection through ProxySQL or Frappe Cloud devtools can still be there.

    it's important to kill all the connections except current threads.
    """

    # Get process list
    thread_ids_str = ",".join([str(thread_id) for thread_id in thread_ids])
    query = (
        "SELECT ID from INFORMATION_SCHEMA.PROCESSLIST "
        "where DB=DATABASE() AND USER!='system user' "
        f"AND ID NOT IN ({thread_ids_str});"
    )

    rows = run_sql_query(db, query)
    db_pids = [row[0] for row in rows]
    if not db_pids:
        return

    # Kill the processes
    for pid in db_pids:
        with contextlib.suppress(Exception):
            run_sql_query(db, f"KILL {pid};")
