from __future__ import annotations

import os
import re
import shutil
import subprocess

import peewee

from agent.database_physical_backup import DatabaseConnectionClosedWithDatabase
from agent.database_server import DatabaseServer
from agent.job import job, step


class DatabasePhysicalRestore(DatabaseServer):
    def __init__(
        self,
        backup_db: str,
        target_db: str,
        target_db_root_password: str,
        target_db_port: int,
        target_db_host: str,
        innodb_tables: list[str],
        myisam_tables: list[str],
        table_schema: str,
        backup_db_base_directory: str,
        target_db_base_directory: str = "/var/lib/mysql",
    ):
        self._target_db_instance: peewee.MySQLDatabase = None
        self._target_db_instance_for_myisam: peewee.MySQLDatabase = None
        self.target_db = target_db
        self.target_db_user = "root"
        self.target_db_password = target_db_root_password
        self.target_db_host = target_db_host
        self.target_db_port = target_db_port
        self.target_db_directory = os.path.join(target_db_base_directory, target_db)

        self.backup_db = backup_db
        self.backup_db_directory = os.path.join(backup_db_base_directory, backup_db)

        self.innodb_tables = innodb_tables
        self.myisam_tables = myisam_tables
        self.table_schema = table_schema

        super().__init__()

    @job("Physical Restore Database")
    def restore_job(self):
        self.validate_connection_to_target_db()
        self.warmup_myisam_files()
        self.check_and_fix_myisam_table_files()
        self.warmup_innodb_files()
        # https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#importing-transportable-tablespaces-for-non-partitioned-tables
        self.prepare_target_db_for_restore()
        self.create_tables_from_table_schema()
        self.discard_innodb_tablespaces_from_target_db()
        self.perform_innodb_file_operations()
        self.import_tablespaces_in_target_db()
        self.hold_write_lock_on_myisam_tables()
        self.perform_myisam_file_operations()
        self.unlock_all_tables()

    @step("Validate Connection to Target Database")
    def validate_connection_to_target_db(self):
        self._get_target_db().execute_sql("SELECT 1;")

    @step("Warmup MyISAM Files")
    def warmup_myisam_files(self):
        files = os.listdir(self.backup_db_directory)
        files = [file for file in files if file.endswith(".MYI") or file.endswith(".MYD")]
        file_paths = [os.path.join(self.backup_db_directory, file) for file in files]
        self._warmup_files(file_paths)

    @step("Check and Fix MyISAM Table Files")
    def check_and_fix_myisam_table_files(self):
        """
        Check issues in MyISAM table files
        myisamchk :path

        If any issues found, try to repair the table
        """
        files = os.listdir(self.backup_db_directory)
        files = [file for file in files if file.endswith(".MYI")]
        for file in files:
            myisamchk_command = [
                "myisamchk",
                os.path.join(self.backup_db_directory, file),
            ]
            try:
                subprocess.check_output(myisamchk_command)
            except subprocess.CalledProcessError:
                myisamchk_command.append("--recover")
                try:
                    subprocess.check_output(myisamchk_command)
                except subprocess.CalledProcessError as e:
                    print(f"Error while repairing MyISAM table file: {e.output}")
                    print("Stopping the process")
                    raise Exception from e

        self._get_target_db_for_myisam().execute_sql("UNLOCK TABLES;")

    @step("Warmup InnoDB Files")
    def warmup_innodb_files(self):
        files = os.listdir(self.backup_db_directory)
        files = [file for file in files if file.endswith(".ibd")]
        file_paths = [os.path.join(self.backup_db_directory, file) for file in files]
        self._warmup_files(file_paths)

    @step("Prepare Database for Restoration")
    def prepare_target_db_for_restore(self):
        """
        Prepare the database for import
        - fetch existing tables list in database
        - delete all tables
        """
        tables = self._get_target_db().get_tables()
        # before start dropping tables, disable foreign key checks
        # it will reduce the time to drop tables and will not cause any block while dropping tables
        self._get_target_db().execute_sql("SET SESSION FOREIGN_KEY_CHECKS = 0;")
        for table in tables:
            self._get_target_db().execute_sql(f"DROP TABLE IF EXISTS {table};")
        self._get_target_db().execute_sql(
            "SET SESSION FOREIGN_KEY_CHECKS = 1;"
        )  # re-enable foreign key checks

    @step("Create Tables from Table Schema")
    def create_tables_from_table_schema(self):
        # https://github.com/frappe/frappe/pull/26855
        schema_file_content: str = re.sub(
            r"/\*M{0,1}!999999\\- enable the sandbox mode \*/",
            "",
            self.table_schema,
        )
        # # https://github.com/frappe/frappe/pull/28879
        schema_file_content: str = re.sub(
            r"/\*![0-9]* DEFINER=[^ ]* SQL SECURITY DEFINER \*/",
            "",
            self.table_schema,
        )
        # create the tables
        sql_stmts = schema_file_content.split(";\n")
        for sql_stmt in sql_stmts:
            if sql_stmt.strip():
                self._get_target_db().execute_sql(sql_stmt)

    @step("Discard InnoDB Tablespaces")
    def discard_innodb_tablespaces_from_target_db(self):
        # https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#foreign-key-constraints
        self._get_target_db().execute_sql("SET SESSION foreign_key_checks = 0;")
        for table in self.innodb_tables:
            self._get_target_db().execute_sql(f"ALTER TABLE {table} DISCARD TABLESPACE;")
        self._get_target_db().execute_sql(
            "SET SESSION foreign_key_checks = 1;"
        )  # re-enable foreign key checks

    @step("Copying InnoDB Table Files")
    def perform_innodb_file_operations(self):
        self._perform_file_operations(engine="innodb")

    @step("Import InnoDB Tablespaces")
    def import_tablespaces_in_target_db(self):
        for table in self.innodb_tables:
            self._get_target_db().execute_sql(f"ALTER TABLE {table} IMPORT TABLESPACE;")

    @step("Hold Write Lock on MyISAM Tables")
    def hold_write_lock_on_myisam_tables(self):
        """
        MyISAM doesn't support foreign key constraints
        So, need to take write lock on MyISAM tables

        Discard tablespace query on innodb already took care of locks
        """
        tables = [f"`{table}` WRITE" for table in self.myisam_tables]
        self._get_target_db_for_myisam().execute_sql("LOCK TABLES {};".format(", ".join(tables)))

    @step("Copying MyISAM Table Files")
    def perform_myisam_file_operations(self):
        self._perform_file_operations(engine="myisam")

    @step("Unlock All Tables")
    def unlock_all_tables(self):
        self._get_target_db().execute_sql("UNLOCK TABLES;")
        self._get_target_db_for_myisam().execute_sql("UNLOCK TABLES;")

    def _warmup_files(self, file_paths: list[str]):
        """
        Once the snapshot is converted to disk and attached to the instance,
        All the files are not immediately available to the system.

        AWS EBS volumes are lazily loaded from S3.

        So, before doing any operations on the disk, need to warm up the disk.
        But, we will selectively warm up only required blocks.

        Ref - https://docs.aws.amazon.com/ebs/latest/userguide/ebs-initialize.html
        """
        for file in file_paths:
            subprocess.run(["dd", "if=" + file, "of=/dev/null", "bs=1M"], check=True)

    def _perform_file_operations(self, engine: str):
        for file in os.listdir(self.backup_db_directory):
            # copy only .ibd, .cfg if innodb
            if engine == "innodb" and not (file.endswith(".ibd") or file.endswith(".cfg")):
                continue

            # copy one .MYI, .MYD files if myisam
            if engine == "myisam" and not (file.endswith(".MYI") or file.endswith(".MYD")):
                continue

            shutil.copy(
                os.path.join(self.backup_db_directory, file),
                os.path.join(self.target_db_directory, file),
            )

        # change ownership to mysql user and group and mode to 660
        for file in os.listdir(self.target_db_directory):
            file_path = os.path.join(self.target_db_directory, file)
            os.chmod(file_path, 0o660)
            shutil.chown(file_path, user="mysql", group="mysql")

    def _get_target_db(self) -> peewee.MySQLDatabase:
        if self._target_db_instance is not None:
            if not self._target_db_instance.is_connection_usable():
                raise DatabaseConnectionClosedWithDatabase()
            return self._target_db_instance

        self._target_db_instance = peewee.MySQLDatabase(
            self.target_db,
            user=self.target_db_user,
            password=self.target_db_password,
            host=self.target_db_host,
            port=self.target_db_port,
        )
        self._target_db_instance.connect()
        # Set session wait timeout to 4 hours [EXPERIMENTAL]
        self._target_db_instance.execute_sql("SET SESSION wait_timeout = 14400;")
        return self._target_db_instance

    def _get_target_db_for_myisam(self) -> peewee.MySQLDatabase:
        if self._target_db_instance_for_myisam is not None:
            if not self._target_db_instance_for_myisam.is_connection_usable():
                raise DatabaseConnectionClosedWithDatabase()
            return self._target_db_instance_for_myisam

        self._target_db_instance_for_myisam = peewee.MySQLDatabase(
            self.target_db,
            user=self.target_db_user,
            password=self.target_db_password,
            host=self.target_db_host,
            port=self.target_db_port,
            autocommit=False,
        )
        self._target_db_instance_for_myisam.connect()
        # Set session wait timeout to 4 hours [EXPERIMENTAL]
        self._target_db_instance_for_myisam.execute_sql("SET SESSION wait_timeout = 14400;")
        return self._target_db_instance_for_myisam

    def __del__(self):
        if self._target_db_instance is not None:
            self._target_db_instance.close()
        if self._target_db_instance_for_myisam is not None:
            self._target_db_instance_for_myisam.close()
