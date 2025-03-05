from __future__ import annotations

import json
import os
import re
import shutil
import subprocess

from agent.base import AgentException
from agent.database import CustomPeeweeDB
from agent.database_physical_backup import (
    DatabaseConnectionClosedWithDatabase,
    get_path_of_physical_backup_metadata,
)
from agent.database_server import DatabaseServer
from agent.job import job, step
from agent.utils import compute_file_hash, get_mariadb_table_name_from_path


class DatabasePhysicalRestore(DatabaseServer):
    def __init__(
        self,
        backup_db: str,
        target_db: str,
        target_db_root_password: str,
        target_db_port: int,
        target_db_host: str,
        backup_db_base_directory: str,
        target_db_base_directory: str = "/var/lib/mysql",
        restore_specific_tables: bool = False,
        tables_to_restore: list[str] | None = None,
    ):
        if tables_to_restore is None:
            tables_to_restore = []

        self._target_db_instance: CustomPeeweeDB = None
        self._target_db_instance_for_myisam: CustomPeeweeDB = None
        self.target_db = target_db
        self.target_db_user = "root"
        self.target_db_password = target_db_root_password
        self.target_db_host = target_db_host
        self.target_db_port = target_db_port
        self.target_db_directory = os.path.join(target_db_base_directory, target_db)

        self.backup_db = backup_db
        self.backup_db_base_directory = backup_db_base_directory
        self.backup_db_directory = os.path.join(backup_db_base_directory, backup_db)

        self.restore_specific_tables = restore_specific_tables
        self.tables_to_restore = tables_to_restore

        # Initialize the variables
        self.files_metadata: dict[str, dict[str, str]] = {}
        self.innodb_tables: list[str] = []
        self.myisam_tables: list[str] = []
        self.sequence_tables: list[str] = []

        super().__init__()

    @job("Physical Restore Database")
    def create_restore_job(self):
        from filewarmer import FWUP

        self.fwup = FWUP()
        self.validate_backup_files()
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
        self.perform_post_restoration_validation_and_fixes()

    @step("Validate Backup Files")
    def validate_backup_files(self):  # noqa: C901
        # fetch the required metadata to proceed
        backup_metadata_path = get_path_of_physical_backup_metadata(
            self.backup_db_base_directory, self.backup_db
        )
        if not os.path.exists(backup_metadata_path):
            raise Exception(f"Backup metadata not found for {self.backup_db}")

        backup_metadata = None
        with open(backup_metadata_path, "r") as f:
            backup_metadata = json.load(f)
        if not backup_metadata:
            raise Exception(f"Backup metadata is empty for {self.backup_db}")

        self.files_metadata = backup_metadata["files_metadata"]
        self.innodb_tables = backup_metadata["innodb_tables"]
        self.myisam_tables = backup_metadata["myisam_tables"]
        self.sequence_tables = backup_metadata.get("sequence_tables", [])
        self.table_schema = backup_metadata["table_schema"]
        if self.restore_specific_tables:
            # remove invalid tables from tables_to_restore
            all_tables = self.innodb_tables + self.myisam_tables
            self.tables_to_restore = [table for table in self.tables_to_restore if table in all_tables]

            # remove the unwanted tables
            self.innodb_tables = [table for table in self.innodb_tables if table in self.tables_to_restore]
            self.myisam_tables = [table for table in self.myisam_tables if table in self.tables_to_restore]

        # validate files
        files = os.listdir(self.backup_db_directory)
        output = ""
        invalid_files = set()
        for file in files:
            if not self.is_db_file_need_to_be_restored(file):
                continue
            if file not in self.files_metadata:
                continue
            file_metadata = self.files_metadata[file]
            file_path = os.path.join(self.backup_db_directory, file)
            # validate file size
            file_size = os.path.getsize(file_path)
            if file_size != file_metadata["size"]:
                output += f"[INVALID] [FILE SIZE] {file} - {file_size} bytes\n"
                invalid_files.add(file)
                continue

            # if file checksum is provided, validate checksum
            if file_metadata["checksum"]:
                checksum = compute_file_hash(file_path, raise_exception=True)
                if checksum != file_metadata["checksum"]:
                    output += f"[INVALID] [CHECKSUM] {file} - {checksum}\n"
                    invalid_files.add(file)

        if invalid_files:
            output += "Invalid Files:\n"
            for file in invalid_files:
                output += f"{file}\n"
            raise AgentException({"output": output})

        return {"output": output}

    @step("Validate Connection to Target Database")
    def validate_connection_to_target_db(self):
        self._get_target_db().execute_sql("SELECT 1;")

    @step("Warmup MyISAM Files")
    def warmup_myisam_files(self):
        files = os.listdir(self.backup_db_directory)
        files = [file for file in files if file.endswith(".MYI") or file.endswith(".MYD")]
        file_paths = [os.path.join(self.backup_db_directory, file) for file in files]
        file_paths = [file for file in file_paths if self.is_db_file_need_to_be_restored(file)]
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
        files = [file for file in files if self.is_db_file_need_to_be_restored(file)]
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
        file_paths = [file for file in file_paths if self.is_db_file_need_to_be_restored(file)]
        self._warmup_files(file_paths)

    @step("Prepare Database for Restoration")
    def prepare_target_db_for_restore(self):
        # Only perform this, if we are restoring all tables
        if self.restore_specific_tables:
            return

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
            self._get_target_db().execute_sql(self.get_drop_table_statement(table))
        self._get_target_db().execute_sql(
            "SET SESSION FOREIGN_KEY_CHECKS = 1;"
        )  # re-enable foreign key checks

    @step("Create Tables from Table Schema")
    def create_tables_from_table_schema(self):
        if self.restore_specific_tables:
            sql_stmts = []
            for table in self.tables_to_restore:
                sql_stmts.append(self.get_drop_table_statement(table))
                sql_stmts.append(self.get_create_table_statement(self.table_schema, table))
        else:
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

        # before start dropping tables, disable foreign key checks
        # it will reduce the time to drop tables and will not cause any block while dropping tables
        self._get_target_db().execute_sql("SET SESSION FOREIGN_KEY_CHECKS = 0;")

        # Drop and re-create the tables
        for sql_stmt in sql_stmts:
            if sql_stmt.strip():
                self._get_target_db().execute_sql(sql_stmt)

        # re-enable foreign key checks
        self._get_target_db().execute_sql("SET SESSION FOREIGN_KEY_CHECKS = 1;")

    @step("Discard InnoDB Tablespaces")
    def discard_innodb_tablespaces_from_target_db(self):
        # https://mariadb.com/kb/en/innodb-file-per-table-tablespaces/#foreign-key-constraints
        self._get_target_db().execute_sql("SET SESSION foreign_key_checks = 0;")
        for table in self.innodb_tables:
            self._get_target_db().execute_sql(f"ALTER TABLE `{table}` DISCARD TABLESPACE;")
        self._get_target_db().execute_sql(
            "SET SESSION foreign_key_checks = 1;"
        )  # re-enable foreign key checks

    @step("Copying InnoDB Table Files")
    def perform_innodb_file_operations(self):
        self._perform_file_operations(engine="innodb")

    @step("Import InnoDB Tablespaces")
    def import_tablespaces_in_target_db(self):
        for table in self.innodb_tables:
            self._get_target_db().execute_sql(f"ALTER TABLE `{table}` IMPORT TABLESPACE;")

    @step("Hold Write Lock on MyISAM Tables")
    def hold_write_lock_on_myisam_tables(self):
        """
        MyISAM doesn't support foreign key constraints
        So, need to take write lock on MyISAM tables

        Discard tablespace query on innodb already took care of locks
        """
        if not self.myisam_tables:
            return
        tables = [f"`{table}` WRITE" for table in self.myisam_tables]
        self._get_target_db_for_myisam().execute_sql("LOCK TABLES {};".format(", ".join(tables)))

    @step("Copying MyISAM Table Files")
    def perform_myisam_file_operations(self):
        self._perform_file_operations(engine="myisam")

    @step("Validate And Fix Tables")
    def perform_post_restoration_validation_and_fixes(self):
        innodb_tables_with_fts = self.get_innodb_tables_with_fts_index()
        """
        FLUSH TABLES ... FOR EXPORT does not support FULLTEXT indexes.
        https://dev.mysql.com/doc/refman/8.4/en/innodb-table-import.html#:~:text=in%20the%20operation.-,Limitations,-The%20Transportable%20Tablespaces

        We can either drop + add index.
        Or, run `OPTIMIZE TABLE` on the table to rebuild the index.
        https://mariadb.com/kb/en/optimize-table/#updating-an-innodb-fulltext-index
        """

        for table in innodb_tables_with_fts:
            """
            No need to waste time on checking whether index is corrupted or not
            Because, physical restoration will not work for FULLTEXT index.
            """
            if self.repair_table(table, "innodb"):
                raise Exception(f"Failed to repair table {table}")

        """
        MyISAM table corruption can generally happen due to mismatch of no of records in MYD file.

        myisamchk can't find and fix this issue.
        Because this out of sync happen after creating a blank MyISAM table and just copying MYF & MYI files.

        Usually, DB Restart will fix this issue. But we can't do in live database.
        So running `REPAIR TABLE ... USE_FRM` can fix the issue.
        https://dev.mysql.com/doc/refman/8.4/en/myisam-repair.html
        """
        for table in self.myisam_tables:
            if self.is_table_corrupted(table) and not self.repair_table(table, "myisam"):
                raise Exception(f"Failed to repair table {table}")

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

        self.fwup.warmup(file_paths, method="io_uring")

    def _perform_file_operations(self, engine: str):
        for file in os.listdir(self.backup_db_directory):
            # skip if file is not need to be restored
            if not self.is_db_file_need_to_be_restored(file):
                continue

            # copy only .ibd, .cfg if innodb
            if engine == "innodb" and not (file.endswith(".ibd") or file.endswith(".cfg")):
                continue

            # copy one .MYI, .MYD files if myisam
            if engine == "myisam" and not (file.endswith(".MYI") or file.endswith(".MYD")):
                continue

            shutil.copyfile(
                os.path.join(self.backup_db_directory, file),
                os.path.join(self.target_db_directory, file),
            )

    def _get_target_db(self) -> CustomPeeweeDB:
        if self._target_db_instance is not None:
            if not self._target_db_instance.is_connection_usable():
                raise DatabaseConnectionClosedWithDatabase()
            return self._target_db_instance

        self._target_db_instance = CustomPeeweeDB(
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

    def _get_target_db_for_myisam(self) -> CustomPeeweeDB:
        if self._target_db_instance_for_myisam is not None:
            if not self._target_db_instance_for_myisam.is_connection_usable():
                raise DatabaseConnectionClosedWithDatabase()
            return self._target_db_instance_for_myisam

        self._target_db_instance_for_myisam = CustomPeeweeDB(
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

    def is_table_need_to_be_restored(self, table_name: str) -> bool:
        if not self.restore_specific_tables:
            return True
        return table_name in self.innodb_tables or table_name in self.myisam_tables

    def is_db_file_need_to_be_restored(self, file_name: str) -> bool:
        return self.is_table_need_to_be_restored(get_mariadb_table_name_from_path(file_name))

    def is_sequence_table(self, table_name: str) -> bool:
        return table_name in self.sequence_tables

    def get_create_table_statement(self, sql_dump, table_name) -> str:
        if self.is_sequence_table(table_name):
            # Define the regex pattern to match the CREATE SEQUENCE statement
            pattern = re.compile(rf"CREATE SEQUENCE `{table_name}`[\s\S]*?;", re.DOTALL)
        else:
            # Define the regex pattern to match the CREATE TABLE statement
            pattern = re.compile(rf"CREATE TABLE `{table_name}`[\s\S]*?;(?=\s*(?=\n|$))", re.DOTALL)

        # Search for the CREATE TABLE statement in the SQL dump
        match = pattern.search(sql_dump)
        if match:
            return match.group(0)

        if self.is_sequence_table(table_name):
            raise Exception(f"CREATE SEQUENCE statement for {table_name} not found in SQL dump")
        else:  # noqa: RET506
            raise Exception(f"CREATE TABLE statement for {table_name} not found in SQL dump")

    def get_drop_table_statement(self, table_name) -> str:
        if self.is_sequence_table(table_name):
            return f"DROP SEQUENCE IF EXISTS `{table_name}`;"

        return f"DROP TABLE IF EXISTS `{table_name}`;"

    def is_table_corrupted(self, table_name: str) -> bool:
        result = run_sql_query(self._get_target_db(), f"CHECK TABLE `{table_name}` QUICK;")
        """
        +-----------------------------------+-------+----------+------------------------------------------------------+
        | Table                             | Op    | Msg_type | Msg_text                                             |
        +-----------------------------------+-------+----------+------------------------------------------------------+
        | _8edd549f4b072174.__global_search | check | warning  | Size of indexfile is: 22218752      Should be: 4096  |
        | _8edd549f4b072174.__global_search | check | warning  | Size of datafile is:  31303496       Should be: 0    |
        | _8edd549f4b072174.__global_search | check | error    | Record-count is not ok; is     152774   Should be: 0 |
        | _8edd549f4b072174.__global_search | check | warning  | Found     172605 key parts. Should be: 0             |
        | _8edd549f4b072174.__global_search | check | error    | Corrupt                                              |
        +-----------------------------------+-------+----------+------------------------------------------------------+

        +-------------------------------------------+-------+----------+--------------------------------------------------------+
        | Table                                     | Op    | Msg_type | Msg_text                                               |
        +-------------------------------------------+-------+----------+--------------------------------------------------------+
        | _8edd549f4b072174.energy_point_log_id_seq | check | note     | The storage engine for the table doesn't support check |
        +-------------------------------------------+-------+----------+--------------------------------------------------------+
        """  # noqa: E501
        isError = False
        for row in result:
            if row[2] == "error":
                isError = True
                break
        return isError

    def repair_table(self, table_name: str, engine: str) -> bool:
        if engine == "innodb":
            result = run_sql_query(self._get_target_db(), f"OPTIMIZE TABLE `{table_name}`;")
        elif engine == "myisam":
            result = run_sql_query(self._get_target_db(), f"REPAIR TABLE `{table_name}` USE_FRM;")
        else:
            raise Exception(f"Engine {engine} is not supported")
        """
        +---------------------------------------------------+--------+----------+----------+
        | Table                                             | Op     | Msg_type | Msg_text |
        +---------------------------------------------------+--------+----------+----------+
        | _8edd549f4b072174.tabInsights Query Execution Log | repair | status   | OK       |
        +---------------------------------------------------+--------+----------+----------+

        Msg Type can be status, error, info, note, or warning
        """
        isErrorOccurred = False
        for row in result:
            if row[2] == "error":
                isErrorOccurred = True
                break

        return not isErrorOccurred

    def get_innodb_tables_with_fts_index(self):
        rows = run_sql_query(
            self._get_target_db(),
            f"""
        SELECT
            DISTINCT(t.TABLE_NAME)
        FROM
            information_schema.STATISTICS s
        JOIN
            information_schema.TABLES t
            ON s.TABLE_SCHEMA = t.TABLE_SCHEMA
            AND s.TABLE_NAME = t.TABLE_NAME
        WHERE
            s.INDEX_TYPE = 'FULLTEXT'
            AND t.TABLE_SCHEMA = '{self.target_db}'
            AND t.ENGINE = 'InnoDB'
        """,
        )
        return [row[0] for row in rows]

    def __del__(self):
        if self._target_db_instance is not None:
            self._target_db_instance.close()
        if self._target_db_instance_for_myisam is not None:
            self._target_db_instance_for_myisam.close()


def run_sql_query(db: CustomPeeweeDB, query: str) -> list[str]:
    """
    Return the result of the query as a list of rows
    """
    cursor = db.execute_sql(query)
    if not cursor.description:
        return []
    rows = cursor.fetchall()
    return [row for row in rows]
