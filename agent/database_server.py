from __future__ import annotations

import contextlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import psutil
from mariadb_binlog_indexer import Indexer as BinlogIndexer
from peewee import MySQLDatabase

from agent.database import Database
from agent.job import job, step
from agent.server import Server


class DatabaseServer(Server):
    def __init__(self, directory=None):
        super().__init__(directory=directory)

        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]

        self.mariadb_directory = "/var/lib/mysql"
        self.pt_stalk_directory = "/var/lib/pt-stalk"

        self.job = None
        self.step = None

    def ping(self, private_ip, mariadb_root_password):
        """Ping the MariaDB server to check if it is reachable."""
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host=private_ip,
                port=3306,
            )
            mariadb.connect(reuse_if_open=True)
            mariadb.execute_sql("SELECT 1;")
            # Close the connection to avoid leaving it open
            mariadb.close()
            return True
        except Exception:
            return False

    def search_binary_log(
        self,
        log,
        database,
        start_datetime,
        stop_datetime,
        search_pattern,
        max_lines,
    ):
        log = os.path.join(self.mariadb_directory, log)
        LINES_TO_SKIP = r"^(USE|COMMIT|START TRANSACTION|DELIMITER|ROLLBACK|#)"
        command = (
            f"mysqlbinlog --short-form --database {database} "
            f"--start-datetime '{start_datetime}' "
            f"--stop-datetime '{stop_datetime}' "
            f" {log} | grep -Piv '{LINES_TO_SKIP}'"
        )

        DELIMITER = "/*!*/;"

        events = []
        timestamp = 0
        for line in self.execute(command, skip_output_log=True)["output"].split(DELIMITER):
            line = line.strip()
            if line.startswith("SET TIMESTAMP"):
                timestamp = int(line.split("=")[-1].split(".")[0])
            else:
                if any(line.startswith(skip) for skip in ["SET", "/*!"]):
                    continue
                if line and timestamp and re.search(search_pattern, line):
                    events.append(
                        {
                            "query": line,
                            "timestamp": str(datetime.utcfromtimestamp(timestamp)),
                        }
                    )
                    if len(events) > max_lines:
                        break
        return events

    @property
    def binary_logs(self):
        BINARY_LOG_FILE_PATTERN = r"mysql-bin.\d+"
        files = []
        for file in Path(self.mariadb_directory).iterdir():
            if re.match(BINARY_LOG_FILE_PATTERN, file.name):
                unix_timestamp = int(file.stat().st_mtime)
                files.append(
                    {
                        "name": file.name,
                        "size": file.stat().st_size,
                        "modified": str(datetime.utcfromtimestamp(unix_timestamp)),
                    }
                )
        return sorted(files, key=lambda x: x["name"])

    def get_slave_status(self, private_ip, mariadb_root_password):
        """Get the slave status of the MariaDB server."""
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host=private_ip,
                port=3306,
            )
            gtid_binlog_pos = self.sql(mariadb, "SELECT @@GLOBAL.gtid_binlog_pos;")[0][
                "@@GLOBAL.gtid_binlog_pos"
            ]
            gtid_current_pos = self.sql(mariadb, "SELECT @@GLOBAL.gtid_current_pos;")[0][
                "@@GLOBAL.gtid_current_pos"
            ]
            gtid_slave_pos = self.sql(mariadb, "SELECT @@GLOBAL.gtid_slave_pos;")
            if len(gtid_slave_pos) > 0:
                gtid_slave_pos = gtid_slave_pos[0].get("@@GLOBAL.gtid_slave_pos", "")
            else:
                gtid_slave_pos = ""

            rows = self.sql(mariadb, "SHOW SLAVE STATUS;")
            return {
                "success": True,
                "message": "Slave status retrieved successfully.",
                "data": {
                    "gtid_binlog_pos": gtid_binlog_pos,
                    "gtid_current_pos": gtid_current_pos,
                    "gtid_slave_pos": gtid_slave_pos,
                    "slave_status": rows[0] if rows else {},
                },
            }
        except Exception:
            import traceback

            return {
                "success": False,
                "message": "Failed to retrieve slave status.",
                "error": traceback.format_exc(),
            }

    def configure_replication(
        self,
        private_ip,
        mariadb_root_password,
        master_private_ip,
        master_mariadb_root_password,
        gtid_slave_pos=None,
    ):
        try:
            """Configure replication on the MariaDB server."""
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host=private_ip,
                port=3306,
            )
            mariadb.execute_sql("STOP SLAVE;")
            mariadb.execute_sql("RESET SLAVE ALL;")
            if gtid_slave_pos:
                mariadb.execute_sql(f"SET GLOBAL gtid_slave_pos = '{gtid_slave_pos}';")
            mariadb.execute_sql(f"""CHANGE MASTER TO
                MASTER_HOST = '{master_private_ip}',
                MASTER_PORT = 3306,
                MASTER_USER = 'root',
                MASTER_PASSWORD = '{master_mariadb_root_password}',
                MASTER_USE_GTID=slave_pos;
            """)
            return {
                "success": True,
                "message": "Replication configured successfully.",
            }
        except Exception:
            import traceback

            return {
                "success": False,
                "message": "Failed to configure replication.",
                "error": traceback.format_exc(),
            }

    def reset_replication(self, private_ip, mariadb_root_password):
        """Reset the replication configuration on the MariaDB server."""
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host=private_ip,
                port=3306,
            )
            mariadb.execute_sql("STOP SLAVE;")
            mariadb.execute_sql("RESET SLAVE ALL;")
            return {
                "success": True,
                "message": "Slave configuration reset successfully.",
            }
        except Exception:
            import traceback

            return {
                "success": False,
                "message": "Failed to reset replication configuration.",
                "error": traceback.format_exc(),
            }

    def start_replication(self, private_ip, mariadb_root_password):
        """Start replication on the MariaDB server."""
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host=private_ip,
                port=3306,
            )
            mariadb.execute_sql("START SLAVE;")
            return {
                "success": True,
                "message": "Replication resumed successfully.",
            }
        except Exception:
            import traceback

            return {
                "success": False,
                "message": "Failed to resume replication.",
                "error": traceback.format_exc(),
            }

    def stop_replication(self, private_ip, mariadb_root_password) -> bool:
        """Stop replication on the MariaDB server."""
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host=private_ip,
                port=3306,
            )
            mariadb.execute_sql("STOP SLAVE;")
            return {
                "success": True,
                "message": "Replication stopped successfully.",
            }
        except Exception:
            import traceback

            return {
                "success": False,
                "message": "Failed to stop replication.",
                "error": traceback.format_exc(),
            }

    def processes(self, private_ip, mariadb_root_password):
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host=private_ip,
                port=3306,
            )
            return self.sql(mariadb, "SHOW FULL PROCESSLIST")
        except Exception:
            import traceback

            traceback.print_exc()
        return []

    def variables(self, private_ip, mariadb_root_password):
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host=private_ip,
                port=3306,
            )
            return self.sql(mariadb, "SHOW VARIABLES")
        except Exception:
            import traceback

            traceback.print_exc()
        return []

    def locks(self, private_ip, mariadb_root_password):
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host=private_ip,
                port=3306,
            )
            return self.sql(
                mariadb,
                """
                    SELECT l.*, t.*
                    FROM information_schema.INNODB_LOCKS l
                    JOIN information_schema.INNODB_TRX t ON l.lock_trx_id = t.trx_id
            """,
            )
        except Exception:
            import traceback

            traceback.print_exc()
        return []

    def kill_processes(self, private_ip, mariadb_root_password, kill_threshold):
        processes = self.processes(private_ip, mariadb_root_password)
        try:
            mariadb = MySQLDatabase(
                "mysql",
                user="root",
                password=mariadb_root_password,
                host=private_ip,
                port=3306,
            )
            for process in processes:
                if (
                    (process["Time"] or 0) >= kill_threshold
                    and process["User"] != "system user"
                    and process["Command"] not in ["Binlog Dump", "Slave_SQL", "Slave_IO"]
                ):
                    mariadb.execute_sql(f"KILL {process['Id']}")
        except Exception:
            import traceback

            traceback.print_exc()

    def get_deadlocks(
        self,
        database,
        start_datetime,
        stop_datetime,
        max_lines,
        private_ip,
        mariadb_root_password,
    ):
        mariadb = MySQLDatabase(
            "percona",
            user="root",
            password=mariadb_root_password,
            host=private_ip,
            port=3306,
        )

        return self.sql(
            mariadb,
            f"""
            select *
            from deadlock
            where user = %s
            and ts >= %s
            and ts <= %s
            order by ts
            limit {int(max_lines)}""",
            (database, start_datetime, stop_datetime),
        )

    @staticmethod
    def sql(db, query, params=()):
        """Similar to frappe.db.sql, get the results as dict."""

        cursor = db.execute_sql(query, params)
        rows = cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        return list(map(lambda x: dict(zip(columns, x)), rows))

    @job("Column Statistics")
    def fetch_column_stats_job(self, schema, table, private_ip, mariadb_root_password, doc_name):
        self._fetch_column_stats_step(schema, table, private_ip, mariadb_root_password)
        return {"doc_name": doc_name}

    @step("Fetch Column Statistics")
    def _fetch_column_stats_step(self, schema, table, private_ip, mariadb_root_password):
        return self.fetch_column_stats(schema, table, private_ip, mariadb_root_password)

    def fetch_column_stats(self, schema, table, private_ip, mariadb_root_password):
        db = Database(private_ip, 3306, "root", mariadb_root_password, schema)
        results = db.fetch_database_column_statistics(table)
        return {"output": json.dumps(results)}

    def explain_query(self, schema, query, private_ip, mariadb_root_password):
        mariadb = MySQLDatabase(
            schema,
            user="root",
            password=mariadb_root_password,
            host=private_ip,
            port=3306,
        )

        if not query.lower().startswith(("select", "update", "delete")):
            return []

        try:
            return self.sql(mariadb, f"EXPLAIN {query}")
        except Exception as e:
            print(e)

    def get_stalk(self, name):
        diagnostics = []
        for file in Path(self.pt_stalk_directory).iterdir():
            if os.path.getsize(os.path.join(self.pt_stalk_directory, file.name)) > 16 * (1024**2):
                # Skip files larger than 16 MB
                continue
            if re.match(name, file.name):
                pt_stalk_path = os.path.join(self.pt_stalk_directory, file.name)
                with open(pt_stalk_path, errors="replace") as f:
                    output = f.read()

                diagnostics.append(
                    {
                        "type": file.name.replace(name, "").strip("-"),
                        "output": output,
                    }
                )
        return sorted(diagnostics, key=lambda x: x["type"])

    def get_stalks(self):
        stalk_pattern = r"(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})-output"
        stalks = []
        for file in Path(self.pt_stalk_directory).iterdir():
            matched = re.match(stalk_pattern, file.name)
            if matched:
                stalk = matched.group(1)
                stalks.append(
                    {
                        "name": stalk,
                        "timestamp": datetime.strptime(stalk, "%Y_%m_%d_%H_%M_%S")
                        .replace(tzinfo=timezone.utc)
                        .isoformat(),
                    }
                )
        return sorted(stalks, key=lambda x: x["name"])

    def purge_binlog(self, private_ip: str, mariadb_root_password: str, to_binlog: str) -> bool:
        try:
            mariadb = Database(private_ip, 3306, "root", mariadb_root_password, "mysql")
            mariadb.execute_query(f"PURGE BINARY LOGS TO '{to_binlog}';", commit=True)
            return True
        except Exception:
            return False

    def get_binlogs(self) -> dict:
        binlogs_in_disk = []
        for file in Path(self.mariadb_directory).iterdir():
            if re.match(r"mysql-bin.\d+", file.name):
                stat = file.stat()
                binlogs_in_disk.append(
                    {
                        "name": file.name,
                        "size": stat.st_size,
                        "modified_at": stat.st_mtime,
                    }
                )

        # sort binlogs by name
        binlogs_in_disk.sort(key=lambda x: x["name"])

        return {
            "binlogs_in_disk": binlogs_in_disk,
            "indexed_binlogs": self._get_indexed_binlogs(),
            "current_binlog": self._get_current_binlog(),
        }

    @property
    def binlog_indexer(self) -> BinlogIndexer:
        return BinlogIndexer(os.path.join(self.directory, "binlog-indexes"), "queries.db")

    @job("Add Binlogs To Indexer", priority="low")
    def add_binlogs_to_index_job(self, binlogs: list[str]) -> dict:
        return self.add_binlogs_to_index(binlogs)

    @step("Add Binlogs To Indexer")
    def add_binlogs_to_index(self, binlogs: list[str]) -> dict:
        data = {
            "indexed_binlogs": [],
            "message": "",
            "current_binlog": self._get_current_binlog(),
        }
        cpu_usage = psutil.cpu_percent(interval=5)
        if cpu_usage > 50:
            data["message"] = "CPU usage > 50%. Skipped indexing"
            return data

        indexed_binlogs = []
        for binlog in binlogs:
            try:
                self.binlog_indexer.add(os.path.join(self.mariadb_directory, binlog))
                indexed_binlogs.append(binlog)
            except Exception as e:
                data["message"] = f"Failed to index binlog {binlog}: {e}"
                return data

        data["indexed_binlogs"] = indexed_binlogs
        return data

    @job("Remove Binlogs From Indexer", priority="low")
    def remove_binlogs_from_index_job(self, binlogs: list[str]) -> dict:
        return self.remove_binlogs_from_index(binlogs)

    @step("Remove Binlogs From Indexer")
    def remove_binlogs_from_index(self, binlogs: list[str]):
        data = {
            "unindexed_binlogs": [],
            "message": "",
            "current_binlog": self._get_current_binlog(),
        }
        cpu_usage = psutil.cpu_percent(interval=5)
        if cpu_usage > 50:
            data["message"] = "CPU usage > 50%. Not safe to unindex binlogs"
            return False
        unindexed_binlogs = []

        for binlog in binlogs:
            try:
                self.binlog_indexer.remove(os.path.join(self.mariadb_directory, binlog))
                unindexed_binlogs.append(binlog)
            except Exception as e:
                data["message"] = f"Failed to unindex binlog {binlog}: {e}"
                return data

        data["unindexed_binlogs"] = unindexed_binlogs
        return data

    @job("Upload Binlogs To S3", priority="low")
    def upload_binlogs_to_s3_job(self, binlogs: list[str], offsite: dict) -> dict:
        return self.upload_binlogs_to_s3(binlogs, offsite)

    @step("Upload Binlogs To S3")
    def upload_binlogs_to_s3(self, binlogs: list[str], offsite):
        import boto3

        offsite_files = {}
        failed_uploads = {}

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

        tmp_folder = get_tmp_folder_path()
        for binlog in binlogs:
            binlog_file_path = os.path.join(self.mariadb_directory, binlog)
            binlog_gz_path = os.path.join(tmp_folder, f"{binlog}.gz")
            offsite_path = os.path.join(prefix, f"{binlog}.gz")
            try:
                cmd = f"gzip -c {binlog_file_path} > {binlog_gz_path}"
                subprocess.run(cmd, shell=True, check=True)
                # Size in bytes of gzipped file
                gzipped_size = os.path.getsize(binlog_gz_path)
                # Upload gzipped file to S3
                with open(binlog_gz_path, "rb") as f:
                    s3.upload_fileobj(f, bucket, offsite_path)
                offsite_files[binlog] = {
                    "size": gzipped_size,
                    "path": offsite_path,
                }
            except Exception as e:
                failed_uploads[binlog] = str(e)
            finally:
                # Delete the gzipped file if it exists
                with contextlib.suppress(Exception):
                    os.remove(binlog_gz_path)

        return {
            "offsite_files": offsite_files,
            "failed_uploads": failed_uploads,
        }

    def _get_current_binlog(self) -> str | None:
        index_file = Path(self.mariadb_directory) / "mysql-bin.index"
        if index_file.exists():
            file_names = [x.strip() for x in index_file.read_text().split("\n")]
            file_names = [x for x in file_names if x]
            if len(file_names) > 0:
                return file_names[-1].split("/var/lib/mysql/", 1)[-1]
        return None

    def _get_indexed_binlogs(self) -> list[str]:
        return [
            x[0]
            for x in self.binlog_indexer._execute_query(
                "db", "select distinct binlog from query order by binlog asc"
            )
        ]

    def get_timeline(
        self, start_timestamp: int, end_timestamp: int, database: str | None = None, type: str | None = None
    ):
        return self.binlog_indexer.get_timeline(start_timestamp, end_timestamp, type, database)

    def get_row_ids(
        self,
        start_timestamp: int,
        end_timestamp: int,
        type: str,
        database: str,
        table: str | None = None,
        search_str: str | None = None,
    ):
        return self.binlog_indexer.get_row_ids(
            start_timestamp, end_timestamp, type, database, table, search_str
        )

    def get_queries(self, row_ids: dict[str, list[int]], database: str):
        return self.binlog_indexer.get_queries(row_ids, database)


def get_tmp_folder_path():
    path = "/opt/volumes/mariadb/tmp/"
    if not os.path.exists(path):
        return "/tmp/"
    return path
