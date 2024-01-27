from decimal import Decimal
import os
from agent.server import Server
from pathlib import Path
from datetime import datetime, timezone
import re
from peewee import MySQLDatabase


class DatabaseServer(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]

        self.mariadb_directory = "/var/lib/mysql"
        self.pt_stalk_directory = "/var/lib/pt-stalk"

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
            f" {log} | grep -Piv '{LINES_TO_SKIP}' | head -n {max_lines}"
        )

        DELIMITER = "/*!*/;"

        events = []
        timestamp = 0
        for line in self.execute(command, skip_output_log=True)[
            "output"
        ].split(DELIMITER):
            line = line.strip()
            if line.startswith("SET TIMESTAMP"):
                timestamp = int(line.split("=")[-1].split(".")[0])
            else:
                if any(line.startswith(skip) for skip in ["SET", "/*!"]):
                    continue
                elif line and timestamp and re.search(search_pattern, line):
                    events.append(
                        {
                            "query": line,
                            "timestamp": str(
                                datetime.utcfromtimestamp(timestamp)
                            ),
                        }
                    )
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
                        "modified": str(
                            datetime.utcfromtimestamp(unix_timestamp)
                        ),
                    }
                )
        return sorted(files, key=lambda x: x["name"])

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

    def kill_processes(
        self, private_ip, mariadb_root_password, kill_threshold
    ):
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
                if (process["Time"] or 0) >= kill_threshold:
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

    def fetch_column_stats(
        self, schema, table, private_ip, mariadb_root_password
    ):
        """Get various stats about columns in a table.

        Refer:
            - https://mariadb.com/kb/en/engine-independent-table-statistics/
            - https://mariadb.com/kb/en/mysqlcolumn_stats-table/
        """
        mariadb = MySQLDatabase(
            "mysql",
            user="root",
            password=mariadb_root_password,
            host=private_ip,
            port=3306,
        )

        try:
            self.sql(
                mariadb,
                f"ANALYZE TABLE `{schema}`.`{table}` PERSISTENT FOR ALL",
            )

            results = self.sql(
                mariadb,
                """
                SELECT column_name, nulls_ratio, avg_length, avg_frequency, decode_histogram(hist_type,histogram) as histogram
                from mysql.column_stats
                WHERE db_name = %s
                    and table_name = %s """,
                (schema, table),
            )

            for row in results:
                for column in ["nulls_ratio", "avg_length", "avg_frequency"]:
                    row[column] = float(row[column]) if row[column] else None
        except Exception as e:
            print(e)

        return results

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
            if re.match(name, file.name):
                diagnostics.append(
                    {
                        "type": file.name.replace(name, "").strip("-"),
                        "output": open(
                            os.path.join(self.pt_stalk_directory, file.name)
                        ).read(),
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
                        "timestamp": datetime.strptime(
                            stalk, "%Y_%m_%d_%H_%M_%S"
                        )
                        .replace(tzinfo=timezone.utc)
                        .isoformat(),
                    }
                )
        return sorted(stalks, key=lambda x: x["name"])

    def get_performance_report(self, private_ip, mariadb_root_password):
        mariadb = MySQLDatabase(
            "mysql",
            user="root",
            password=mariadb_root_password,
            host=private_ip,
            port=3306,
        )

        reports_sql = {
            "total_allocated_memory": "select total_allocated from sys.`x$memory_global_total`",
            "top_memory_by_event": "select * from sys.`x$memory_global_by_current_bytes`",
            "top_memory_by_user": "select * from sys.`x$memory_by_user_by_current_bytes`",
            "top_memory_by_host": "select * from sys.`x$memory_by_host_by_current_bytes`",
            "top_memory_by_thread": "select * from sys.`x$memory_by_thread_by_current_bytes`",
            "top_io_by_file_activity_report": "select * from sys.`x$io_global_by_file_by_bytes`",
            "top_io_by_file_by_time": "select * from sys.`x$io_global_by_file_by_latency`",
            "top_io_by_event_category": "select * from sys.`x$io_global_by_wait_by_bytes`",
            "top_io_in_time_by_event_category": "select * from sys.`x$io_global_by_wait_by_latency`",
            "top_io_by_user_or_thread": "select * from sys.`x$io_by_thread_by_latency`",
            "schema_index_statistics": "select * from sys.`x$schema_index_statistics`",
            "schema_table_statistics": "select * from sys.`x$schema_table_statistics`",
            "schema_table_statistics_with_buffer": "select * from sys.`x$schema_table_statistics_with_buffer`",
            "schema_tables_with_full_table_scans": "select * from sys.`schema_tables_with_full_table_scans`",
            "schema_unused_indexes": "select * from sys.`schema_unused_indexes`",
            "global_waits_by_time": "select * from sys.`x$waits_global_by_latency`",
            "waits_by_user_by_time": "select * from sys.`x$waits_by_user_by_latency`",
            "wait_classes_by_time": "select * from sys.`x$wait_classes_global_by_latency`",
            "waits_classes_by_avg_time": "select * from sys.`x$wait_classes_global_by_avg_latency`",
            "innodb_buffer_stats_by_schema": "select * from sys.`x$innodb_buffer_stats_by_schema`",
            "innodb_buffer_stats_by_table": "select * from sys.`x$innodb_buffer_stats_by_table`",
            "user_resource_use_overview": "select * from sys.`x$user_summary`",
            "user_resource_use_io_statistics": "select * from sys.`x$user_summary_by_file_io_type`",
        }

        data = {}
        for key, sql in reports_sql.items():
            data[key] = self.sql(mariadb, sql)
            if key == "total_allocated_memory":
                data[key] = data[key][0]["total_allocated_memory"]

        # convert Decimal to float
        for key, value in data.items():
            if isinstance(value, list):
                for row in value:
                    for column in row:
                        if isinstance(row[column], Decimal):
                            row[column] = float(row[column])
            elif isinstance(value, dict):
                for column in value:
                    if isinstance(value[column], Decimal):
                        value[column] = float(value[column])
            elif isinstance(value, Decimal):
                data[key] = float(value)

        return data