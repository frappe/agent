from __future__ import annotations

import contextlib
import json
from decimal import Decimal
from typing import Any

import peewee


class Database:
    def __init__(self, host, port, user, password, database):
        self.database_name = database
        self.db: CustomPeeweeDB = CustomPeeweeDB(database, user=user, password=password, host=host, port=port)

    # Methods
    def execute_query(self, query: str, commit: bool = False, as_dict: bool = False) -> tuple[bool, Any]:
        """
        This function will take the query and run in database.

        It will return a tuple of (bool, str)
        bool: Whether the query has been executed successfully
        str: The output of the query. It can be the output or error message as well
        """
        try:
            return True, self._run_sql(query, commit=commit, as_dict=as_dict)
        except (peewee.ProgrammingError, peewee.InternalError, peewee.OperationalError) as e:
            return False, str(e)
        except Exception:
            return (
                False,
                "Failed to execute query due to unknown error. Please check the query and try again later.",
            )

    """
    NOTE: These methods require root access to the database
    - create_user
    - remove_user
    - modify_user_permissions
    """

    def create_user(self, username: str, password: str):
        query = f"""
            CREATE OR REPLACE USER '{username}'@'%' IDENTIFIED BY '{password}';
            FLUSH PRIVILEGES;
        """
        self._run_sql(
            query,
            commit=True,
        )

    def remove_user(self, username: str):
        self._run_sql(
            f"""
                DROP USER IF EXISTS '{username}'@'%';
                FLUSH PRIVILEGES;
            """,
            commit=True,
        )

    def modify_user_permissions(self, username: str, mode: str, permissions: dict | None = None) -> None:  # noqa C901
        """
        Args:
            username: username of the user, whos privileges are to be modified
            mode: permission mode
                - read_only: read only access to all tables
                - read_write: read write access to all tables
                - granular: granular access to tables

            permissions: list of permissions [only required if mode is granular]
            {
                "<table_name>": {
                    "mode": "read_only" // read_only or read_write,
                    "columns": "*" // "*" or ["column1", "column2", ...]
                },
                ...
            }
            all_read_only: True if you want to make all tables read only for the user
            all_read_write: True if you want to make all tables read write for the user

        Returns:
            It will return nothing, if anything goes wrong it will raise an exception
        """
        if not permissions:
            permissions = {}

        if mode not in ["read_only", "read_write", "granular"]:
            raise ValueError("mode must be read_only, read_write or granular")
        privileges_map = {
            "read_only": "SELECT",
            "read_write": "ALL",
        }
        # fetch existing privileges
        records = self._run_sql(f"SHOW GRANTS FOR '{username}'@'%';", as_dict=False)
        granted_records: list[str] = []
        if len(records) > 0 and records[0]["output"]["data"] and len(records[0]["output"]["data"]) > 0:
            granted_records = [x[0] for x in records[0]["output"]["data"] if len(x) > 0]

        queries = []
        """
        First revoke all existing privileges

        Prepare revoke permission sql query

        `Show Grants` output:
        GRANT SELECT ON `_cbace6eaa306751d`.* TO `_cbace6eaa306751d_read_only`@`%`
        ...

        That need to be converted to this for revoke privileges
        REVOKE SELECT ON _cbace6eaa306751d.* FROM '_cbace6eaa306751d_read_only'@'%'
        """
        for record in granted_records:
            if record.startswith("GRANT USAGE"):
                # dont revoke usage
                continue
            queries.append(
                record.replace("GRANT", "REVOKE").replace(f"TO `{username}`@`%`", f"FROM `{username}`@`%`")
                + ";"
            )

        # add new privileges
        if mode == "read_only" or mode == "read_write":
            privilege = privileges_map[mode]
            queries.append(f"GRANT {privilege} ON {self.database_name}.* TO `{username}`@`%`;")
        elif mode == "granular":
            for table_name in permissions:
                columns = ""
                if isinstance(permissions[table_name]["columns"], list):
                    if len(permissions[table_name]["columns"]) == 0:
                        raise ValueError(
                            "columns cannot be an empty list. please specify '*' or at least one column"
                        )
                    requested_columns = permissions[table_name]["columns"]
                    columns = ",".join([f"`{x}`" for x in requested_columns])
                    columns = f"({columns})"

                privilege = privileges_map[permissions[table_name]["mode"]]
                if columns == "" or privilege == "SELECT":
                    queries.append(
                        f"GRANT {privilege} {columns} ON `{self.database_name}`.`{table_name}` TO `{username}`@`%`;"  # noqa: E501
                    )
                else:
                    # while usisng column level privileges `ALL` doesnt work
                    # So we need to provide all possible privileges for that columns
                    for p in ["SELECT", "INSERT", "UPDATE", "REFERENCES"]:
                        queries.append(
                            f"GRANT {p} {columns} ON `{self.database_name}`.`{table_name}` TO `{username}`@`%`;"  # noqa: E501
                        )

        # flush privileges to apply changes
        queries.append("FLUSH PRIVILEGES;")
        queries_str = "\n".join(queries)

        self._run_sql(queries_str, commit=True, allow_all_stmt_types=True)

    def fetch_database_table_sizes(self) -> dict:
        data = self._run_sql(
            f"SELECT table_name, data_length, index_length FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA"
            f"='{self.database_name}'",
            as_dict=True,
        )
        if len(data) == 0:
            return []
        data = data[0]["output"]
        tables = {}
        for d in data:
            tables[d["table_name"]] = {
                "data_length": int(d["data_length"]),
                "index_length": int(d["index_length"]),
                "total_size": int(d["data_length"]) + int(d["index_length"]),
            }
            d["data_length"] = int(d["data_length"])
            d["index_length"] = int(d["index_length"])
            d["total_size"] = d["data_length"] + d["index_length"]
        return tables

    def fetch_database_table_schema(self, include_index_info: bool = True):
        index_info = {}
        index_usage_info = {}
        data = self._run_sql(
            f"""SELECT
                            TABLE_NAME AS `table`,
                            COLUMN_NAME AS `column`,
                            DATA_TYPE AS `data_type`,
                            IS_NULLABLE AS `is_nullable`,
                            COLUMN_DEFAULT AS `default`
                        FROM
                            INFORMATION_SCHEMA.COLUMNS
                        WHERE
                            TABLE_SCHEMA='{self.database_name}';
                    """,
            as_dict=True,
        )
        if len(data) == 0:
            return {}
        data = data[0]["output"]
        tables = {}  # <table_name>: [<column_1_info>, <column_2_info>, ...]

        if include_index_info:
            index_info = self.fetch_database_table_indexes()
            index_usage_info = self.fetch_database_table_index_usage()

        for record in data:
            if record["table"] not in tables:
                tables[record["table"]] = []
            indexes = index_info.get(record["table"], {}).get(record["column"], [])
            column_index_usage = {}
            for index in indexes:
                column_index_usage[index] = index_usage_info.get(record["table"], {}).get(index, 0)

            tables[record["table"]].append(
                {
                    "column": record["column"],
                    "data_type": record["data_type"],
                    "is_nullable": record["is_nullable"] == "YES",
                    "default": record["default"],
                    "index_info": {
                        "is_indexed": len(indexes) > 0,
                        "indexes": indexes,
                        "index_usage": column_index_usage,
                    },
                }
            )
        return tables

    def fetch_database_table_indexes(self):
        data = self._run_sql(
            f"""
        SELECT
            TABLE_NAME AS `table`,
            COLUMN_NAME AS `column`,
            INDEX_NAME AS `index`
        FROM
            INFORMATION_SCHEMA.STATISTICS
        WHERE
            TABLE_SCHEMA='{self.database_name}'
        """,
            as_dict=True,
        )
        if len(data) == 0:
            return {}
        data = data[0]["output"]
        tables = {}  # <table_name>: { <column_name> : [<index1>, <index2>, ...] }
        for record in data:
            if record["table"] not in tables:
                tables[record["table"]] = {}
            if record["column"] not in tables[record["table"]]:
                tables[record["table"]][record["column"]] = []
            tables[record["table"]][record["column"]].append(record["index"])
        return tables

    def fetch_database_table_index_usage(self):
        data = self._run_sql(
            f"""
        SELECT
            TABLE_NAME AS `table`,
            INDEX_NAME AS `index`,
            ROWS_READ AS `rows_read`
        FROM
            INFORMATION_SCHEMA.INDEX_STATISTICS
        WHERE
            TABLE_SCHEMA='{self.database_name}'
        """,
            as_dict=True,
        )
        if len(data) == 0:
            return {}
        data = data[0]["output"]
        tables = {}  # <table_name>: { <index_name> : <rows_read> }
        for record in data:
            if record["table"] not in tables:
                tables[record["table"]] = {}
            tables[record["table"]][record["index"]] = int(record["rows_read"])
        return tables

    def explain_query(self, query) -> list:
        result = self._run_sql(query=query, as_dict=True)
        return result[0]["output"]

    def explain_queries(self, queries: list) -> dict:
        if len(queries) == 0:
            return {}
        sql_query = ""
        for query in queries:
            sql_query += f"EXPLAIN {query};\n"
        result = self._run_sql(query=sql_query, as_dict=True)
        data = {}
        for record in result:
            data[record["query"]] = record["output"]
        return data

    def fetch_database_column_statistics(self, table):
        """Get various stats about columns in a table.

        Refer:
            - https://mariadb.com/kb/en/engine-independent-table-statistics/
            - https://mariadb.com/kb/en/mysqlcolumn_stats-table/
        """
        self._run_sql(
            f"ANALYZE TABLE `{self.database_name}`.`{table}` PERSISTENT FOR ALL",
        )

        results = self._run_sql(
            f"""
            SELECT
                column_name, nulls_ratio, avg_length, avg_frequency,
                decode_histogram(hist_type,histogram) as histogram
            FROM
                mysql.column_stats
            WHERE
                db_name='{self.database_name}'
                and table_name='{table}'
            """,
            as_dict=True,
        )
        if len(results) == 0:
            raise Exception("Failed to fetch column stats")
        result = results[0]["output"]

        for row in results:
            for column in ["nulls_ratio", "avg_length", "avg_frequency"]:
                row[column] = float(row[column]) if row[column] else None

        return result

    def fetch_summarized_performance_report(self):
        queries = f"""
-- Top 10 time consuming queries;
SELECT
    (SUM_TIMER_WAIT / SUM(SUM_TIMER_WAIT) OVER() * 100) AS percent,
    round(SUM_TIMER_WAIT/1000000000, 1) AS total_time_ms,
    COUNT_STAR AS calls,
    round(AVG_TIMER_WAIT/1000000000, 1) AS avg_time_ms,
    DIGEST_TEXT AS query
FROM performance_schema.events_statements_summary_by_digest
    WHERE SCHEMA_NAME='{self.database_name}'
    ORDER BY SUM_TIMER_WAIT DESC
    LIMIT 10;

-- Top 10 queries with full table scans;
-- https://mariadb.com/docs/server/ref/mdb/sys/statements_with_full_table_scans/
SELECT
  sys.format_statement(
    t.DIGEST_TEXT
  ) AS query,
  t.DIGEST_TEXT AS example,
  t.COUNT_STAR AS calls,
  t.SUM_ROWS_SENT AS rows_sent,
  t.SUM_ROWS_EXAMINED AS rows_examined
FROM
  performance_schema.events_statements_summary_by_digest as t
WHERE
  t.SCHEMA_NAME = '{self.database_name}' and
  (
    t.SUM_NO_INDEX_USED > 0
    or t.SUM_NO_GOOD_INDEX_USED > 0
  )
  and t.DIGEST_TEXT not like 'SHOW%'
ORDER BY
  round(
    ifnull(
      t.SUM_NO_INDEX_USED / nullif(
        t.COUNT_STAR,
        0
      ),
      0
    ) * 100,
    0
  ) DESC,
  t.SUM_TIMER_WAIT DESC
LIMIT 10;

-- Unused Indexes;
-- https://mariadb.com/docs/server/ref/mdb/sys/schema_unused_indexes/
SELECT
    performance_schema.table_io_waits_summary_by_index_usage.OBJECT_NAME AS table_name,
    performance_schema.table_io_waits_summary_by_index_usage.INDEX_NAME AS index_name
FROM
    performance_schema.table_io_waits_summary_by_index_usage
WHERE
    performance_schema.table_io_waits_summary_by_index_usage.OBJECT_SCHEMA = '{self.database_name}' and
    performance_schema.table_io_waits_summary_by_index_usage.INDEX_NAME is not null and
    performance_schema.table_io_waits_summary_by_index_usage.COUNT_STAR = 0 and
    performance_schema.table_io_waits_summary_by_index_usage.OBJECT_SCHEMA <> 'mysql' and
    performance_schema.table_io_waits_summary_by_index_usage.INDEX_NAME <> 'PRIMARY'
ORDER BY
    performance_schema.table_io_waits_summary_by_index_usage.OBJECT_SCHEMA,
    performance_schema.table_io_waits_summary_by_index_usage.OBJECT_NAME


-- Redundant Indexes;
-- https://mariadb.com/docs/server/ref/mdb/sys/schema_redundant_indexes/
SELECT
  redundant_keys.table_name AS table_name,
  redundant_keys.index_name AS redundant_index_name,
  redundant_keys.index_columns AS redundant_index_columns,
  dominant_keys.index_name AS dominant_index_name,
  dominant_keys.index_columns AS dominant_index_columns
FROM
  (
    sys.x$schema_flattened_keys redundant_keys
    JOIN sys.x$schema_flattened_keys dominant_keys ON(
      redundant_keys.table_schema = dominant_keys.table_schema
      and redundant_keys.table_name = dominant_keys.table_name
    )
  )
WHERE
  redundant_keys.table_schema = '{self.database_name}' and
  redundant_keys.index_name <> dominant_keys.index_name
  and (
    redundant_keys.index_columns = dominant_keys.index_columns
    and (
      redundant_keys.non_unique > dominant_keys.non_unique
      or redundant_keys.non_unique = dominant_keys.non_unique
      and if(
        redundant_keys.index_name = 'PRIMARY',
        '', redundant_keys.index_name
      ) > if(
        dominant_keys.index_name = 'PRIMARY',
        '', dominant_keys.index_name
      )
    )
    or locate(
      concat(
        redundant_keys.index_columns,
        ','
      ),
      dominant_keys.index_columns
    ) = 1
    and redundant_keys.non_unique = 1
    or locate(
      concat(
        dominant_keys.index_columns,
        ','
      ),
      redundant_keys.index_columns
    ) = 1
    and dominant_keys.non_unique = 0
  );
"""

        result = self._run_sql(queries, as_dict=True)
        return {
            "top_10_time_consuming_queries": result[0]["output"],
            "top_10_queries_with_full_table_scan": result[1]["output"],
            "unused_indexes": result[2]["output"],
            "redundant_indexes": result[3]["output"],
        }

    def fetch_process_list(self):
        result = self._run_sql("SHOW FULL PROCESSLIST", as_dict=True)
        if len(result) == 0:
            return []
        return [
            {
                "id": str(record["Id"]),
                "command": record["Command"],
                "query": record["Info"],
                "state": record["State"],
                "time": record["Time"],
                "db_user": record["User"],
                "db_user_host": record["Host"].split(":")[0],
            }
            for record in result[0]["output"]
            if record["db"] == self.database_name
        ]

    def kill_process(self, pid: str):
        with contextlib.suppress(Exception):
            processes = self.fetch_process_list()
            """
            It's important to validate whether the pid belongs to the current database
            As we are running it as `root` user, it can be possible to kill processes from other databases
            by forging the request
            """
            isFound = False
            for process in processes:
                if process.get("id") == pid:
                    isFound = True
                    break
            if not isFound:
                return
            """
            The query can fail if the process is already killed.
            Anyway we need to reload the process list after killing to verify if the process is killed.

            We can safely ignore the exception
            """
            self._run_sql(f"KILL {pid}")

    # Private helper methods
    def _run_sql(  # noqa C901
        self, query: str, commit: bool = False, as_dict: bool = False, allow_all_stmt_types: bool = False
    ) -> list[dict]:
        """
        Run sql query in database
        It supports multi-line SQL queries. Each SQL Query should be terminated with `;\n`

        Args:
        query: SQL query string
        commit: True if you want to commit the changes. If commit is false, it will rollback the changes and
                also wouldn't allow to run ddl, dcl or tcl queries
        as_dict: True if you want to return the result as a dictionary (like frappe.db.sql).
                Otherwise it will return a dict of columns and data
        allow_all_stmt_types: True if you want to allow all type of sql statements
            Default: False

        Return Format:
        For as_dict = True:
        [
            {
                "output": [
                    {
                        "name" : "Administrator",
                        "modified": "2019-01-01 00:00:00",
                    },
                    ...
                ]
                "query": "SELECT name, modified FROM `tabUser`",
                "row_count": 10
            },
            ...
        ]

        For as_dict = False:
        [
            {
                "output": {
                    "columns": ["name", "modified"],
                    "data": [
                        ["Administrator", "2019-01-01 00:00:00"],
                        ...
                    ]
                },
                "query": "SELECT name, modified FROM `tabUser`",
                "row_count": 10
            },
            ...
        ]
        """

        queries = [x.strip() for x in query.split(";\n")]
        queries = [x for x in queries if x and not x.startswith("--")]

        if len(queries) == 0:
            raise peewee.ProgrammingError("No query provided")

        # Start transaction
        self.db.begin()
        results = []
        with self.db.atomic() as transaction:
            try:
                for q in queries:
                    self.last_executed_query = q
                    if not commit and self._is_ddl_query(q):
                        raise peewee.ProgrammingError("Provided DDL query is not allowed in read only mode")
                    if not allow_all_stmt_types and self._is_dcl_query(q):
                        raise peewee.ProgrammingError("DCL query is not allowed to execute")
                    if not allow_all_stmt_types and self._is_tcl_query(q):
                        raise peewee.ProgrammingError("TCL query is not allowed to execute")
                    output = None
                    row_count = None
                    cursor = self.db.execute_sql(q)
                    row_count = cursor.rowcount
                    if cursor.description:
                        rows = cursor.fetchall()
                        columns = [d[0] for d in cursor.description]
                        if as_dict:
                            output = list(map(lambda x: dict(zip(columns, x)), rows))
                        else:
                            output = {"columns": columns, "data": rows}
                    results.append({"query": q, "output": output, "row_count": row_count})
            except:
                # if query execution fails, rollback the transaction and raise the error
                transaction.rollback()
                raise
            else:
                if commit:
                    # If commit is True, try to commit the transaction
                    try:
                        transaction.commit()
                    except:
                        transaction.rollback()
                        raise
                else:
                    # If commit is False, rollback the transaction to discard the changes
                    transaction.rollback()

        with contextlib.suppress(Exception):
            self.db.close()
        return results

    def _is_ddl_query(self, query: str) -> bool:
        return query.upper().startswith(("CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "COMMENT"))

    def _is_dcl_query(self, query: str) -> bool:
        return query.upper().startswith(("GRANT", "REVOKE"))

    def _is_tcl_query(self, query: str) -> bool:
        query = query.upper().replace(" ", "")
        return query.startswith(("COMMIT", "ROLLBACK", "SAVEPOINT", "BEGINTRANSACTION"))


class JSONEncoderForSQLQueryResult(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return str(obj)


class CustomPeeweeDB(peewee.MySQLDatabase):
    """
    Override peewee.MySQLDatabase to modify `execute_sql` method

    All queries coming from end-user has value inside query, so we can't pass the params seperately.
    Peewee set `params` arg of `execute_sql` to `()` by default.

    We are overriding `execute_sql` method to pass the params as None
    So that, pymysql doesn't try to parse the query and insert params in the query
    """

    __exception_wrapper__ = peewee.ExceptionWrapper(
        {
            "ConstraintError": peewee.IntegrityError,
            "DatabaseError": peewee.DatabaseError,
            "DataError": peewee.DataError,
            "IntegrityError": peewee.IntegrityError,
            "InterfaceError": peewee.InterfaceError,
            "InternalError": peewee.InternalError,
            "NotSupportedError": peewee.NotSupportedError,
            "OperationalError": peewee.OperationalError,
            "ProgrammingError": peewee.ProgrammingError,
            "TransactionRollbackError": peewee.OperationalError,
        }
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute_sql(self, sql):
        if self.in_transaction():
            commit = False
        elif self.commit_select:
            commit = True
        else:
            commit = not sql[:6].lower().startswith("select")

        with self.__exception_wrapper__:
            cursor = self.cursor(commit)
            try:
                cursor.execute(sql, None)  # params passed as none
            except Exception:
                if self.autorollback and not self.in_transaction():
                    self.rollback()
                raise
            else:
                if commit and not self.in_transaction():
                    self.commit()
        return cursor
