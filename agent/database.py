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
        except (peewee.ProgrammingError, peewee.InternalError) as e:
            return False, str(e)
        except Exception:
            return (
                False,
                "Failed to execute query due to unknown error. Please check the query and try again later.",
            )

    """
    NOTE: These methods require root access to the database
    - add_user
    - remove_user
    - modify_user_access
    """

    def add_user(self, username: str, password: str):
        query = f"""CREATE OR REPLACE USER '{username}'@'%' IDENTIFIED BY '{password}';
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

    def modify_user_access(self, username: str, mode: str, permissions: dict | None = None) -> None:  # noqa C901
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
        privileges = {
            "read_only": "SELECT",
            "read_write": "ALL",
        }
        # fetch existing privileges
        records = self._run_sql(f"SHOW GRANTS FOR '{username}'@'%';", as_dict=False)
        granted_records: list[str] = []
        if len(records) > 0 and records[0]["output"]["data"]:
            granted_records = records[0]["output"]["data"]

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
            queries.append(
                record.replace("GRANT", "REVOKE").replace(f"TO `{username}`@`%", f"FROM `{username}`@`%`")
                + ";"
            )

        # add new privileges
        if mode == "read_only" or mode == "read_write":
            privilege = privileges[mode]
            queries.append(f"GRANT {privilege} ON {self.database_name}.* TO `{username}`@`%`;")
        elif mode == "granular":
            for table_name in permissions:
                columns = ""
                if isinstance(permissions[table_name]["columns"], list):
                    if len(permissions[table_name]["columns"]) == 0:
                        raise ValueError(
                            "columns cannot be an empty list. please specify '*' or [at least one column]"
                        )
                    columns = ",".join(permissions[table_name]["columns"])
                    columns = f"({columns})"

                privilege = privileges[permissions[table_name]["mode"]]
                queries.append(
                    f"GRANT {privilege} {columns} ON {self.database_name}.`{table_name}` TO `{username}`@`%`;"
                )

        # flush privileges to apply changes
        queries.append("FLUSH PRIVILEGES;")
        queries_str = "\n".join(queries)

        self._run_sql(queries_str, commit=True, allow_all_stmt_types=True)

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
                also wouldnt allow to run ddl, dcl or tcl queries
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
