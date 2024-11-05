from __future__ import annotations

import contextlib
import json
from decimal import Decimal
from typing import Any

from peewee import InternalError, MySQLDatabase, ProgrammingError


class Database:
    def __init__(self, host, port, user, password, database):
        self.db: MySQLDatabase = MySQLDatabase(database, user=user, password=password, host=host, port=port)

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
        except (ProgrammingError, InternalError) as e:
            return False, str(e)
        except Exception:
            return (
                False,
                "Failed to execute query due to unknown error. Please check the query and try again later.",
            )

    # Private helper methods
    def _run_sql(self, query: str, params=(), commit: bool = False, as_dict: bool = False) -> list[dict]:  # noqa: C901
        """
        Run sql query in database
        It supports multi-line SQL queries. Each SQL Query should be terminated with `;\n`

        Args:
        query: SQL query
        params: If you are using parameters in the query, you can pass them as a tuple
        commit: True if you want to commit the changes. If commit is false, it will rollback the changes and
                also wouldnt allow to run ddl, dcl or tcl queries
        as_dict: True if you want to return the result as a dictionary (like frappe.db.sql).
                 Otherwise it will return a dict of columns and data

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
            raise ProgrammingError("No query provided")

        # Start transaction
        self.db.begin()
        results = []
        with self.db.atomic() as transaction:
            try:
                for q in queries:
                    self.last_executed_query = q
                    if not commit and self._is_ddl_query(q):
                        raise ProgrammingError("Provided DDL query is not allowed in read only mode")
                    if self._is_dcl_query(q):
                        raise ProgrammingError("DCL query is not allowed to execute")
                    if self._is_tcl_query(q):
                        raise ProgrammingError("TCL query is not allowed to execute")
                    output = None
                    row_count = None
                    cursor = self.db.execute_sql(q, params)
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
