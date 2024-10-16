from __future__ import annotations
from peewee import MySQLDatabase, ProgrammingError, InternalError

class Database:
    def __init__(self, host, port, user, password, database):
        self.db: 'MySQLDatabase' = MySQLDatabase(
            database,
            user=user,
            password=password,
            host=host,
            port=port,
            autocommit=False,
        )

    # Methods
    def execute_query(self, query:str, commit:bool=False, as_dict:bool=False) -> list[bool, str]:
        """
        This function will take the query and run in database.

        It will return a tuple of (bool, str)
        bool: Whether the query has been executed successfully
        str: The output of the query. It can be the output or error message as well
        """
        try:
            return True, self._sql(query, commit=commit, as_dict=as_dict)
        except (ProgrammingError, InternalError) as e:
            return False, "Error while executing query: " + str(e)
        except Exception as e:
            return False, "Failed to execute query. Please check the query and try again later."

    # Private helper methods
    def _sql(self, query:str, params=(), commit:bool=False, as_dict:bool=False) -> dict|None:
        """
        Run sql query in database

        Args:
        query: SQL query
        params: If you are using parameters in the query, you can pass them as a tuple
        commit: True if you want to commit the changes. If commit is false, it will rollback the changes and also wouldnt allow to run ddl, dcl or tcl queries
        as_dict: True if you want to return the result as a dictionary (like frappe.db.sql, get the results as dict). Otherwise it will return a dict of columns and data

        Return Format:
        For as_dict = True:
        [
            {
                "name" : "Administrator",
                "modified": "2019-01-01 00:00:00",
            },
            ...
        ]

        For as_dict = False:
        {
            "columns": ["name", "modified"],
            "data": [
                ["Administrator", "2019-01-01 00:00:00"],
                ...
            ]
        }
        """

        query = query.strip()
        if not commit and self._is_restricted_query_for_no_commit_mode(query):
            raise ProgrammingError("Provided query is not allowed in read only mode")

        # Start transaction
        self.db.begin()
        result = None
        try:
            cursor = self.db.execute_sql(query, params)
            if cursor.description:
                rows = cursor.fetchall()
                columns = [d[0] for d in cursor.description]
                if as_dict:
                    result = list(map(lambda x: dict(zip(columns, x)), rows))
                else:
                    result = {
                        "columns": columns,
                        "data": rows
                    }
        except:
            # if query execution fails, rollback the transaction and raise the error
            self.db.rollback()
            raise
        else:
            if commit:
                # If commit is True, try to commit the transaction
                try:
                    self.db.commit()
                except:
                    self.db.rollback()
                    raise
            else:
                # If commit is False, rollback the transaction to discard the changes
                self.db.rollback()
        return result

    def _is_restricted_query_for_no_commit_mode(self, query:str) -> bool:
        return self._is_ddl_query(query) or self._is_dcl_query(query) or self._is_tcl_query(query)

    def _is_ddl_query(self, query:str) -> bool:
        return query.upper().startswith(("CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "COMMENT"))

    def _is_dcl_query(self, query:str) -> bool:
        return query.upper().startswith(("GRANT", "REVOKE"))
    
    def _is_tcl_query(self, query:str) -> bool:
        return query.upper().replace(" ", "").startswith(("COMMIT", "ROLLBACK", "SAVEPOINT", "BEGINTRANSACTION"))
