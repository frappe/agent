from __future__ import annotations

import re

from agent.base import Base
from agent.database import Database
from agent.job import Job, Step, job, step
from agent.server import Server


class SQLRunner(Base):
    def __init__(
        self,
        database: str,
        db_host: str,
        db_port: int,
        queries: list[str],
        db_user: str | None = None,
        db_password: str | None = None,
        site: str | None = None,
        bench: str | None = None,
        read_only: bool = False,
        continue_on_error: bool = False,
    ):
        super().__init__()

        self.db_host = db_host
        self.db_port = db_port
        self.database = database
        self.db_user = db_user
        self.db_password = db_password
        self.site = site
        self.bench = bench
        self.read_only = read_only
        self.continue_on_error = continue_on_error

        # In case press hasn't provided the db_user and db_password, we will use the site and bench
        # So, ensure that site and bench are provided
        if (db_user is None or db_password is None) and (site is None or bench is None):
            raise Exception("site and bench are required if db_user or db_password is not provided")

        # Validate bench and site
        if site or bench:
            bench_obj = Server().get_bench(bench)
            site_obj = bench_obj.get_site(site)
            # If db_user or db_password is not provided,
            # Assume we need to use the site's database credentials
            if db_user is None or db_password is None:
                self.db_user = site_obj.user
                self.db_password = site_obj.password

        self.job = None
        self.step = None

        self.queries: list[SQLQuery] = [SQLQuery(query) for query in queries]

    @property
    def job_record(self):
        if self.job is None:
            self.job = Job()
        return self.job

    @property
    def step_record(self):
        if self.step is None:
            self.step = Step()
        return self.step

    @step_record.setter
    def step_record(self, value):
        self.step = value

    @job("Run SQL Queries")
    def run_sql_queries_job(self):
        return self.run_sql_queries_step()

    @step("Run SQL Queries")
    def run_sql_queries_step(self):
        return self.run_sql_queries()

    def run_sql_queries(self) -> list[dict]:
        database = Database(self.db_host, self.db_port, self.db_user, self.db_password, self.database)
        data = database._run_sql_v2(
            self.queries, commit=(not self.read_only), continue_on_error=self.continue_on_error
        )
        return [q.to_json() for q in data]


class SQLQuery:
    def __init__(self, raw_query: str):
        """
        Parse the SQL Queries to get the metadata

        Expected format from press:
        <query> /* <type>_<id> */
        """
        pattern = re.compile(r"\/\*(.*?)\*\/")
        try:
            metadata = pattern.search(raw_query).group(1).strip().split("_", 1)
        except Exception as e:
            raise Exception("Invalid SQL Query Format") from e

        self.id = metadata[1]
        self.type = metadata[0]
        self.query = raw_query
        self.columns = []
        self.data = []
        self.row_count = 0
        self.error_code = ""
        self.error_message = ""
        self.duration = 0.0
        self.success = False

    def to_json(self):
        # TODO: handle decimal kind of stuffs
        return {
            "id": self.id,
            "type": self.type,
            "query": self.query,
            "columns": self.columns,
            "data": self.data,
            "row_count": self.row_count,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "duration": self.duration,
            "success": self.success,
        }
