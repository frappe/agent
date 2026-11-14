from __future__ import annotations

import json
import unittest

from agent.database import Database
from agent.database_optimizer import DBExplain, DBOptimizer, DBTable, OptimizeDatabaseQueries
from agent.tests.test_database import DatabaseTestInstance


class TestDatabaseOptimizer(unittest.TestCase):
    def test_basic_index_existence_analysis(self):
        def possible_indexes(q):
            user = DBTable.from_frappe_output(USER_TABLE)
            has_role = DBTable.from_frappe_output(HAS_ROLE_TABLE)
            return [
                i.column
                for i in DBOptimizer(
                    query=q,
                    tables={"tabUser": user, "tabHas Role": has_role},
                ).potential_indexes()
            ]

        self.assertEqual(
            ["creation"],
            possible_indexes("select `name` from `tabUser` order by `creation` desc limit 1"),
        )

        self.assertEqual(
            ["full_name"],
            possible_indexes("select `name` from `tabUser` where full_name = 'xyz'"),
        )

        self.assertIn(
            "user",
            possible_indexes("select `name` from `tabUser` u join `tabHas Role` h on h.user = u.name"),
        )

    def test_suggestion_using_table_stats(self):
        user = DBTable.from_frappe_output(USER_TABLE)
        has_role = DBTable.from_frappe_output(HAS_ROLE_TABLE)

        tables = {"tabUser": user, "tabHas Role": has_role}
        self.assertEqual(user.total_rows, 92)

        # This should suggest adding api_key as it definitely has highest cardinality.
        optimizer = DBOptimizer(
            query="select name from tabUser where enabled = 1 and api_key = 'xyz'", tables=tables
        )
        self.assertIn("api_key", [i.column for i in optimizer.potential_indexes()])

        index = optimizer.suggest_index()
        self.assertEqual(index.column, "api_key")

        # This should suggest nothing as modified is already indexed
        optimizer = DBOptimizer(
            query="select name from tabUser order by modified asc",
            tables=tables,
        )
        self.assertIsNone(optimizer.suggest_index())

        # This should suggest nothing as modified is already indexed
        optimizer = DBOptimizer(
            query="select name from tabUser u join `tabHas Role` r on r.parent = u.name where r.role='System Manager'",  # noqa: E501
            tables=tables,
        )
        index = optimizer.suggest_index()
        self.assertEqual(index.column, "role")
        self.assertEqual(index.table, "tabHas Role")

    def test_complex_sub_query_aliases(self):
        """Check if table identification is correct for subqueries."""

        q = """SELECT *,
                    (SELECT COUNT(*) FROM `tabHD Ticket Comment` WHERE `tabHD Ticket Comment`.`reference_ticket`=`tabHD Ticket`.`name`) `count_comment`,
                    (SELECT COUNT(*) FROM `tabCommunication` WHERE `tabCommunication`.`reference_doctype`='HD Ticket' AND `tabCommunication`.`reference_name`=`tabHD Ticket`.`name`) `count_msg`,
                FROM `tabHD Ticket`
                WHERE `agent_group`='L2'
                ORDER BY `modified` DESC
                LIMIT 20
            """  # noqa: E501
        explain = [DBExplain.from_frappe_output(e) for e in json.loads(EXPLAIN_OUTPUT)]
        optimizer = DBOptimizer(query=q, explain_plan=explain)
        optimizer.update_table_data(DBTable.from_frappe_output(HD_TICKET_TABLE))
        optimizer.update_table_data(DBTable.from_frappe_output(HD_TICKET_COMMENT_TABLE))
        optimizer.update_table_data(DBTable.from_frappe_output(COMMUNICATION_TABLE))

        self.assertTrue(optimizer.can_be_optimized())
        index = optimizer.suggest_index()
        self.assertEqual(index.table, "tabHD Ticket Comment")
        self.assertEqual(index.column, "reference_ticket")


# Table stats extracted using describe-database-table for testing.

USER_TABLE = {
    "table_name": "tabUser",
    "total_rows": 92,
    "schema": [
        {
            "column": "name",
            "type": "varchar(140)",
            "is_nullable": False,
            "default": None,
            "cardinality": 91,
        },
        {"column": "creation", "type": "datetime(6)", "is_nullable": True, "default": None},
        {
            "column": "modified",
            "type": "datetime(6)",
            "is_nullable": True,
            "default": None,
            "cardinality": 91,
        },
        {
            "column": "modified_by",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
        },
        {"column": "owner", "type": "varchar(140)", "is_nullable": True, "default": None},
        {"column": "docstatus", "type": "int(1)", "is_nullable": False, "default": "0"},
        {"column": "idx", "type": "int(8)", "is_nullable": False, "default": "0"},
        {"column": "enabled", "type": "int(1)", "is_nullable": False, "default": "1"},
        {"column": "email", "type": "varchar(140)", "is_nullable": False, "default": ""},
        {
            "column": "first_name",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
            "cardinality": 88,
        },
        {
            "column": "reset_password_key",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
            "cardinality": 84,
        },
        {
            "column": "user_type",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": "System User",
            "cardinality": 2,
        },
        {
            "column": "api_key",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
            "cardinality": 70,
        },
        {"column": "api_secret", "type": "text", "is_nullable": True, "default": None},
        {"column": "_user_tags", "type": "text", "is_nullable": True, "default": None},
        {"column": "_comments", "type": "text", "is_nullable": True, "default": None},
        {"column": "_assign", "type": "text", "is_nullable": True, "default": None},
        {"column": "_liked_by", "type": "text", "is_nullable": True, "default": None},
    ],
    "indexes": [
        {
            "unique": True,
            "cardinality": 91,
            "name": "PRIMARY",
            "sequence": 1,
            "nullable": False,
            "column": "name",
            "type": "BTREE",
        },
        {
            "unique": True,
            "cardinality": 91,
            "name": "username",
            "sequence": 1,
            "nullable": True,
            "column": "username",
            "type": "BTREE",
        },
        {
            "unique": False,
            "cardinality": 91,
            "name": "modified",
            "sequence": 1,
            "nullable": True,
            "column": "modified",
            "type": "BTREE",
        },
        {
            "unique": False,
            "cardinality": 91,
            "name": "reset_password_key_index",
            "sequence": 1,
            "nullable": True,
            "column": "reset_password_key",
            "type": "BTREE",
        },
    ],
}


HAS_ROLE_TABLE = {
    "table_name": "tabHas Role",
    "total_rows": 96,
    "schema": [
        {
            "column": "name",
            "type": "varchar(140)",
            "is_nullable": "NO",
            "default": None,
            "cardinality": 92,
        },
        {"column": "creation", "type": "datetime(6)", "is_nullable": "YES", "default": None},
        {"column": "modified", "type": "datetime(6)", "is_nullable": "YES", "default": None},
        {
            "column": "modified_by",
            "type": "varchar(140)",
            "is_nullable": "YES",
            "default": None,
        },
        {"column": "owner", "type": "varchar(140)", "is_nullable": "YES", "default": None},
        {"column": "docstatus", "type": "int(1)", "is_nullable": "NO", "default": "0"},
        {"column": "idx", "type": "int(8)", "is_nullable": "NO", "default": "0"},
        {
            "column": "role",
            "type": "varchar(140)",
            "is_nullable": "YES",
            "default": None,
            "cardinality": 78,
        },
        {
            "column": "parent",
            "type": "varchar(140)",
            "is_nullable": "YES",
            "default": None,
            "cardinality": 92,
        },
        {
            "column": "parentfield",
            "type": "varchar(140)",
            "is_nullable": "YES",
            "default": None,
        },
        {
            "column": "parenttype",
            "type": "varchar(140)",
            "is_nullable": "YES",
            "default": None,
        },
    ],
    "indexes": [
        {
            "unique": True,
            "cardinality": 92,
            "name": "PRIMARY",
            "sequence": 1,
            "nullable": "",
            "column": "name",
            "type": "BTREE",
        },
        {
            "unique": False,
            "cardinality": 92,
            "name": "parent",
            "sequence": 1,
            "nullable": "YES",
            "column": "parent",
            "type": "BTREE",
        },
    ],
}


HD_TICKET_TABLE = {
    "table_name": "tabHD Ticket",
    "total_rows": 3820,
    "schema": [
        {
            "column": "name",
            "type": "bigint(20)",
            "is_nullable": False,
            "default": None,
            "cardinality": 3529,
        },
        {"column": "creation", "type": "datetime(6)", "is_nullable": True, "default": None},
        {
            "column": "modified",
            "type": "datetime(6)",
            "is_nullable": True,
            "default": None,
            "cardinality": 3529,
        },
        {
            "column": "modified_by",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
        },
        {"column": "owner", "type": "varchar(140)", "is_nullable": True, "default": None},
        {"column": "docstatus", "type": "int(1)", "is_nullable": False, "default": "0"},
        {"column": "idx", "type": "int(8)", "is_nullable": False, "default": "0"},
        {"column": "subject", "type": "varchar(140)", "is_nullable": True, "default": None},
        {"column": "raised_by", "type": "varchar(140)", "is_nullable": True, "default": None},
        {
            "column": "status",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": "Open",
            "cardinality": 8,
        },
        {"column": "priority", "type": "varchar(140)", "is_nullable": True, "default": None},
        {
            "column": "ticket_type",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "agent_group",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": "L1",
            "cardinality": 9,
        },
        {
            "column": "ticket_split_from",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
        },
        {"column": "description", "type": "longtext", "is_nullable": True, "default": None},
        {"column": "template", "type": "varchar(140)", "is_nullable": True, "default": None},
        {"column": "sla", "type": "varchar(140)", "is_nullable": True, "default": None},
        {
            "column": "response_by",
            "type": "datetime(6)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "response_by_variance",
            "type": "decimal(21,9)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "agreement_status",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "resolution_by",
            "type": "datetime(6)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "resolution_by_variance",
            "type": "decimal(21,9)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "service_level_agreement_creation",
            "type": "datetime(6)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "on_hold_since",
            "type": "datetime(6)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "total_hold_time",
            "type": "decimal(21,9)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "first_response_time",
            "type": "decimal(21,9)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "first_responded_on",
            "type": "datetime(6)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "avg_response_time",
            "type": "decimal(21,9)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "resolution_details",
            "type": "longtext",
            "is_nullable": True,
            "default": None,
        },
        {"column": "opening_date", "type": "date", "is_nullable": True, "default": None},
        {"column": "opening_time", "type": "time(6)", "is_nullable": True, "default": None},
        {
            "column": "resolution_date",
            "type": "datetime(6)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "resolution_time",
            "type": "decimal(21,9)",
            "is_nullable": True,
            "default": None,
        },
        {
            "column": "user_resolution_time",
            "type": "decimal(21,9)",
            "is_nullable": True,
            "default": None,
        },
        {"column": "contact", "type": "varchar(140)", "is_nullable": True, "default": None},
        {"column": "customer", "type": "varchar(140)", "is_nullable": True, "default": None},
        {
            "column": "email_account",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
        },
        {"column": "attachment", "type": "text", "is_nullable": True, "default": None},
        {"column": "_user_tags", "type": "text", "is_nullable": True, "default": None},
        {"column": "_comments", "type": "text", "is_nullable": True, "default": None},
        {"column": "_assign", "type": "text", "is_nullable": True, "default": None},
        {"column": "_liked_by", "type": "text", "is_nullable": True, "default": None},
        {"column": "_seen", "type": "text", "is_nullable": True, "default": None},
    ],
    "indexes": [
        {
            "unique": True,
            "cardinality": 3529,
            "name": "PRIMARY",
            "sequence": 1,
            "nullable": False,
            "column": "name",
            "type": "BTREE",
        },
        {
            "unique": False,
            "cardinality": 8,
            "name": "status",
            "sequence": 1,
            "nullable": True,
            "column": "status",
            "type": "BTREE",
        },
        {
            "unique": False,
            "cardinality": 3529,
            "name": "modified",
            "sequence": 1,
            "nullable": True,
            "column": "modified",
            "type": "BTREE",
        },
    ],
}


HD_TICKET_COMMENT_TABLE = {
    "table_name": "tabHD Ticket Comment",
    "total_rows": 2683,
    "schema": [
        {
            "column": "name",
            "type": "varchar(140)",
            "is_nullable": False,
            "default": None,
            "cardinality": 2683,
        },
        {"column": "creation", "type": "datetime(6)", "is_nullable": True, "default": None},
        {
            "column": "modified",
            "type": "datetime(6)",
            "is_nullable": True,
            "default": None,
            "cardinality": 2345,
        },
        {
            "column": "reference_ticket",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
            "cardinality": 1379,
        },
        {
            "column": "commented_by",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
        },
        {"column": "content", "type": "longtext", "is_nullable": True, "default": None},
        {"column": "is_pinned", "type": "int(1)", "is_nullable": False, "default": "0"},
    ],
    "indexes": [
        {
            "unique": True,
            "cardinality": 2345,
            "name": "PRIMARY",
            "sequence": 1,
            "nullable": False,
            "column": "name",
            "type": "BTREE",
        },
        {
            "unique": False,
            "cardinality": 2345,
            "name": "modified",
            "sequence": 1,
            "nullable": True,
            "column": "modified",
            "type": "BTREE",
        },
    ],
}


COMMUNICATION_TABLE = {
    "table_name": "tabCommunication",
    "total_rows": 20727,
    "schema": [
        {
            "column": "name",
            "type": "varchar(140)",
            "is_nullable": False,
            "default": None,
            "cardinality": 19713,
        },
        {"column": "creation", "type": "datetime(6)", "is_nullable": True, "default": None},
        {
            "column": "modified",
            "type": "datetime(6)",
            "is_nullable": True,
            "default": None,
            "cardinality": 19713,
        },
        {
            "column": "reference_doctype",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
            "cardinality": 1,
        },
        {
            "column": "reference_name",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
            "cardinality": 3798,
        },
        {
            "column": "reference_owner",
            "type": "varchar(140)",
            "is_nullable": True,
            "default": None,
            "cardinality": 1314,
        },
    ],
    "indexes": [
        {
            "unique": True,
            "cardinality": 19713,
            "name": "PRIMARY",
            "sequence": 1,
            "nullable": False,
            "column": "name",
            "type": "BTREE",
        },
        {
            "unique": False,
            "cardinality": 19713,
            "name": "modified",
            "sequence": 1,
            "nullable": True,
            "column": "modified",
            "type": "BTREE",
        },
        {
            "unique": False,
            "cardinality": 2,
            "name": "reference_doctype_reference_name_index",
            "sequence": 1,
            "nullable": True,
            "column": "reference_doctype",
            "type": "BTREE",
        },
        {
            "unique": False,
            "cardinality": 9856,
            "name": "reference_doctype_reference_name_index",
            "sequence": 2,
            "nullable": True,
            "column": "reference_name",
            "type": "BTREE",
        },
    ],
}


EXPLAIN_OUTPUT = """[
    {
        "Extra": "",
        "id": 1,
        "key": "modified",
        "key_len": "9",
        "possible_keys": null,
        "ref": null,
        "rows": "20",
        "select_type": "PRIMARY",
        "table": "tabHD Ticket",
        "type": "index"
    },
    {
        "Extra": "Using index condition; Using where",
        "id": 4,
        "key": "reference_doctype_reference_name_index",
        "key_len": "563",
        "possible_keys": "reference_doctype_reference_name_index",
        "ref": "const",
        "rows": "10236",
        "select_type": "DEPENDENT SUBQUERY",
        "table": "tabCommunication",
        "type": "ref"
    },
    {
        "Extra": "Using index condition; Using where",
        "id": 3,
        "key": "reference_doctype_reference_name_index",
        "key_len": "563",
        "possible_keys": "reference_doctype_reference_name_index",
        "ref": "const",
        "rows": "10236",
        "select_type": "DEPENDENT SUBQUERY",
        "table": "tabCommunication",
        "type": "ref"
    },
    {
        "Extra": "Using where; Using index",
        "id": 2,
        "key": "reference_ticket_index",
        "key_len": "563",
        "possible_keys": "reference_ticket_index",
        "ref": null,
        "rows": "2823",
        "select_type": "DEPENDENT SUBQUERY",
        "table": "tabHD Ticket Comment",
        "type": "index"
    }
]"""


class FakeSite:
    """The parts of `Site` that `OptimizeDatabaseQueries` uses.

    `describe_database_table` gives the same output as `bench describe-database-table`
    in Frappe v15. Like Frappe, it takes the name of a DocType, not of a table.
    """

    def __init__(self, instance: DatabaseTestInstance, database: str):
        self.instance = instance
        self.database = database
        self.analyze_calls = []
        self.described_doctypes = []

    def db_instance(self, username: str | None = None, password: str | None = None) -> Database:
        db = Database(
            self.instance.host,
            self.instance.port,
            username or "root",
            password or self.instance.db_root_password,
            self.database,
        )
        fetch = db.fetch_database_column_statistics

        def fetch_and_record(table, *args):
            self.analyze_calls.append((table, *args))
            return fetch(table, *args)

        db.fetch_database_column_statistics = fetch_and_record
        return db

    def describe_database_table(self, doctype, columns=None):
        self.described_doctypes.append(doctype)
        table = f"tab{doctype}"
        db = self.db_instance()
        exists = db._run_sql(
            f"SELECT table_rows FROM information_schema.tables"
            f" WHERE table_schema = '{self.database}' AND table_name = '{table}'",
            as_dict=True,
        )[0]["output"]
        if not exists:
            return None

        schema = [
            {
                "column": field["Field"],
                "type": field["Type"],
                "is_nullable": field["Null"] == "YES",
                "default": field["Default"],
            }
            for field in db._run_sql(f"DESCRIBE `{table}`", as_dict=True)[0]["output"]
        ]
        indexes = []
        for idx in db._run_sql(f"SHOW INDEX FROM `{table}`", as_dict=True)[0]["output"]:
            indexes.append(
                {
                    "unique": not idx["Non_unique"],
                    "cardinality": idx["Cardinality"],
                    "name": idx["Key_name"],
                    "sequence": idx["Seq_in_index"],
                    "nullable": idx["Null"] == "YES",
                    "column": idx["Column_name"],
                    "type": idx["Index_type"],
                }
            )
            if idx["Seq_in_index"] == 1:
                for column in schema:
                    if column["column"] == idx["Column_name"]:
                        column["cardinality"] = idx["Cardinality"]
        total_rows = exists[0]["table_rows"]
        return {"table_name": table, "total_rows": total_rows, "schema": schema, "indexes": indexes}


class TestOptimizeDatabaseQueries(unittest.TestCase):
    """Run the slow query analysis end to end against MariaDB."""

    @classmethod
    def setUpClass(cls):
        cls.instance = DatabaseTestInstance()
        cls.instance.create_database("site_db")
        cls.db = Database(
            cls.instance.host, cls.instance.port, "root", cls.instance.db_root_password, "site_db"
        )
        rows = ",\n".join(
            f"('{i}', 'Account {i % 50}', 'Company', '2026-01-{i % 28 + 1:02d}', {i}, {i % 2}, 'x', NOW())"
            for i in range(500)
        )
        cls.db._run_sql(
            f"""
            CREATE TABLE `tabGL Entry` (
                name varchar(140) PRIMARY KEY,
                account varchar(140),
                company varchar(140),
                posting_date date,
                debit decimal(21, 9),
                is_cancelled int,
                remarks text,
                modified datetime(6),
                INDEX posting_date (posting_date)
            );
            INSERT INTO `tabGL Entry` VALUES {rows};
            CREATE TABLE `tabToDo` (name varchar(140) PRIMARY KEY, status varchar(140));
            CREATE TABLE `tabBad'Name` (name varchar(140) PRIMARY KEY, status varchar(140));
            INSERT INTO `tabBad'Name` VALUES ('1', 'Open'), ('2', 'Closed');
            CREATE TABLE `not_a_doctype` (id int);
            ANALYZE TABLE `tabGL Entry`, `tabToDo`, `tabBad'Name`
            """,
            commit=True,
            allow_all_stmt_types=True,
        )

    @classmethod
    def tearDownClass(cls):
        cls.instance.destroy()

    def test_analyze_suggests_an_index_and_skips_the_queries_it_cannot_analyze(self):
        site = FakeSite(self.instance, "site_db")
        query = "select sum(`debit`) from `tabGL Entry` where `account` = 'Account 7' and `is_cancelled` = 0"
        indexed = "select `name` from `tabGL Entry` where `posting_date` = '2026-01-05'"
        truncated = "UPDATE `tabGL Entry` SET company='{\\'user\\': \\'a@b.c\\', # Time: 2611"
        not_a_doctype = "select * from not_a_doctype where id = 1"

        result = OptimizeDatabaseQueries(site, [truncated, not_a_doctype, indexed, query], "").analyze()

        self.assertEqual(list(result.keys()), [query])
        self.assertEqual([(i.table, i.column) for i in result[query]], [("tabGL Entry", "account")])
        # `describe-database-table` takes the name of the DocType
        self.assertEqual(site.described_doctypes, ["GL Entry"])

    def test_analyze_reads_each_column_once_and_only_the_columns_that_can_get_an_index(self):
        """ANALYZE reads the full table, so it must not run more than needed."""
        site = FakeSite(self.instance, "site_db")
        first = "select sum(`debit`) from `tabGL Entry` where `account` = 'Account 7' and `is_cancelled` = 0"
        second = (
            "select `name` from `tabGL Entry` where `account` = 'Account 3'"
            " and `company` = 'Company' and `remarks` = 'x'"
        )
        empty_table = "select `name` from `tabToDo` where `status` = 'Open'"

        result = OptimizeDatabaseQueries(site, [first, second, empty_table], "").analyze()

        self.assertEqual(list(result.keys()), [first, second])
        self.assertEqual(result[second][0].column, "account")
        # `remarks` is a text column, and `tabToDo` has no rows
        self.assertEqual(
            site.analyze_calls,
            [("tabGL Entry", ["account", "is_cancelled"]), ("tabGL Entry", ["company"])],
        )

    def test_analyze_skips_a_table_whose_name_is_not_safe_in_sql(self):
        site = FakeSite(self.instance, "site_db")
        query = "select `name` from `tabBad'Name` where `status` = 'Open'"

        result = OptimizeDatabaseQueries(site, [query], "").analyze()

        self.assertEqual(result, {})
        self.assertEqual(site.described_doctypes, [])
        self.assertEqual(site.analyze_calls, [])

    def test_column_statistics_of_all_columns(self):
        """The `Column Statistics` job of the database server gets all the columns."""
        rows = self.db.fetch_database_column_statistics("tabGL Entry")

        by_column = {row["column_name"]: row for row in rows}
        self.assertTrue({"account", "company", "posting_date", "is_cancelled"} <= set(by_column))
        self.assertEqual(by_column["account"]["avg_frequency"], 10.0)
        self.assertIn("histogram", by_column["account"])

    def test_column_statistics_refuse_a_name_that_is_not_safe_in_sql(self):
        with self.assertRaises(ValueError):
            self.db.fetch_database_column_statistics("tabGL Entry", ["account`) INDEXES (); SELECT (1"])
        with self.assertRaises(ValueError):
            self.db.fetch_database_column_statistics("tabBad'Name", ["status"])
