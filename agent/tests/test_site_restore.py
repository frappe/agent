from __future__ import annotations

import gzip
import json
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from agent.site import Site
from agent.tests.test_database import DatabaseTestInstance

DB_NAME = "restore_db"
DB_PASSWORD = "restore_password"

# What mariadb-dump writes for a Frappe JSON field holding '', which json_valid rejects
INVALID_JSON_DUMP = """
DROP TABLE IF EXISTS `tabEncounter`;
CREATE TABLE `tabEncounter` (
  `name` varchar(140) NOT NULL,
  `custom_post` longtext DEFAULT NULL CHECK (json_valid(`custom_post`)),
  PRIMARY KEY (`name`)
);
INSERT INTO `tabEncounter` VALUES ('ENC-1','');
"""


class TestRestoreSiteTables(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = DatabaseTestInstance()
        cls.db.create_database(DB_NAME)
        cls.db.create_database_user(DB_NAME, DB_NAME, DB_PASSWORD)

    @classmethod
    def tearDownClass(cls):
        cls.db.destroy()

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.site = self._create_site()
        os.makedirs(self.site.backup_directory)
        with gzip.open(os.path.join(self.site.backup_directory, "tabEncounter.sql.gz"), "wt") as f:
            f.write(INVALID_JSON_DUMP)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _create_site(self) -> Site:
        site_directory = os.path.join(self.test_dir, "sites", "site.test")
        os.makedirs(site_directory)
        config = {
            "db_name": DB_NAME,
            "db_password": DB_PASSWORD,
            "db_host": "127.0.0.1",
            "db_port": self.db.port,
        }
        with open(os.path.join(site_directory, "site_config.json"), "w") as f:
            json.dump(config, f)
        bench = SimpleNamespace(
            sites_directory=os.path.dirname(site_directory),
            host="127.0.0.1",
            db_port=self.db.port,
            server=SimpleNamespace(job_record=None),
        )
        return Site("site.test", bench)

    def _restored_rows(self) -> str:
        query = f"SELECT name, QUOTE(custom_post) FROM {DB_NAME}.tabEncounter"
        root_password = self.db.db_root_password
        output = self.db.execute_cmd(f'mysql -h 127.0.0.1 -uroot -p{root_password} -sN -e "{query}"')
        return output.strip()

    def test_restore_site_tables_restores_rows_that_fail_json_check_constraint(self):
        Site.restore_site_tables.__wrapped__(self.site)
        self.assertEqual(self._restored_rows(), "ENC-1\t''")

    def test_restore_touched_tables_restores_rows_that_fail_json_check_constraint(self):
        with patch.object(Site, "tables_to_restore", ["tabEncounter"]), patch.object(
            Site, "drop_new_tables", return_value={}
        ):
            self.site._restore_touched_tables()
        self.assertEqual(self._restored_rows(), "ENC-1\t''")
