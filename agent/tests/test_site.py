from __future__ import annotations

import json
import os
import shutil
import unittest
from unittest.mock import patch

from agent.base import AgentException
from agent.bench import Bench
from agent.server import Server
from agent.site import Site


class TestSite(unittest.TestCase):
    """Tests for class methods of Site."""

    def _create_needed_paths(self):
        os.makedirs(self.sites_directory)
        os.makedirs(self.apps_directory)
        os.makedirs(self.assets_dir)
        with open(self.common_site_config, "w") as c:
            json.dump({}, c)
        with open(self.bench_config, "w") as c:
            json.dump({"docker_image": "fake_img_url"}, c)
        with open(self.apps_txt, "w") as a:
            a.write("frappe\n")
            a.write("erpnext\n")

    def _make_site_config(self, site_name: str, content: str | None = None):
        if not content:
            content = json.dumps({"db_name": "fake_db_name", "db_password": "fake_db_password"})
        site_config = os.path.join(self.sites_directory, site_name, "site_config.json")
        with open(site_config, "w") as s:
            s.write(content)

    def setUp(self):
        self.test_dir = "test_dir"
        if os.path.exists(self.test_dir):
            raise FileExistsError(
                f"""
                Directory {self.test_dir} exists. This directory will be used
                for running tests and will be deleted.
                """
            )

        self.bench_name = "test-bench"
        self.benches_directory = os.path.join(self.test_dir, "benches")
        self.bench_dir = os.path.join(self.benches_directory, self.bench_name)

        self.sites_directory = os.path.join(self.bench_dir, "sites")
        self.apps_directory = os.path.join(self.bench_dir, "apps")
        self.common_site_config = os.path.join(self.sites_directory, "common_site_config.json")
        self.bench_config = os.path.join(self.bench_dir, "config.json")
        self.apps_txt = os.path.join(self.sites_directory, "apps.txt")
        self.assets_dir = os.path.join(self.sites_directory, "assets")

        self._create_needed_paths()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _create_test_site(self, site_name: str):
        site_dir = os.path.join(self.sites_directory, site_name)
        os.makedirs(site_dir)
        site_config = os.path.join(site_dir, "site_config.json")
        with open(site_config, "w") as s:
            json.dump({}, s)

    def _get_test_bench(self) -> Bench:
        with patch.object(Server, "__init__", new=lambda x: None):
            server = Server()
        server.benches_directory = self.benches_directory
        return Bench(self.bench_name, server)

    def test_rename_site_works_for_existing_site(self):
        """Ensure rename_site renames site."""
        bench = self._get_test_bench()
        old_name = "old-site-name"
        self._create_test_site(old_name)
        with patch.object(Site, "config"):
            site = Site(old_name, bench)
        new_name = "new-site-name"

        with patch.object(Site, "rename", new=Site.rename.__wrapped__):
            site.rename(new_name)

        self.assertTrue(os.path.exists(os.path.join(self.sites_directory, new_name)))
        self.assertFalse(os.path.exists(os.path.join(self.sites_directory, old_name)))

    def test_valid_sites_property_of_bench_throws_if_site_config_is_corrupt(
        self,
    ):
        bench = self._get_test_bench()
        site_name = "corrupt-site.frappe.cloud"
        self._create_test_site(site_name)
        self._make_site_config(
            site_name,
            content="""
{
   "db_name": "fake_db_name",
   "db_password": "fake_db_password"
   "some_key": "some_value"
}
""",  # missing comma above
        )
        with self.assertRaises(AgentException):
            bench._sites(validate_configs=True)

    def test_valid_sites_property_of_bench_doesnt_throw_for_assets_apps_txt(
        self,
    ):
        bench = self._get_test_bench()
        site_name = "corrupt-site.frappe.cloud"
        self._create_test_site(site_name)
        self._make_site_config(site_name)
        try:
            bench._sites()
        except AgentException:
            self.fail("sites property of bench threw error for assets and apps.txt")
        self.assertEqual(len(bench.sites), 1)
        try:
            bench.valid_sites[site_name]
        except KeyError:
            self.fail("Site not found in bench.sites")
