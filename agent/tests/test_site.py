import json
import os
import shutil
import unittest
from unittest.mock import patch

from agent.site import Site
from agent.bench import Bench
from agent.server import Server


class TestSite(unittest.TestCase):
    """Tests for class methods of Site."""

    def _create_needed_paths(self):
        os.makedirs(self.sites_directory)
        os.makedirs(self.apps_directory)
        with open(self.common_site_config, "w") as c:
            json.dump({}, c)

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
        self.common_site_config = os.path.join(
            self.sites_directory, "common_site_config.json"
        )

        self._create_needed_paths()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _create_test_site(self, site_name: str):
        site_dir = os.path.join(self.sites_directory, site_name)
        os.makedirs(site_dir)
        site_config = os.path.join(site_dir, "site_config.json")
        with open(site_config, "w") as s:
            json.dump({}, s)

    def _get_test_bench(self):
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

        self.assertTrue(
            os.path.exists(os.path.join(self.sites_directory, new_name))
        )
        self.assertFalse(
            os.path.exists(os.path.join(self.sites_directory, old_name))
        )
