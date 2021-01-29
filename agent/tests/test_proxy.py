import json
import os
import shutil
import unittest
from unittest.mock import patch

from agent.proxy import Proxy


class TestProxy(unittest.TestCase):
    """Tests for class methods of Proxy."""

    def _create_needed_files(self):
        """Create host dirs for 2 domains test json files."""
        os.makedirs(os.path.join(self.hosts_directory, self.domain_1))
        os.makedirs(os.path.join(self.hosts_directory, self.domain_2))
        os.makedirs(self.upstreams_directory)

        map_1 = os.path.join(self.hosts_directory, self.domain_1, "map.json")
        map_2 = os.path.join(self.hosts_directory, self.domain_2, "map.json")

        with open(map_1, "w") as m:
            json.dump({self.domain_1: self.default_domain}, m)
        with open(map_2, "w") as m:
            json.dump({self.domain_2: self.default_domain}, m)

    def setUp(self):
        self.test_dir = "test_dir"
        if os.path.exists(self.test_dir):
            raise FileExistsError(
                f"""
                Directory {self.test_dir} exists.  This directory will be used
                for running tests and will be deleted
                """
            )

        self.default_domain = "xxx.frappe.cloud"
        self.domain_1 = "balu.codes"
        self.domain_2 = "www.balu.codes"
        self.tld = "frappe.cloud"

        self.hosts_directory = os.path.join(self.test_dir, "nginx/hosts")
        self.upstreams_directory = os.path.join(
            self.test_dir, "nginx/upstreams"
        )
        self._create_needed_files()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _get_fake_proxy(self):
        """Get Proxy object with only config and hosts_directory attrs."""
        with patch.object(Proxy, "__init__", new=lambda x: None):
            proxy = Proxy()
        proxy.hosts_directory = self.hosts_directory
        return proxy

    def test_hosts_redirects_default_domain(self):
        """
        Ensure hosts property redirects default domain when redirect.json is
        present.
        """
        proxy = self._get_fake_proxy()
        os.makedirs(os.path.join(self.hosts_directory, self.default_domain))
        redirect_file = os.path.join(
            self.hosts_directory, self.default_domain, "redirect.json"
        )
        with open(redirect_file, "w") as r:
            json.dump({self.default_domain: self.domain_1}, r)

        self.assertLessEqual(
            {self.default_domain: {"redirect": self.domain_1}}.items(),
            proxy.hosts.items(),
        )

    def _test_add_host(self, proxy, host):
        # TODO: test contents of map.json and certificate dirs
        with patch.object(Proxy, "add_host", new=Proxy.add_host.__wrapped__):
            # get undecorated method with __wrapped__
            proxy.add_host(host, "www.test.com", {})

        self.assertTrue(
            os.path.exists(
                os.path.join(proxy.hosts_directory, host, "map.json")
            )
        )

    def test_add_hosts_works_without_hosts_dir(self):
        """Ensure add_host works when hosts directory doesn't exist."""
        proxy = self._get_fake_proxy()
        shutil.rmtree(proxy.hosts_directory)
        self._test_add_host(proxy, "test.com")

    def test_add_hosts_works_with_hosts_dir(self):
        """Ensure add_host works when hosts directory exists."""
        proxy = self._get_fake_proxy()
        self._test_add_host(proxy, "test.com")

    def test_add_hosts_works_with_host_dir(self):
        """Ensure add_host works when host directory of host exists."""
        proxy = self._get_fake_proxy()
        host = "test.com"
        host_directory = os.path.join(proxy.hosts_directory, host)
        os.mkdir(host_directory)
        self._test_add_host(proxy, host)

    def _test_add_upstream(self, proxy, upstream):
        upstream_dir = os.path.join(proxy.upstreams_directory, upstream)
        with patch.object(
            Proxy, "add_upstream", new=Proxy.add_upstream.__wrapped__
        ):
            # get undecorated method with __wrapped__
            proxy.add_upstream(upstream)
        self.assertTrue(os.path.exists(upstream_dir))

    def test_add_upstream_works_with_upstreams_dir(self):
        """Ensure add_upstream works when upstreams directory exists."""
        proxy = self._get_fake_proxy()
        proxy.upstreams_directory = self.upstreams_directory
        self._test_add_upstream(proxy, "0.0.0.0")

    def test_add_upstream_works_without_upstreams_dir(self):
        """Ensure add_upstream works when upstreams directory doesn't exist."""
        proxy = self._get_fake_proxy()
        proxy.upstreams_directory = self.upstreams_directory
        os.rmdir(proxy.upstreams_directory)
        self._test_add_upstream(proxy, "0.0.0.0")

    def test_remove_redirect_for_default_domain_deletes_host_dir(self):
        """Ensure removing redirect of default domain deletes the host dir."""
        proxy = self._get_fake_proxy()
        proxy.domain = self.tld
        host_dir = os.path.join(self.hosts_directory, self.default_domain)
        os.makedirs(host_dir)
        redir_file = os.path.join(host_dir, "redirect.json")
        with open(redir_file, "w") as r:
            json.dump({self.default_domain: self.domain_1}, r)

        with patch.object(
            Proxy, "remove_redirect", new=Proxy.remove_redirect.__wrapped__
        ):
            proxy.remove_redirect(self.default_domain)
        self.assertFalse(os.path.exists(redir_file))
        self.assertFalse(os.path.exists(host_dir))

    def test_setup_redirect_creates_redirect_json_for_given_hosts(self):
        """Ensure setup redirect creates redirect.json files"""
        proxy = self._get_fake_proxy()
        proxy.domain = self.tld
        host = self.domain_2
        target = self.domain_1
        with patch.object(
            Proxy, "setup_redirect", new=Proxy.setup_redirect.__wrapped__
        ):
            proxy.setup_redirect(host, target)
        host_dir = os.path.join(proxy.hosts_directory, host)
        redir_file = os.path.join(host_dir, "redirect.json")
        self.assertTrue(os.path.exists(redir_file))

    def test_remove_redirect_deletes_redirect_json_for_given_hosts(self):
        """Ensure remove redirect deletes redirect.json files"""
        proxy = self._get_fake_proxy()
        proxy.domain = self.tld
        host = self.domain_2
        target = self.domain_1
        with patch.object(
            Proxy, "setup_redirect", new=Proxy.setup_redirect.__wrapped__
        ):
            proxy.setup_redirect(host, target)
            # assume that setup redirects works properly based on previous test
        with patch.object(
            Proxy, "remove_redirect", new=Proxy.remove_redirect.__wrapped__
        ):
            proxy.remove_redirect(host)
        host_dir = os.path.join(proxy.hosts_directory, host)
        redir_file = os.path.join(host_dir, "redirect.json")
        self.assertFalse(os.path.exists(redir_file))

    def test_rename_on_site_host_renames_host_directory(self):
        """Ensure rename site renames host directory."""
        proxy = self._get_fake_proxy()
        old_host_dir = os.path.join(proxy.hosts_directory, self.default_domain)
        os.makedirs(old_host_dir)
        with patch.object(
            Proxy,
            "rename_host_dir",
            new=Proxy.rename_host_dir.__wrapped__,
        ):
            proxy.rename_host_dir(self.default_domain, "yyy.frappe.cloud")
        new_host_dir = os.path.join(proxy.hosts_directory, "yyy.frappe.cloud")
        self.assertFalse(os.path.exists(old_host_dir))
        self.assertTrue(os.path.exists(new_host_dir))

    def test_rename_on_site_host_renames_redirect_json(self):
        """Ensure rename site updates redirect.json if exists."""
        proxy = self._get_fake_proxy()
        old_host_dir = os.path.join(proxy.hosts_directory, self.default_domain)
        os.makedirs(old_host_dir)
        redirect_file = os.path.join(old_host_dir, "redirect.json")
        with open(redirect_file, "w") as r:
            json.dump({self.default_domain: self.domain_1}, r)
        with patch.object(
            Proxy,
            "rename_host_dir",
            new=Proxy.rename_host_dir.__wrapped__,
        ):
            proxy.rename_host_dir(self.default_domain, "yyy.frappe.cloud")
        with patch.object(
            Proxy,
            "rename_site_in_host_dir",
            new=Proxy.rename_site_in_host_dir.__wrapped__,
        ):
            proxy.rename_site_in_host_dir(
                "yyy.frappe.cloud", self.default_domain, "yyy.frappe.cloud"
            )
        new_host_dir = os.path.join(proxy.hosts_directory, "yyy.frappe.cloud")
        redirect_file = os.path.join(new_host_dir, "redirect.json")
        with open(redirect_file) as r:
            self.assertDictEqual(
                json.load(r), {"yyy.frappe.cloud": self.domain_1}
            )

    def test_rename_updates_map_json_of_custom(self):
        """Ensure custom domains have map.json updated on site rename."""
        proxy = self._get_fake_proxy()
        with patch.object(
            Proxy,
            "rename_site_in_host_dir",
            new=Proxy.rename_site_in_host_dir.__wrapped__,
        ):
            proxy.rename_site_in_host_dir(
                self.domain_1, self.default_domain, "yyy.frappe.cloud"
            )
        host_directory = os.path.join(proxy.hosts_directory, self.domain_1)
        map_file = os.path.join(host_directory, "map.json")
        with open(map_file) as m:
            self.assertDictEqual(
                json.load(m), {self.domain_1: "yyy.frappe.cloud"}
            )

    def test_rename_updates_redirect_json_of_custom(self):
        """Ensure redirect.json updated for domains redirected to default."""
        proxy = self._get_fake_proxy()
        host_directory = os.path.join(proxy.hosts_directory, self.domain_1)
        redirect_file = os.path.join(host_directory, "redirect.json")
        with open(redirect_file, "w") as r:
            json.dump({self.domain_1: self.default_domain}, r)
        with patch.object(
            Proxy,
            "rename_site_in_host_dir",
            new=Proxy.rename_site_in_host_dir.__wrapped__,
        ):
            proxy.rename_site_in_host_dir(
                self.domain_1, self.default_domain, "yyy.frappe.cloud"
            )
        redirect_file = os.path.join(host_directory, "redirect.json")
        with open(redirect_file) as r:
            self.assertDictEqual(
                json.load(r), {self.domain_1: "yyy.frappe.cloud"}
            )

    def test_rename_does_not_update_redirect_json_of_custom(self):
        """Test redirects not updated for domains not redirected to default."""
        proxy = self._get_fake_proxy()
        host_directory = os.path.join(proxy.hosts_directory, self.domain_1)
        redirect_file = os.path.join(host_directory, "redirect.json")
        original_dict = {self.domain_1: self.domain_2}
        with open(redirect_file, "w") as r:
            json.dump(original_dict, r)
        with patch.object(
            Proxy,
            "rename_site_in_host_dir",
            new=Proxy.rename_site_in_host_dir.__wrapped__,
        ):
            proxy.rename_site_in_host_dir(
                self.domain_1, self.default_domain, "yyy.frappe.cloud"
            )
        with open(redirect_file) as r:
            self.assertDictEqual(json.load(r), original_dict)
