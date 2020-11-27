import json
import os
import shutil
import unittest
from unittest.mock import patch

from agent.proxy import Proxy


class TestProxy(unittest.TestCase):
    """Tests for class methods of Proxy."""

    def _create_needed_files(self):
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
        with patch.object(Proxy, "add_host", new=Proxy.add_host.__wrapped__):
            # get undecorated method with __wrapped__
            proxy.add_host(host, "www.test.com", {})

        self.assertTrue(
            os.path.exists(
                os.path.join(proxy.hosts_directory, host, "map.json")
            )
        )
        # TODO: test contents of map.json and certificate dirs <13-11-20, Balamurali M> #

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
        host_dir = os.path.join(self.hosts_directory, self.default_domain)
        os.makedirs(host_dir)
        redir_file = os.path.join(host_dir, "redirect.json")
        with open(redir_file, "w") as r:
            json.dump({self.default_domain: self.domain_1}, r)

        proxy.domain = self.tld
        with patch.object(
            Proxy, "remove_redirects", new=Proxy.remove_redirects.__wrapped__
        ):
            proxy.remove_redirects([self.default_domain])
        self.assertFalse(os.path.exists(redir_file))
        self.assertFalse(os.path.exists(host_dir))

    def test_setup_redirects_creates_redirect_json_for_all_hosts(self):
        """Ensure setup redirect creates redirect.json files"""
        proxy = self._get_fake_proxy()
        proxy.domain = self.tld
        hosts = [self.default_domain, self.domain_2]
        target = self.domain_1
        with patch.object(
            Proxy, "setup_redirects", new=Proxy.setup_redirects.__wrapped__
        ):
            proxy.setup_redirects(hosts, target)
        for host in hosts:
            host_dir = os.path.join(proxy.hosts_directory, host)
            redir_file = os.path.join(host_dir, "redirect.json")
            self.assertTrue(os.path.exists(redir_file))

    def test_remove_redirects_creates_redirect_json_for_all_hosts(self):
        """Ensure remove redirect deletes redirect.json files"""
        proxy = self._get_fake_proxy()
        proxy.domain = self.tld
        hosts = [self.default_domain, self.domain_2]
        target = self.domain_1
        with patch.object(
            Proxy, "setup_redirects", new=Proxy.setup_redirects.__wrapped__
        ):
            proxy.setup_redirects(hosts, target)
            # assume that setup redirects works properly based on previous test
        with patch.object(
            Proxy, "remove_redirects", new=Proxy.remove_redirects.__wrapped__
        ):
            proxy.remove_redirects(hosts)
        for host in hosts:
            host_dir = os.path.join(proxy.hosts_directory, host)
            redir_file = os.path.join(host_dir, "redirect.json")
            self.assertFalse(os.path.exists(redir_file))
