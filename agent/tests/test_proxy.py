import json
import os
import shutil
import unittest
from unittest.mock import patch

from agent.proxy import Proxy


class ProxyTest(unittest.TestCase):
    """Tests for class methods of Proxy."""

    def _create_needed_files(self):
        os.makedirs(os.path.join(self.hosts_directory, self.domain_1))
        os.makedirs(os.path.join(self.hosts_directory, self.domain_2))

        self.redirect_1 = os.path.join(
            self.hosts_directory, self.domain_1, "redirect.json")
        self.redirect_2 = os.path.join(
            self.hosts_directory, self.domain_2, "redirect.json")

        self.map_1 = os.path.join(
            self.hosts_directory, self.domain_1, "map.json")
        self.map_2 = os.path.join(
            self.hosts_directory, self.domain_2, "map.json")

        with open(self.map_1, 'w') as fp:
            json.dump({self.domain_1: self.default_domain}, fp)
        with open(self.map_2, 'w') as fp:
            json.dump({self.domain_2: self.default_domain}, fp)

    def setUp(self):
        self.test_files_dir = "test_files"
        if os.path.exists(self.test_files_dir):
            raise FileExistsError(f"""Directory {self.test_files_dir} exists.
                                  This directory will be used for running tests
                                  and will be deleted""")

        self.default_domain = "xxx.frappe.cloud"
        self.domain_1 = "balu.codes"
        self.domain_2 = "www.balu.codes"
        self.tld = "frappe.cloud"

        self.hosts_directory = os.path.join(self.test_files_dir, "nginx/hosts")
        self._create_needed_files()

    def _get_fake_proxy(self):
        """Get Proxy object with only config and hosts_directory attrs."""
        config = {"domain": self.tld}
        with patch.object(Proxy, '__init__', new=lambda x: None), \
                patch.object(Proxy, 'config', new=lambda: config):
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
            self.hosts_directory, self.default_domain, "redirect.json")
        with open(redirect_file, 'w') as fp:
            json.dump({self.default_domain: self.domain_1}, fp)

        self.assertLessEqual(
            {self.default_domain: {'redirect': self.domain_1}}.items(),
            proxy.hosts.items())

    def _test_add_host(self, proxy, host):
        with patch.object(Proxy, 'add_host', new=Proxy.add_host.__wrapped__):
            # get undecorated method with __wrapped__
            proxy.add_host(host, "www.test.com", {})

        self.assertTrue(os.path.exists(os.path.join(
            proxy.hosts_directory, host, "map.json")))
        # TODO: test contents of map.json and certificate dirs <13-11-20, Balamurali M> #

    def test_add_hosts_works_without_hosts_dir(self):
        """Ensure add_host works when hosts directory doesn't exist"""
        proxy = self._get_fake_proxy()
        shutil.rmtree(proxy.hosts_directory)
        host = "test.com"
        self._test_add_host(proxy, host)

    def test_add_hosts_works_with_hosts_dir(self):
        """Ensure add_host works when hosts directory exists"""
        proxy = self._get_fake_proxy()
        host = "test.com"
        self._test_add_host(proxy, host)

    def test_add_hosts_works_with_host_dir(self):
        """Ensure add_host works when host directory of host exists"""
        proxy = self._get_fake_proxy()
        host = "test.com"
        host_directory = os.path.join(proxy.hosts_directory, host)
        os.mkdir(host_directory)
        self._test_add_host(proxy, host)

    def tearDown(self):
        shutil.rmtree(self.test_files_dir)
