import unittest
import os
from agent.proxy import Proxy
import json
import shutil


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

        # monkey patched methods
        self.original_init = Proxy.__init__
        self.original_config = Proxy.config

        self.default_domain = "xxx.frappe.cloud"
        self.domain_1 = "balu.codes"
        self.domain_2 = "www.balu.codes"
        self.tld = "frappe.cloud"

        self.hosts_directory = os.path.join(self.test_files_dir, "nginx/hosts")
        self._create_needed_files()

    def test_hosts_has_redirect_if_default_domain_not_in_a_target(self):
        """Ensure hosts property redirects default domain."""

        def __init__(self):
            pass
        Proxy.__init__ = __init__
        Proxy.config = {}
        proxy = Proxy()
        proxy.hosts_directory = self.hosts_directory
        proxy.config = {"domain": self.tld}

        os.makedirs(os.path.join(self.hosts_directory, self.default_domain))
        redirect_file = os.path.join(
            self.hosts_directory, self.default_domain, "redirect.json")
        with open(redirect_file, 'w') as fp:
            json.dump({self.default_domain: self.domain_1}, fp)

        self.assertLessEqual(
            {self.default_domain: {'redirect': self.domain_1}}.items(),
            proxy.hosts.items())

    def tearDown(self):
        Proxy.__init__ = self.original_init
        Proxy.config = self.original_config
        shutil.rmtree(self.test_files_dir)
