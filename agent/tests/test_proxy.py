import unittest
import os
from agent.proxy import Proxy
import json


def delete_safely(path: str):
    if os.path.exists(path):
        os.remove(path)


class ProxyTest(unittest.TestCase):
    """Tests for class methods of Proxy."""

    def _create_needed_files(self):
        try:
            os.makedirs(os.path.join(self.hosts_directory, self.domain_1))
            os.makedirs(os.path.join(self.hosts_directory, self.domain_2))
        except OSError or FileExistsError:
            pass

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
        # monkey patched methods
        self.original_init = Proxy.__init__
        self.original_config = Proxy.config

        self.default_domain = "xxx.frappe.cloud"
        self.domain_1 = "balu.codes"
        self.domain_2 = "www.balu.codes"

        self.hosts_directory = "test/nginx/hosts"
        self._create_needed_files()

    def test_hosts_has_redirect_if_default_domain_not_in_a_target(self):
        """
        Ensure hosts property redirects default domain.

        If a redirect.json doesn't have default domain in target, it means
        primary is not default domain and latter should be redirected to that
        target.
        """

        def __init__(self):
            pass
        Proxy.__init__ = __init__
        Proxy.config = {}
        proxy = Proxy()
        proxy.hosts_directory = self.hosts_directory
        proxy.config = {
            "domain": "frappe.cloud"
        }

        primary = self.domain_2
        # redirect.json without default domain as target
        with open(self.redirect_1, 'w') as fp:
            json.dump({self.domain_1: primary}, fp)

        self.assertDictContainsSubset(
            {
                self.default_domain: {'redirect': primary}
            },
            proxy.hosts,
            msg=f"Hosts value is \n{proxy.hosts}")

    def tearDown(self):
        Proxy.__init__ = self.original_init
        Proxy.config = self.original_config
        delete_safely(self.map_1)
        delete_safely(self.map_2)
        delete_safely(self.redirect_1)
        delete_safely(self.redirect_2)
        os.removedirs(os.path.join(self.hosts_directory, self.domain_1))
        os.removedirs(os.path.join(self.hosts_directory, self.domain_2))
