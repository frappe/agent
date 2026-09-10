from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from agent.server import Server


class TestServerNginxAccess(unittest.TestCase):
    """The IP access list only means something with the proxy IP beside it."""

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.nginx_directory = os.path.join(self.directory, "nginx")
        os.makedirs(self.nginx_directory)

        with open(os.path.join(self.directory, "config.json"), "w") as f:
            json.dump(
                {
                    "name": "u326-mumbai.frappe.cloud",
                    "benches_directory": os.path.join(self.directory, "benches"),
                    "nginx_directory": self.nginx_directory,
                },
                f,
            )

        self.server = Server(self.directory)

    def tearDown(self):
        shutil.rmtree(self.directory)

    def update_config_ip(self, ip_accept, ip_drop, proxy_ip):
        """Call past the @step decorator, which needs a job record."""
        with patch.object(Server, "update_config_ip", new=Server.update_config_ip.__wrapped__):
            self.server.update_config_ip(ip_accept, ip_drop, proxy_ip)

    def rendered_nginx_config(self):
        self.server._generate_nginx_config()
        with open(os.path.join(self.nginx_directory, "nginx.conf")) as f:
            return f.read()

    def test_update_config_ip_stores_the_proxy_ip_with_the_access_lists(self):
        self.update_config_ip(["183.82.5.84/32"], ["0.0.0.0/0"], "10.3.1.0/24")

        config = self.server.config
        self.assertEqual(config["ip_accept"], ["183.82.5.84/32"])
        self.assertEqual(config["ip_drop"], ["0.0.0.0/0"])
        self.assertEqual(config["proxy_ip"], "10.3.1.0/24")

    def test_update_config_ip_keeps_the_stored_proxy_ip_when_none_is_sent(self):
        """An older Press sends no proxy IP, and dropping it would disarm the rules."""
        self.update_config_ip([], [], "10.3.1.0/24")
        self.update_config_ip(["183.82.5.84/32"], ["0.0.0.0/0"], None)

        self.assertEqual(self.server.config["proxy_ip"], "10.3.1.0/24")

    def test_nginx_reads_the_visitor_ip_off_the_header_when_the_proxy_ip_is_stored(self):
        self.update_config_ip(["183.82.5.84/32"], ["0.0.0.0/0"], "10.3.1.0/24")

        config = self.rendered_nginx_config()

        self.assertIn("real_ip_header X-Real-IP;", config)
        self.assertIn("set_real_ip_from 10.3.1.0/24;", config)
        self.assertIn("allow 183.82.5.84/32;", config)
        self.assertIn("deny 0.0.0.0/0;", config)

    def test_nginx_matches_the_proxy_instead_of_the_visitor_without_a_proxy_ip(self):
        """The state that let every visitor past the deny rules on u326-mumbai."""
        self.update_config_ip(["183.82.5.84/32"], ["0.0.0.0/0"], None)

        config = self.rendered_nginx_config()

        self.assertNotIn("real_ip_header", config)
        self.assertNotIn("set_real_ip_from", config)
