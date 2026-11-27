from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from jinja2 import Environment, PackageLoader

from agent.server import nginx_has_http2_fix


def nginx_says(stderr: str):
    return patch("agent.server.subprocess.run", return_value=Mock(stderr=stderr))


class TestNginxHttp2(unittest.TestCase):
    def test_http2_only_from_the_fixed_nginx_version(self):
        cases = {
            "nginx version: nginx/1.18.0 (Ubuntu)\n": False,
            "nginx version: nginx/1.31.8\n": False,
            "nginx version: nginx/1.31.9\n": True,
            "nginx version: nginx/1.32.0\n": True,
            "": False,
        }
        for stderr, expected in cases.items():
            with self.subTest(stderr=stderr), nginx_says(stderr):
                self.assertIs(nginx_has_http2_fix(), expected)

    def test_no_http2_without_nginx(self):
        with patch("agent.server.subprocess.run", side_effect=FileNotFoundError):
            self.assertFalse(nginx_has_http2_fix())

    def test_agent_vhost_enables_http2_only_when_allowed(self):
        template = Environment(loader=PackageLoader("agent", "templates")).get_template(
            "agent/nginx.conf.jinja2"
        )
        context = {
            "web_port": 25052,
            "name": "f1.example.com",
            "tls_directory": "/home/frappe/agent/tls",
            "nginx_directory": "/home/frappe/agent/nginx",
            "pages_directory": "/home/frappe/agent/repo/agent/pages",
        }
        self.assertIn("http2 on;", template.render(nginx_http2=True, **context))
        self.assertNotIn("http2 on;", template.render(nginx_http2=False, **context))
