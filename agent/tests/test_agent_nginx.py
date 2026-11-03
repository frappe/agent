from __future__ import annotations

import re
import unittest

from jinja2 import Environment, PackageLoader


def render_agent_nginx(name: str) -> str:
    template = Environment(loader=PackageLoader("agent", "templates")).get_template("agent/nginx.conf.jinja2")
    return template.render(
        web_port=25052,
        name=name,
        tls_directory="/home/frappe/agent/tls",
        nginx_directory="/home/frappe/agent/nginx",
        pages_directory="/home/frappe/agent/repo/agent/pages",
    )


class TestAgentNginx(unittest.TestCase):
    def test_proxy_relay_forwards_the_query_string_to_the_upstream_agent(self):
        rendered = render_agent_nginx("n2-ksa.frappe.cloud")
        relay = re.search(r"proxy_pass https://\$upstream_host/(\S+);", rendered)
        self.assertIsNotNone(relay)
        self.assertEqual(relay.group(1), "$upstream_path$is_args$args")
