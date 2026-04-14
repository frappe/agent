from __future__ import annotations

import json
import os
import shutil
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from agent.base import AgentException
from agent.bench import Bench, _get_cors_origins, _normalize_cors_origins
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

    def _render_bench_nginx(self, cors_origins, standalone=False):
        output_file = os.path.join(self.test_dir, "nginx.conf")
        with patch.object(Server, "__init__", new=lambda x: None):
            server = Server()

        context = {
            "bench_name": self.bench_name,
            "bench_name_slug": self.bench_name.replace("-", "_"),
            "domain": "example.com",
            "sites": [SimpleNamespace(name="site.test", host="site.test")],
            "domains": {},
            "http_timeout": 120,
            "web_port": 8000,
            "socketio_port": 9000,
            "sites_directory": self.sites_directory,
            "standalone": standalone,
            "error_pages_directory": os.path.join(self.test_dir, "errors"),
            "nginx_directory": os.path.join(self.test_dir, "nginx"),
            "tls_protocols": None,
            "code_server": {},
            "cors_origins": cors_origins,
        }
        server._render_template("bench/nginx.conf.jinja2", context, output_file)

        with open(output_file) as f:
            return f.read()

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

    @unittest.skip("fails with 'Server' has no attr 'job'")
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

    def test_get_cors_origins_normalizes_string_defaults_and_overrides(self):
        sites = [
            SimpleNamespace(name="site-a.test", config={"domains": ["cdn.site-a.test"]}),
            SimpleNamespace(
                name="site-b.test",
                config={"allow_cors": "https://portal.example.com"},
            ),
            SimpleNamespace(name="site-c.test", config={"allow_cors": []}),
        ]

        self.assertCountEqual(
            _get_cors_origins(sites, "https://bench.example.com"),
            [
                ('"site-a.test:https://bench.example.com"', "$http_origin"),
                ('"cdn.site-a.test:https://bench.example.com"', "$http_origin"),
                ('"site-b.test:https://portal.example.com"', "$http_origin"),
            ],
        )

    def test_get_cors_origins_supports_wildcard_values(self):
        sites = [
            SimpleNamespace(
                name="site.test",
                config={"domains": ["cdn.site.test"], "allow_cors": "*"},
            )
        ]

        self.assertCountEqual(
            _get_cors_origins(sites),
            [
                (r"~^site\.test:.*$", '"*"'),
                (r"~^cdn\.site\.test:.*$", '"*"'),
            ],
        )

    def test_rendered_bench_nginx_adds_cors_headers_to_assets(self):
        rendered = self._render_bench_nginx(
            _get_cors_origins(
                [
                    SimpleNamespace(
                        name="site.test",
                        config={"allow_cors": "https://portal.example.com"},
                    )
                ]
            )
        )

        self.assertRegex(
            rendered,
            r"location /assets \{\s+try_files \$uri =404;\s+"
            r"add_header Cache-Control \"max-age=31536000, immutable\";\s+"
            r"add_header Access-Control-Allow-Origin \$cors_origin_test_bench always;\s+"
            r"add_header Access-Control-Allow-Methods \$cors_methods_test_bench always;\s+"
            r"add_header Vary \$cors_vary_test_bench always;\s+"
            r"if \(\$request_method = OPTIONS\) \{\s+"
            r"add_header Access-Control-Allow-Origin \$cors_origin_test_bench always;\s+"
            r"add_header Access-Control-Allow-Methods \$cors_methods_test_bench always;\s+"
            r"add_header Access-Control-Max-Age 86400 always;\s+"
            r"add_header Vary \$cors_vary_test_bench always;\s+"
            r"return 204;",
        )

    def test_rendered_standalone_bench_nginx_adds_cors_headers_to_assets(self):
        rendered = self._render_bench_nginx(
            _get_cors_origins(
                [
                    SimpleNamespace(
                        name="site.test",
                        config={"allow_cors": "*"},
                    )
                ]
            ),
            standalone=True,
        )

        self.assertIn('~^site\\.test:.*$ "*";', rendered)
        self.assertIn(
            (
                "map $cors_origin_test_bench $cors_vary_test_bench {\n"
                '\t"" "";\n\t"*" "";\n\tdefault "Origin";\n}'
            ),
            rendered,
        )
        self.assertRegex(
            rendered,
            r"location /assets \{\s+try_files \$uri =404;\s+"
            r"add_header Cache-Control \"max-age=31536000, immutable\";\s+"
            r"add_header Access-Control-Allow-Origin \$cors_origin_test_bench always;\s+"
            r"add_header Access-Control-Allow-Methods \$cors_methods_test_bench always;\s+"
            r"add_header Vary \$cors_vary_test_bench always;\s+"
            r"if \(\$request_method = OPTIONS\) \{\s+"
            r"add_header Access-Control-Allow-Origin \$cors_origin_test_bench always;\s+"
            r"add_header Access-Control-Allow-Methods \$cors_methods_test_bench always;\s+"
            r"add_header Access-Control-Max-Age 86400 always;\s+"
            r"add_header Vary \$cors_vary_test_bench always;\s+"
            r"return 204;",
        )

    def test_rendered_bench_nginx_omits_cors_when_no_origins(self):
        rendered = self._render_bench_nginx([])

        self.assertNotIn("cors_origin_test_bench", rendered)
        self.assertNotIn("Access-Control-Allow-Origin", rendered)
        self.assertNotIn("$request_method = OPTIONS", rendered)

    def test_normalize_cors_origins_strips_embedded_quotes(self):
        self.assertEqual(
            _normalize_cors_origins('"https://example.com"'),
            ["https://example.com"],
        )
        self.assertEqual(
            _normalize_cors_origins(['"https://a.com"', "'https://b.com'"]),
            ["https://a.com", "https://b.com"],
        )

    def test_normalize_cors_origins_rejects_invalid_urls(self):
        self.assertEqual(_normalize_cors_origins("not-a-url"), [])
        self.assertEqual(_normalize_cors_origins("javascript:alert(1)"), [])
        self.assertEqual(_normalize_cors_origins(["/relative/path", ""]), [])
        self.assertEqual(
            _normalize_cors_origins(["https://valid.com", "garbage"]),
            ["https://valid.com"],
        )

    def test_normalize_cors_origins_strips_path_from_url(self):
        self.assertEqual(
            _normalize_cors_origins("https://example.com/some/path?q=1"),
            ["https://example.com"],
        )

    def test_get_cors_origins_with_quoted_allow_cors(self):
        sites = [
            SimpleNamespace(
                name="site.test",
                config={
                    "domains": ["cdn.site.test"],
                    "allow_cors": '"https://portal.example.com"',
                },
            )
        ]

        self.assertCountEqual(
            _get_cors_origins(sites),
            [
                ('"site.test:https://portal.example.com"', "$http_origin"),
                ('"cdn.site.test:https://portal.example.com"', "$http_origin"),
            ],
        )
