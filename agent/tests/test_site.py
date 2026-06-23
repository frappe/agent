from __future__ import annotations

import json
import os
import random
import shutil
import string
import unittest
import warnings
from types import SimpleNamespace
from unittest.mock import patch

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from testcontainers.nginx import NginxContainer

from agent.base import AgentException
from agent.bench import Bench, _get_cors_origins, _normalize_cors_origins
from agent.server import Server
from agent.site import Site

NGINX_FUZZ_ALPHABET = string.ascii_letters + string.digits + ':;/?.-_[]@{}"\'\\\n\r\t *,$'
CORS_NGINX_HYPOTHESIS_CASES = 512
VALID_CORS_FUZZ_ORIGINS = [
    "*",
    "https://example.com",
    "http://example.test:8080/path?q=1",
    "https://127.0.0.1:9000",
    "https://[::1]:9443",
]
VALID_CORS_FUZZ_HOSTNAMES = [
    "site.test",
    "cdn.site.test",
    "assets.example.com",
    "127.0.0.1",
    "[::1]",
]
KNOWN_BAD_CORS_PAYLOADS = [
    'https://example.com"; add_header x y;',
    "https://example.com\nadd_header x y",
    r"https://example.com\foo",
    "https://user:pass@example.com",
    "https://[::1",
    "https://example.com:bad",
    "\"*\"",
    "\\\"*\\\"",
    "'https://example.com'",
]


def _random_nginx_payload(rng, max_length=48):
    return "".join(rng.choice(NGINX_FUZZ_ALPHABET) for _ in range(rng.randint(0, max_length)))


def _random_wrapped_cors_value(rng, value):
    forms = [
        value,
        f'"{value}"',
        f"'{value}'",
        json.dumps(value),
        json.dumps(json.dumps(value)),
        value.replace('"', r"\""),
        f"{value},{rng.choice(VALID_CORS_FUZZ_ORIGINS)}",
    ]
    return rng.choice(forms)


def _random_cors_input(rng):
    payloads = VALID_CORS_FUZZ_ORIGINS + KNOWN_BAD_CORS_PAYLOADS + [_random_nginx_payload(rng)]
    value = _random_wrapped_cors_value(
        rng,
        rng.choice(payloads),
    )
    choices = [
        value,
        [value, _random_wrapped_cors_value(rng, rng.choice(VALID_CORS_FUZZ_ORIGINS)), None, 1, {}],
        tuple([value, _random_nginx_payload(rng)]),
        {value, _random_wrapped_cors_value(rng, rng.choice(KNOWN_BAD_CORS_PAYLOADS))},
        json.dumps([value, _random_wrapped_cors_value(rng, rng.choice(VALID_CORS_FUZZ_ORIGINS))]),
        None,
        1,
        {"origin": value},
    ]
    return rng.choice(choices)


def _random_cors_domains(rng):
    value = rng.choice(
        [
            "site.test",
            "cdn.site.test",
            "127.0.0.1",
            "[::1]",
            _random_nginx_payload(rng, 32),
            'bad.test"; add_header x y;',
            "bad domain.test",
            None,
            1,
        ]
    )
    choices = [
        [value, "assets.site.test"],
        value,
        tuple([value, _random_nginx_payload(rng, 32)]),
        {value} if isinstance(value, str) else {_random_nginx_payload(rng, 32)},
        None,
        1,
        {"domain": value},
    ]
    return rng.choice(choices)


def _cors_text_strategy(max_size=80):
    return st.text(alphabet=NGINX_FUZZ_ALPHABET, max_size=max_size)


def _cors_input_strategy():
    scalar = st.one_of(
        st.none(),
        st.integers(),
        st.dictionaries(_cors_text_strategy(8), _cors_text_strategy(16), max_size=2),
        st.sampled_from(VALID_CORS_FUZZ_ORIGINS + KNOWN_BAD_CORS_PAYLOADS),
        _cors_text_strategy(),
    )
    return st.one_of(
        scalar,
        st.lists(scalar, max_size=5),
        st.tuples(scalar, scalar),
        st.lists(_cors_text_strategy(), min_size=0, max_size=4).map(json.dumps),
    )


def _domain_input_strategy():
    scalar = st.one_of(
        st.none(),
        st.integers(),
        st.sampled_from(VALID_CORS_FUZZ_HOSTNAMES),
        _cors_text_strategy(64),
    )
    return st.one_of(
        scalar,
        st.lists(scalar, max_size=5),
        st.tuples(scalar, scalar),
        st.dictionaries(_cors_text_strategy(8), scalar, max_size=2),
    )


def _cors_nginx_case_strategy():
    return st.fixed_dictionaries(
        {
            "site_name": st.one_of(
                st.sampled_from(VALID_CORS_FUZZ_HOSTNAMES),
                _cors_text_strategy(64),
            ),
            "domains": _domain_input_strategy(),
            "site_cors": _cors_input_strategy(),
            "bench_cors": _cors_input_strategy(),
        }
    )


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

    def _assert_safe_cors_nginx_entries(self, cors_origins, rendered):
        self.assertNotIn('"; add_header', rendered)
        self.assertNotIn("\nadd_header x y", rendered)
        for matcher, header_value in cors_origins:
            self.assertIn(header_value, ('$http_origin', '"*"'))
            self.assertNotRegex(matcher, r"[\r\n\t ;{}]")

            if header_value == "$http_origin":
                self.assertTrue(matcher.startswith('"') and matcher.endswith('"'))
                self.assertNotIn('"', matcher[1:-1])
                self.assertNotIn("\\", matcher[1:-1])
            else:
                self.assertTrue(matcher.startswith("~^") and matcher.endswith(":.*$"))
                self.assertNotIn('"', matcher)

    def _write_nginx_wrapper_config(self, rendered):
        nginx_directory = os.path.join(self.test_dir, "container-nginx")
        self._write_nginx_config_files(nginx_directory, rendered)
        return nginx_directory

    def _write_nginx_config_files(self, nginx_directory, rendered):
        conf_directory = os.path.join(nginx_directory, "conf.d")
        os.makedirs(conf_directory, exist_ok=True)

        with open(os.path.join(nginx_directory, "nginx.conf"), "w") as f:
            f.write("events {}\nhttp {\n\tinclude /etc/nginx/conf.d/bench.conf;\n}\n")

        with open(os.path.join(conf_directory, "bench.conf"), "w") as f:
            f.write(rendered)

    def _start_nginx_config_container(self, nginx_directory):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="__init__ is deprecated.*")
            container = NginxContainer("nginx:1.27-alpine")
        container.with_command("sleep 60")
        container.with_volume_mapping(os.path.abspath(nginx_directory), "/etc/nginx", mode="ro")
        container.start()
        return container

    def _nginx_cors_case_log(
        self, *, case_index, nginx_directory, cors_inputs, cors_origins, nginx_output
    ):
        return json.dumps(
            {
                "case_index": case_index,
                "nginx_config_directory": nginx_directory,
                "cors_inputs": cors_inputs,
                "cors_origins": cors_origins,
                "nginx_output": nginx_output,
            },
            default=str,
            sort_keys=True,
        )

    def _assert_nginx_accepts_config_in_container(self, rendered, *, cors_inputs, cors_origins):
        nginx_directory = self._write_nginx_wrapper_config(rendered)
        container = self._start_nginx_config_container(nginx_directory)
        try:
            exit_code, output = container.exec("nginx -t -c /etc/nginx/nginx.conf")
        finally:
            container.stop(force=True)

        if exit_code != 0:
            failed_case_log = self._nginx_cors_case_log(
                case_index=0,
                nginx_directory=nginx_directory,
                cors_inputs=cors_inputs,
                cors_origins=cors_origins,
                nginx_output=output.decode(),
            )
            print(failed_case_log)
            self.fail(f"nginx rejected rendered CORS config: {failed_case_log}")

    def _assert_nginx_accepts_cors_case(self, case):
        if not hasattr(self, "_nginx_hypothesis_container"):
            nginx_directory = os.path.join(self.test_dir, "container-nginx-hypothesis")
            self._write_nginx_config_files(nginx_directory, "")
            self._nginx_hypothesis_directory = nginx_directory
            self._nginx_hypothesis_container = self._start_nginx_config_container(nginx_directory)
            self._nginx_hypothesis_case_index = 0
            self.addCleanup(self._nginx_hypothesis_container.stop, force=True)

        site = SimpleNamespace(
            name=case["site_name"],
            config={
                "allow_cors": case["site_cors"],
                "domains": case["domains"],
            },
        )
        cors_origins = _get_cors_origins([site], case["bench_cors"])
        rendered = self._render_bench_nginx(cors_origins)
        self._assert_safe_cors_nginx_entries(cors_origins, rendered)
        self._write_nginx_config_files(self._nginx_hypothesis_directory, rendered)
        exit_code, output = self._nginx_hypothesis_container.exec(
            "nginx -t -c /etc/nginx/nginx.conf"
        )
        if exit_code != 0:
            failed_case_log = self._nginx_cors_case_log(
                case_index=self._nginx_hypothesis_case_index,
                nginx_directory=self._nginx_hypothesis_directory,
                cors_inputs=case,
                cors_origins=cors_origins,
                nginx_output=output.decode(),
            )
            print(failed_case_log)
            self.fail(f"nginx rejected CORS case: {failed_case_log}")
        self._nginx_hypothesis_case_index += 1

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

    def test_normalize_cors_origins_handles_escaped_values(self):
        self.assertEqual(_normalize_cors_origins('"\\"*\\""'), ["*"])
        self.assertEqual(
            _normalize_cors_origins('"\\"https://example.com\\""'),
            ["https://example.com"],
        )
        self.assertEqual(
            _normalize_cors_origins('["https://a.com", "\\"https://b.com\\""]'),
            ["https://a.com", "https://b.com"],
        )

    def test_normalize_cors_origins_handles_multiple_and_duplicate_values(self):
        self.assertEqual(
            _normalize_cors_origins("https://a.com, https://b.com, https://a.com"),
            ["https://a.com", "https://b.com"],
        )

    def test_normalize_cors_origins_rejects_invalid_urls(self):
        self.assertEqual(_normalize_cors_origins("not-a-url"), [])
        self.assertEqual(_normalize_cors_origins("javascript:alert(1)"), [])
        self.assertEqual(_normalize_cors_origins(["/relative/path", ""]), [])
        self.assertEqual(_normalize_cors_origins([None, 1, {}]), [])
        self.assertEqual(
            _normalize_cors_origins(["https://valid.com", "garbage"]),
            ["https://valid.com"],
        )

    def test_normalize_cors_origins_rejects_unsafe_nginx_values(self):
        self.assertEqual(_normalize_cors_origins('https://example.com"; add_header x y;'), [])
        self.assertEqual(_normalize_cors_origins("https://example.com\nadd_header x y"), [])
        self.assertEqual(_normalize_cors_origins(r"https://example.com\foo"), [])
        self.assertEqual(_normalize_cors_origins("https://user:pass@example.com"), [])
        self.assertEqual(_normalize_cors_origins("https://[::1"), [])
        self.assertEqual(_normalize_cors_origins("https://example.com:bad"), [])

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

    def test_get_cors_origins_skips_unsafe_hostnames(self):
        sites = [
            SimpleNamespace(
                name='site.test"; add_header x y;',
                config={
                    "domains": ["cdn.site.test", "bad domain.test"],
                    "allow_cors": '"\\"*\\""',
                },
            )
        ]

        self.assertCountEqual(
            _get_cors_origins(sites),
            [(r"~^cdn\.site\.test:.*$", '"*"')],
        )

    def test_cors_origin_fuzzer_does_not_emit_unsafe_nginx_map_entries(self):
        seed = 20260623
        rng = random.Random(seed)

        for index in range(500):
            site = SimpleNamespace(
                name=rng.choice(
                    ["site.test", _random_nginx_payload(rng, 32), 'site.test"; add_header x y;']
                ),
                config={
                    "allow_cors": _random_cors_input(rng),
                    "domains": _random_cors_domains(rng),
                },
            )

            with self.subTest(seed=seed, index=index, site=site):
                cors_origins = _get_cors_origins([site], _random_cors_input(rng))
                rendered = self._render_bench_nginx(cors_origins)
                self._assert_safe_cors_nginx_entries(cors_origins, rendered)

    @settings(max_examples=200, deadline=None)
    @given(
        site_name=_cors_text_strategy(64),
        domains=_domain_input_strategy(),
        site_cors=_cors_input_strategy(),
        bench_cors=_cors_input_strategy(),
    )
    def test_cors_origin_hypothesis_does_not_emit_unsafe_nginx_map_entries(
        self,
        site_name,
        domains,
        site_cors,
        bench_cors,
    ):
        site = SimpleNamespace(
            name=site_name,
            config={
                "allow_cors": site_cors,
                "domains": domains,
            },
        )

        cors_origins = _get_cors_origins([site], bench_cors)
        rendered = self._render_bench_nginx(cors_origins)
        self._assert_safe_cors_nginx_entries(cors_origins, rendered)

    @settings(
        max_examples=CORS_NGINX_HYPOTHESIS_CASES,
        deadline=None,
        suppress_health_check=[
            HealthCheck.data_too_large,
            HealthCheck.large_base_example,
            HealthCheck.too_slow,
        ],
    )
    @given(case=_cors_nginx_case_strategy())
    def test_cors_origin_hypothesis_cases_are_valid_nginx_config(self, case):
        self._assert_nginx_accepts_cors_case(case)

    def test_rendered_fuzzed_cors_config_is_valid_nginx_config(self):
        cors_values = [
            "*",
            '"\\""*\\""',
            '"https://portal.example.com"',
            '"\\""https://portal.example.com\\""',
            '["https://a.example.com", "\\"https://b.example.com\\""]',
            "https://example.com, https://cdn.example.com, https://example.com",
            'https://example.com"; add_header x y;',
            "https://example.com\nadd_header x y",
            r"https://example.com\foo",
            "https://user:pass@example.com",
            "https://[::1",
            "https://example.com:bad",
            None,
            1,
            {"origin": "https://example.com"},
        ]
        sites = [
            SimpleNamespace(
                name=f"site-{index}.test",
                config={
                    "allow_cors": cors_value,
                    "domains": [
                        f"cdn-{index}.site.test",
                        f'bad-{index}.site.test"; add_header x y;',
                        "bad domain.test",
                    ],
                },
            )
            for index, cors_value in enumerate(cors_values)
        ]

        cors_origins = _get_cors_origins(sites, '"\\""https://bench.example.com\\""')
        rendered = self._render_bench_nginx(cors_origins)

        self._assert_safe_cors_nginx_entries(cors_origins, rendered)
        self._assert_nginx_accepts_config_in_container(
            rendered,
            cors_inputs={
                "bench_cors": '"\\""https://bench.example.com\\""',
                "site_cors_values": cors_values,
            },
            cors_origins=cors_origins,
        )
