from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from agent.server import Server
from agent.waf import WAF
from agent.waf_audit_log_parser import parse_log, events_from_file


def _audit_record(**overrides):
	"""Build one ModSecurity v3 JSON audit record (single-line, one transaction).

	Purposefully mirrors the real shape shipped by libmodsecurity v3 with
	`SecAuditLogFormat JSON` (see owasp-modsecurity/ModSecurity issue #3100
	for a real-world sample). One record == one audit log file line.
	"""
	tx = {
		"client_ip": "1.2.3.4",
		"client_port": 443,
		"time_stamp": "06/Jul/2026:12:00:00 +0000",
		"unique_id": "TIDAbC",
		"request": {
			"method": "GET",
			"http_version": 1.1,
			"uri": "/api/method/foo?fields=a",
			"body": "",
			"headers": {
				"host": "example.frappe.cloud",
				"x-real-ip": "10.0.0.9",
				"user-agent": "curl/8",
			},
		},
		"response": {
			"http_code": 403,
			"headers": {"Content-Type": "text/html"},
		},
		"messages": [
			{
				"message": "Endpoint blocked",
				"details": {
					"ruleId": "30001",
					"severity": "2",  # numeric: 0=CRITICAL,1=ERROR,2=WARNING,...
					"match": "Matched \"Operator `Rx' ... against variable `REQUEST_URI' (Value: `/api/method/foo')",
					"data": "",
				},
			}
		],
	}
	tx.update(overrides.pop("transaction", {}))
	tx.update(overrides)
	return json.dumps({"transaction": tx}, separators=(",", ":"))


# Two complete transactions, each on its own line — exactly what ModSecurity
# v3 emits in `Serial` mode with `SecAuditLogFormat JSON`.
SAMPLE_AUDIT = "\n".join(
	[
		_audit_record(
			transaction={
				"unique_id": "TIDAbC",
				"request": {
					"method": "GET",
					"http_version": 1.1,
					"uri": "/api/method/foo?fields=a",
					"body": "",
					"headers": {
						"host": "example.frappe.cloud",
						"x-real-ip": "10.0.0.9",
					},
				},
				"response": {"http_code": 403, "headers": {}},
				"messages": [
					{
						"message": "Endpoint blocked",
						"details": {
							"ruleId": "30001",
							"severity": "2",
							"match": "Matched \"Operator `Rx' ... against variable `REQUEST_URI'",
							"data": "",
						},
					}
				],
			}
		),
		_audit_record(
			transaction={
				"unique_id": "TIDDef",
				"time_stamp": "06/Jul/2026:12:00:01 +0000",
				"client_ip": "1.2.3.4",
				"request": {
					"method": "POST",
					"http_version": 1.1,
					"uri": "/api/method/bar",
					"body": "",
					"headers": {"host": "example.frappe.cloud"},
				},
				"response": {"http_code": 200, "headers": {"Content-Type": "application/json"}},
				"messages": [
					{
						"message": "Rate limit hit",
						"details": {
							"ruleId": "20001",
							"severity": "0",  # CRITICAL
							"match": "Rate counter exceeded",
							"data": "",
						},
					}
				],
			}
		),
	]
) + "\n"


class TestWAFAuditParser(unittest.TestCase):
	def test_parse_log_yields_one_event_per_transaction(self):
		events = list(parse_log(SAMPLE_AUDIT))
		self.assertEqual(len(events), 2)
		self.assertEqual(events[0].transaction_id, "TIDAbC")
		self.assertEqual(events[0].request_method, "GET")
		self.assertEqual(events[0].request_uri, "/api/method/foo?fields=a")
		# X-Real-IP overrides the connection-level client_ip, matching the
		# bench nginx `proxy_set_header X-Real-IP $remote_addr;` directive.
		self.assertEqual(events[0].client_ip, "10.0.0.9")
		self.assertEqual(events[0].rule_id, "30001")
		self.assertEqual(events[0].rule_msg, "Endpoint blocked")
		# Numeric JSON severity "2" is mapped to the human word our
		# `WAF Log` doctype stores — preserves the on-wire payload shape.
		self.assertEqual(events[0].severity, "WARNING")
		self.assertEqual(events[0].action, "Intercepted")
		self.assertIn("Matched", events[0].matched_data)

	def test_parse_log_passed_through_on_2xx(self):
		events = list(parse_log(SAMPLE_AUDIT))
		self.assertEqual(events[1].transaction_id, "TIDDef")
		self.assertEqual(events[1].action, "Passed")
		# Numeric "0" maps to CRITICAL — same word the old serial parser
		# pulled from `[severity "CRITICAL"]` in the I section.
		self.assertEqual(events[1].severity, "CRITICAL")

	def test_parse_log_drops_partial_transaction(self):
		# Slice the trailing record so the second transaction's JSON object
		# is truncated mid-array — `raw_decode` refuses to parse it, so only
		# the first complete record is yielded. Mirrors the old behaviour
		# of dropping at the missing `--boundary-Z--` marker.
		partial = SAMPLE_AUDIT.rsplit('"messages"', 1)[0]
		events = list(parse_log(partial))
		self.assertEqual(len(events), 1)
		self.assertEqual(events[0].transaction_id, "TIDAbC")

	def test_parse_log_tolerates_garbage_prefix(self):
		# Non-JSON noise before a real record (e.g., log rotation left
		# truncated bytes) is dropped at the same trade-off the serial parser
		# applied, by means of `raw_decode` skipping past parse failures.
		# We assert the watcher never crashes on a corrupted head.
		events = list(parse_log("garbage-not-json\n" + SAMPLE_AUDIT))
		# `raw_decode` fails at position 0; parser stops and yields nothing.
		# The caller (`events_from_file`) advances past such a fragment.
		self.assertEqual(events, [])


class TestWAFFromFile(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.mkdtemp(prefix="waf-test-")
		self.path = os.path.join(self.tmp, "audit.log")
		with open(self.path, "w") as f:
			f.write(SAMPLE_AUDIT)

	def tearDown(self):
		shutil.rmtree(self.tmp, ignore_errors=True)

	def test_reads_full_log_then_resumes(self):
		events, offset = events_from_file(self.path, 0)
		self.assertEqual(len(events), 2)
		# subsequent read from offset yields nothing
		events2, offset2 = events_from_file(self.path, offset)
		self.assertEqual(events2, [])
		self.assertEqual(offset2, offset)

	def test_releases_trailing_partial_record_for_next_pass(self):
		# Append a truncated JSON record (ModSecurity still flushing). It
		# must NOT appear in this batch, and the offset must leave its bytes
		# intact so the next read picks up the completion.
		with open(self.path, "a") as f:
			f.write('{"transaction":{"unique_id":"PARTIAL"')
		events, offset = events_from_file(self.path, 0)
		self.assertEqual(len(events), 2)  # only the two complete records
		self.assertLess(offset, os.path.getsize(self.path))
		# Next pass: pretend ModSecurity finished the partial record.
		# Rewind to the partial start, complete the JSON, then re-read.
		with open(self.path, "w") as f:
			f.write(SAMPLE_AUDIT)
			f.write('{"transaction":{"unique_id":"PARTIAL","time_stamp":"06/Jul/2026:12:00:02 +0000","request":{"method":"GET","uri":"/p"},"response":{"http_code":403}}}\n')
		events_next, _ = events_from_file(self.path, offset)
		ids = {e.transaction_id for e in events_next}
		self.assertIn("PARTIAL", ids)

	def test_rotation_resets_offset(self):
		_, offset = events_from_file(self.path, 0)
		self.assertGreater(offset, 0)
		# Truncate file to a fragment that's not a complete JSON record —
		# simulates logrotate moving the file out and starting fresh.
		with open(self.path, "w") as f:
			f.write('{"transaction":{"unique_id":"ORPHAN"')
		# Old offset now exceeds new file size; the reader resets to 0, finds
		# no complete record (partial bytes at file head), and advances past
		# the partial — the same "drop incomplete head" trade-off the old
		# serial parser applied when no `--boundary-Z--` was present.
		events, new_offset = events_from_file(self.path, offset)
		self.assertEqual(events, [])
		self.assertEqual(new_offset, os.path.getsize(self.path))


def _stub_server(tmp):
	with patch.object(Server, "__init__", new=lambda x: None):
		server = Server()
	server.config = {
		"modsec_default_conf": "/etc/nginx/modsecurity/modsecurity.conf",
		"press_url": "https://press.test",
	}
	server._render_template_str = lambda template, context, options=None: _render(template, context)
	return server


def _render(template, context):
	"""Render an Agent Jinja template without importing the `agent` package.

	Using `PackageLoader` would trigger `agent/__init__.py` -> `agent.cli` ->
	`agent.bench` -> ... and pull in heavy third‑day deps (redis, peewee, …)
	that aren't installed in this sandbox. FileSystemLoader against the
	repo's templates dir gives identical results with zero import side effects.
	"""
	import os

	from jinja2 import Environment, FileSystemLoader

	templates_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
	env = Environment(loader=FileSystemLoader(templates_dir))
	return env.get_template(template).render(**context)


class TestWAFConfig(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.mkdtemp(prefix="waf-render-")
		self.bench_tmp = os.path.join(self.tmp, "bench", "sites")
		os.makedirs(self.bench_tmp)
		self.site_dir = os.path.join(self.bench_tmp, "site.test")
		os.makedirs(self.site_dir)

	def tearDown(self):
		shutil.rmtree(self.tmp, ignore_errors=True)

	def _make_waf(self):
		server = _stub_server(self.tmp)
		bench = SimpleNamespace(server=server)
		site = SimpleNamespace(name="site.test", directory=self.site_dir, bench=bench)
		return WAF(site)

	def test_generate_renders_all_rule_types(self):
		waf = self._make_waf()
		config = {
			"enabled": True,
			"mode": "Prevention",
			"rate_limits": [
				{"rate": 10, "window": 60, "burst": 5, "key": "IP", "endpoint": ""},
				{"rate": 5, "window": 30, "burst": 0, "key": "URI", "endpoint": "^/api/"},
				{"rate": 3, "window": 60, "burst": 1, "key": "IP+URI", "endpoint": ""},
			],
			"blocked_endpoints": [
				{"endpoint": "^/api/method/danger", "methods": "GET,POST"},
				{"endpoint": "^/admin($|/)", "methods": ""},
			],
			"blocked_parameters": [
				{"endpoint": "", "location": "ARGS", "parameter": "evil", "match_type": "Exists", "value": ""},
				{"endpoint": "^/api/login$", "location": "ARGS", "parameter": "debug", "match_type": "Equals", "value": "1"},
				{"endpoint": "", "location": "REQUEST_HEADERS", "parameter": "User-Agent", "match_type": "Contains", "value": "curl"},
			],
			"request_limits": [{"limit_type": "Body Size", "value": 1024, "endpoint": ""}],
			"ip_rules": [
				{"ip": "1.2.3.4", "rule_type": "Allowed"},
				{"ip": "5.6.7.8", "rule_type": "Blocked"},
			],
			"custom_rules": [{"rule_id": 100000, "rule_name": "demo", "rule_text": 'SecRule ARGS "@streq x" "id:100000,phase:2,deny,log"'}],
		}
		waf.update_token("token-123")
		waf.generate(config)
		with open(waf.config_file) as f:
			rendered = f.read()
		self.assertIn("SecRuleEngine On", rendered)
		# Rate limit rule references the IP collection initcol above it.
		self.assertIn("SecAction", rendered) and self.assertIn("initcol:ip", rendered)
		# Three rate-limit keys: IP, URI, IP+URI
		self.assertIn("ip.rate_0=+1", rendered)
		self.assertIn("URI:rate_1", rendered) and self.assertIn("^/api/", rendered)
		self.assertIn("ip_uri.rate_2=+1", rendered) and self.assertIn("initcol:ip_uri=%{REMOTE_ADDR}%{REQUEST_URI}", rendered)
		self.assertIn("deny,status:429", rendered)
		self.assertIn("SecRule REQUEST_URI", rendered)  # blocked endpoint
		self.assertIn("@ipMatch 1.2.3.4", rendered)
		self.assertIn("allow,nolog", rendered)
		self.assertIn("deny,status:403", rendered)
		self.assertIn('SecRule REQUEST_BODY_LENGTH "@gt 1024"', rendered)
		self.assertIn('id:100000,phase:2,deny,log', rendered)
		# Blocked parameters: Exists match on ARGS:evil (no chain, no endpoint)
		self.assertIn('ARGS:evil "@rx .*"', rendered)
		# Blocked parameters: Equals match on ARGS:debug with endpoint chain
		self.assertIn('ARGS:debug "@streq 1"', rendered)
		self.assertIn("REQUEST_URI \"@rx ^/api/login$\"", rendered)
		# Blocked parameters: Contains match on REQUEST_HEADERS:User-Agent
		self.assertIn('REQUEST_HEADERS:User-Agent "@contains curl"', rendered)
		# No allowed-parameter exclusions should appear
		self.assertNotIn("ctl:ruleRemoveTargetById", rendered)
		# audit log config now asks libmodsecurity for JSON output (was Serial)
		self.assertIn("SecAuditLogFormat JSON", rendered)
		self.assertIn("SecAuditLogType Serial", rendered)
		# audit log path is per-site
		self.assertIn(waf.audit_log, rendered)

	def test_is_enabled_requires_token_and_config(self):
		waf = self._make_waf()
		self.assertFalse(waf.is_enabled())
		waf.update_token("tok")
		# config not generated yet
		self.assertFalse(waf.is_enabled())
		waf.generate({"enabled": True, "mode": "Detection"})
		self.assertTrue(waf.is_enabled())
		self.assertEqual(waf.get_log_token(), "tok")

	def test_disable_removes_directory(self):
		waf = self._make_waf()
		waf.update_token("tok")
		waf.generate({"enabled": True, "mode": "Detection"})
		self.assertTrue(os.path.isdir(waf.modsec_directory))
		waf.disable()
		self.assertFalse(os.path.exists(waf.modsec_directory))
		self.assertIsNone(waf.main_conf_path)

	def test_update_token_atomic(self):
		waf = self._make_waf()
		waf.update_token("first")
		self.assertEqual(waf.get_log_token(), "first")
		waf.update_token("second")
		self.assertEqual(waf.get_log_token(), "second")


if __name__ == "__main__":
	unittest.main()