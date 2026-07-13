from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from contextlib import suppress
from typing import TYPE_CHECKING

from agent.base import Base

if TYPE_CHECKING:
	from agent.bench import Bench
	from agent.site import Site


# Reserved rule id bases — kept in sync with templates/waf/main.conf.jinja2.
IP_ALLOW_BASE = 10_000
IP_BLOCK_BASE = 11_000
RATE_LIMIT_BASE = 20_000
BLOCKED_ENDPOINT_BASE = 30_000
BLOCKED_PARAMETER_BASE = 40_000
ALLOWED_PARAM_EXCLUSION_BASE = 50_000
REQUEST_LIMIT_BASE = 60_000


class WAF(Base):
	"""Per-site ModSecurity configuration owned by the Agent.

	Mirrors the lifecycle of a `Site`: lives under
	`<bench_directory>/sites/<name>/modsec/` and co-locates the audit log and
	the bearer token the watcher uses to push events to Press. The bench's
	`nginx.conf` references the resolved `main.conf` produced here.

	The Agent renders this file from a payload sent by Press via the
	`Update WAF Configuration` AgentJob; it never reads back from Press.
	"""

	def __init__(self, site: "Site"):
		super().__init__()
		self.site = site
		self.bench: "Bench" = site.bench
		self.modsec_directory = os.path.join(site.directory, "modsec")
		self.config_file = os.path.join(self.modsec_directory, "main.conf")
		self.audit_log = os.path.join("/var/log/nginx/modsec/", f"{site.name}_audit.log")
		self.token_file = os.path.join(self.modsec_directory, "waf_token")
		self._config_json = os.path.join(self.modsec_directory, "config.json")

	# ------------------------------------------------------------------
	# Helpers
	# ------------------------------------------------------------------

	@property
	def press_url(self) -> str:
		return self.bench.server.press_url

	def get_log_token(self) -> str | None:
		"""Read the locally cached bearer token for log push-back.

		Returns None if no token has been provisioned yet (WAF not active).
		"""
		try:
			with open(self.token_file) as f:
				return f.read().strip()
		except FileNotFoundError:
			return None

	def update_token(self, token: str) -> None:
		os.makedirs(self.modsec_directory, exist_ok=True)
		# Write atomically so the watcher doesn't read a half-written token.
		fd, tmp = tempfile.mkstemp(prefix="waf-token-", dir=self.modsec_directory)
		try:
			with os.fdopen(fd, "w") as f:
				f.write(token)
			os.chmod(tmp, 0o600)
			os.replace(tmp, self.token_file)
		except Exception:
			with suppress(FileNotFoundError):
				os.remove(tmp)
				raise

	def is_enabled(self) -> bool:
		"""Whether the bench nginx template should load ModSecurity for us.

		The bench nginx template consults this rather than just file existence
		so an in-flight `disable()` reload can't race a stale render.
		"""
		return os.path.isfile(self.config_file) and os.path.isfile(self.token_file)

	@property
	def main_conf_path(self) -> str | None:
		"""Path the bench nginx template should reference, or None when off."""
		return self.config_file if self.is_enabled() else None

	# ------------------------------------------------------------------
	# Configuration generation
	# ------------------------------------------------------------------

	def generate(self, config: dict) -> None:
		"""Render `modsec/main.conf` from a Press-sent config payload.

		Idempotent: safe to call on every WAF edit. Persists the payload to
		`config.json` so the watcher can self-describe (site name, audit log
		location) without reaching back into Press.
		"""
		os.makedirs(self.modsec_directory, exist_ok=True)
		# Persist the source-of-truth payload alongside the generated config
		# so the watcher and any post-mortem tooling can read it offline.
		self._write_json(self._config_json, config)

		rendered = self.bench.server._render_template_str(
			"waf/main.conf.jinja2",
			self._template_context(config),
		)
		self._write_atomic(self.config_file, rendered)

	def disable(self) -> None:
		"""Remove the per-site ModSecurity config entirely.

		Called from the `Disable WAF` AgentJob. Removes the whole `modsec/`
		directory but leaves the on-disk audit log in place until the next
		`generate()` rotates it; the watcher stops tailing the moment the
		config file is gone.
		"""
		with suppress(FileNotFoundError):
			shutil.rmtree(self.modsec_directory)

	# ------------------------------------------------------------------
	# Template plumbing
	# ------------------------------------------------------------------

	def _template_context(self, config: dict) -> dict:
		blocked_endpoints = [
			{
				# Endpoints are user-supplied regexes against REQUEST_URI — pass
				# through verbatim. Parameter names below are exact literals.
				"regex": ep.get("endpoint", ""),
				"methods": (ep.get("methods") or "").strip(),
				"label": ep.get("endpoint", ""),
			}
			for ep in config.get("blocked_endpoints", [])
		]
		blocked_parameters = self._build_blocked_parameters(
			config.get("blocked_parameters", [])
		)
		return {
			"site_name": self.site.name,
			"mode": config.get("mode", "Detection"),
			"modsec_default_conf": self.bench.server.config.get(
				"modsec_default_conf", "/etc/nginx/modsecurity/modsecurity.conf"
			),
			"audit_log_path": self.audit_log,
			"ip_rules": config.get("ip_rules", []),
			"rate_limits": config.get("rate_limits", []),
			"blocked_endpoints": blocked_endpoints,
			"blocked_parameters": blocked_parameters,
			"request_limits": config.get("request_limits", []),
			"custom_rules": config.get("custom_rules", []),
		}

	def _build_blocked_parameters(self, raw_params) -> list[dict]:
		"""Convert blocked-parameter rows into template-ready ModSecurity rules."""

		result = []

		for bp in raw_params:
			location = (bp.get("location") or "ARGS").strip()
			parameter = (bp.get("parameter") or "").strip()
			match_type = (bp.get("match_type") or "Exists").strip()
			value = (bp.get("value") or "").strip()
			endpoint = (bp.get("endpoint") or "").strip()

			# Defaults.
			target = location
			operator = "@rx"
			match_value = value

			if match_type == "Exists":
				# Match the existence of a named parameter/header/cookie.
				if location == "ARGS":
					target = "ARGS_NAMES"
				elif location == "REQUEST_HEADERS":
					target = "REQUEST_HEADERS_NAMES"
				elif location == "REQUEST_COOKIES":
					target = "REQUEST_COOKIES_NAMES"
				else:
					target = location

				operator = "@streq"
				match_value = parameter

			else:
				# Match against the value.
				if location == "ARGS":
					target = f"ARGS:{parameter}" if parameter else "ARGS"

				elif location == "REQUEST_HEADERS":
					target = (
						f"REQUEST_HEADERS:{parameter}"
						if parameter
						else "REQUEST_HEADERS"
					)

				elif location == "REQUEST_COOKIES":
					target = (
						f"REQUEST_COOKIES:{parameter}"
						if parameter
						else "REQUEST_COOKIES"
					)

				elif location == "REQUEST_BODY":
					target = "REQUEST_BODY"

				if match_type == "Equals":
					operator = "@streq"

				elif match_type == "Contains":
					operator = "@contains"

				elif match_type == "Regex":
					operator = "@rx"

				else:
					operator = "@rx"

			result.append(
				{
					"endpoint": endpoint,
					"target": target,
					"operator": operator,
					"match_value": match_value,
					"parameter": parameter,
					"location": location,
					"match_type": match_type,
					"label": f"{parameter}={value}" if value else parameter,
				}
			)

		return result

	@staticmethod
	def _escape_regex(value: str) -> str:
		"""Make user-supplied literal strings safe to embed as `@rx ...`.

		WAF rules accept user regexps already, so callers who want regex
		behaviour pass it themselves — but for parameter names and URLs the
		common case is an exact match, hence escaping by default. The
		`encoder` for `blocked_endpoints` keeps regex metacharacters intact
		(is_endpoints_regex) — see `_escape_regex_endpoint`.
		"""
		return re.escape(value)

	@staticmethod
	def _write_atomic(path: str, content: str) -> None:
		directory = os.path.dirname(path)
		fd, tmp = tempfile.mkstemp(prefix="waf-conf-", dir=directory)
		try:
			with os.fdopen(fd, "w") as f:
				f.write(content)
			os.replace(tmp, path)
		except Exception:
			with suppress(FileNotFoundError):
				os.remove(tmp)
			raise

	@staticmethod
	def _write_json(path: str, data) -> None:
		fd, tmp = tempfile.mkstemp(prefix="waf-json-", dir=os.path.dirname(path))
		try:
			with os.fdopen(fd, "w") as f:
				json.dump(data, f, indent=1, sort_keys=True)
			os.replace(tmp, path)
		except Exception:
			with suppress(FileNotFoundError):
				os.remove(tmp)
			raise
