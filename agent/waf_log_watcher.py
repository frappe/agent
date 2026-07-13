"""Long-running WAF audit-log watcher.

Mirrors `agent.nginx_reload_manager` in shape: a thin event loop driven off
`Server().config.json`, signal-handled, log-light. Discovers every active
per-site WAF by walking `<benches_directory>/*/sites/*/modsec/audit.log`,
tails each file from the last consumed offset, parses ModSecurity serial
transactions, batches them, and POSTs to `press.api.waf.ingest_logs` with the
per-site bearer token (`Authorization: Bearer ...`).

Authentication model: each site stores its own `waf_token` next to its
modsec config; on 401 we drop the in-flight batch, re-read the token from
disk (so a `Rotate WAF Log Token` AgentJob takes effect immediately) and
continue. There is no global token because WAF is a per-site feature.

Wired into the Agent supervisor via `agent/templates/agent/supervisor.conf.jinja2`
mirroring `[program:nginx_reload_manager]`.
"""

from __future__ import annotations

import glob
import json
import os
import signal
import time
import traceback
from typing import TYPE_CHECKING

import requests

from agent.waf_audit_log_parser import events_from_file

if TYPE_CHECKING:
	from agent.waf_audit_log_parser import WAFAuditEvent


# Batch/Poll tunables. ModSecurity audit events are sparse under normal
# traffic, so we err on the side of low latency and small batches.
BATCH_SIZE = 50
FLUSH_INTERVAL_SECONDS = 3.0
DISCOVERY_INTERVAL_SECONDS = 60.0
RETRY_BACKOFF_SECONDS = 10.0
HTTP_TIMEOUT = (5, 15)


class WAFLogWatcher:
	def __init__(self, directory: str | None = None, debug: bool = False):
		self.directory = directory or os.getcwd()
		self.config_file = os.path.join(self.directory, "config.json")
		# state_file stores per-site file offsets so crashes don't re-push.
		self.state_file = os.path.join(self.directory, "logs", "waf_log_watcher.state.json")
		# site -> {offset}. Only persists active sites.
		self.offsets: dict[str, int] = {}
		self.exit_requested = False
		self.debug = debug
		self._last_discovery = 0.0
		self._last_flush = 0.0

	# ------------------------------------------------------------------
	# Lifecycle
	# ------------------------------------------------------------------

	def process(self) -> None:
		signal.signal(signal.SIGINT, self.exit_gracefully)
		signal.signal(signal.SIGTERM, self.exit_gracefully)
		self._load_state()
		self._log("WAF log watcher started", print_always=True)
		while not self.exit_requested:
			try:
				self._tick()
				time.sleep(FLUSH_INTERVAL_SECONDS)
			except Exception:
				traceback.print_exc()
				time.sleep(RETRY_BACKOFF_SECONDS)
		self._persist_state()

	def exit_gracefully(self, signum, frame) -> None:
		self._log("exit requested", print_always=True)
		self.exit_requested = True
		self._persist_state()

	# ------------------------------------------------------------------
	# Core loop
	# ------------------------------------------------------------------

	def _tick(self) -> None:
		now = time.time()
		if now - self._last_discovery > DISCOVERY_INTERVAL_SECONDS:
			self._discover_targets()
			self._last_discovery = now

		for site_name, audit_log in self._active_targets.items():
			try:
				events, new_offset = events_from_file(audit_log, self.offsets.get(site_name, 0))
			except Exception:
				self._log(f"failed to read {audit_log}")
				continue
			if events:
				self._flush(site_name, events)
			self.offsets[site_name] = new_offset

	def _flush(self, site_name: str, events: list["WAFAuditEvent"]) -> None:
		"""Push one site's batch to Press. Reviewer-relevant contract lives here."""
		token = self._site_token(site_name)
		if not token:
			# WAF disabled between discovery and flush — drop the batch.
			self._log(f"no token for {site_name}; dropping {len(events)} events")
			return
		payload = [self._event_to_dict(e, site_name) for e in events if e.rule_id] #Only send events with rule_id to avoid noisy logs
		headers = {
			"X-WAF-Token": token,
			"X-Press-Site": site_name,
			"Content-Type": "application/json",
		}
		url = f"{self._press_url}/api/method/press.api.waf.ingest_logs"
		try:
			response = requests.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT)
		except requests.RequestException as exc:
			self._log(f"post failed for {site_name}: {exc}")
			# Leave offsets untouched so the next tick retries; modest backoff.
			time.sleep(RETRY_BACKOFF_SECONDS)
			return

		if response.status_code == 401:
			# Token rotated and the new one hasn't landed on disk yet, or
			# Press-side rotation happened out-of-band. Drop the batch (per
			# the design's rotation contract) and re-read token next flush.
			self._log(f"401 for {site_name}; dropping batch + refreshing token", print_always=True)
			self._clear_cached_token(site_name)
			return

		if response.status_code >= 400:
			self._log(
				f"post failed for {site_name}: HTTP {response.status_code} {response.text[:200]}"
			)
			time.sleep(RETRY_BACKOFF_SECONDS)
			return

		# Success. Offsets were already advanced in _tick; persist happens
		# at the end of the tick so a crash mid-tick replays this batch.
		self._log(f"flushed {len(events)} events for {site_name}")

	# ------------------------------------------------------------------
	# Discovery / target management
	# ------------------------------------------------------------------

	def _discover_targets(self) -> None:
		"""Scan `/var/log/nginx/modsec/*_audit.log` once a minute.

		Keeps the watcher decoupled from Press — sites being added/removed or
		WAF toggled on/off all surface here within one discovery interval.
		"""
		pattern = "/var/log/nginx/modsec/*_audit.log"
		active = {}

		for path in glob.glob(pattern):
			self._log(f"discovered audit log: {path}")

			filename = os.path.basename(path)
			if not filename.endswith("_audit.log"):
				continue

			site_name = filename[: -len("_audit.log")]
			active[site_name] = path

		# Drop offsets for sites that no longer have an audit log (WAF off).
		self.offsets = {k: v for k, v in self.offsets.items() if k in active}
		self._active_targets = active

	# ------------------------------------------------------------------
	# Token management / Press URL
	# ------------------------------------------------------------------

	@property
	def _press_url(self) -> str:
		return self.config.get("press_url", "https://frappecloud.com")

	@property
	def _active_targets(self) -> dict[str, str]:
		if not hasattr(self, "_targets_cache"):
			self._targets_cache = {}
		return self._targets_cache

	@_active_targets.setter
	def _active_targets(self, value: dict[str, str]) -> None:
		self._targets_cache = value

	def _site_token_path(self, site_name: str) -> str:
		"""Resolve the on-disk token file for a site by globbing benches."""
		pattern = os.path.join(self.config["benches_directory"], "*", "sites", site_name, "modsec", "waf_token")
		for path in glob.glob(pattern):
			return path
		return ""

	def _site_token(self, site_name: str) -> str | None:
		"""Read (and cache for one flush cycle) the per-site bearer token."""
		cache = getattr(self, "_token_cache", {})
		if site_name in cache:
			return cache[site_name]
		path = self._site_token_path(site_name)
		if not path or not os.path.exists(path):
			return None
		try:
			with open(path) as f:
				token = f.read().strip()
		except OSError:
			return None
		setattr(self, "_token_cache", {**cache, site_name: token})
		return token

	def _clear_cached_token(self, site_name: str) -> None:
		cache = getattr(self, "_token_cache", {})
		cache.pop(site_name, None)
		setattr(self, "_token_cache", cache)

	# ------------------------------------------------------------------
	# Event serialisation
	# ------------------------------------------------------------------

	@staticmethod
	def _event_to_dict(event: "WAFAuditEvent", site_name: str) -> dict:
		return {
			"transaction_id": event.transaction_id,
			"timestamp": event.timestamp,
			"site": site_name,
			"server": event.server,
			"client_ip": event.client_ip,
			"request_method": event.request_method,
			"request_uri": event.request_uri,
			"rule_id": event.rule_id,
			"rule_msg": event.rule_msg,
			"severity": event.severity,
			"action": event.action,
			"matched_data": event.matched_data,
			"raw_log": event.raw_log,
		}

	# ------------------------------------------------------------------
	# State persistence
	# ------------------------------------------------------------------

	def _load_state(self) -> None:
		try:
			with open(self.state_file) as f:
				self.offsets = json.load(f)
		except FileNotFoundError:
			self.offsets = {}
		except (json.JSONDecodeError, OSError):
			self.offsets = {}

	def _persist_state(self) -> None:
		try:
			os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
			with open(self.state_file, "w") as f:
				json.dump(self.offsets, f)
		except OSError:
			pass

	# ------------------------------------------------------------------
	# Misc
	# ------------------------------------------------------------------

	def _log(self, message: str, print_always: bool = False) -> None:
		if not self.debug and not print_always:
			return
		print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] WAF watcher: {message}")

	@property
	def config(self) -> dict:
		if not hasattr(self, "_config"):
			with open(self.config_file) as f:
				self._config = json.load(f)
		return self._config


if __name__ == "__main__":
	WAFLogWatcher().process()