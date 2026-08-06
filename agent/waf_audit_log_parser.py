"""Parser for ModSecurity JSON audit logs.

The audit log emitted by
ModSecurity v3 with `SecAuditLogFormat JSON` contains one JSON object per
audited transaction, e.g. (from issue owasp-modsecurity/ModSecurity#3100):

    {"transaction":{"client_ip":"1.2.3.4","time_stamp":"Fri Mar  1 ...",
      "unique_id":"170932468683.123247",
      "request":{"method":"HEAD","http_version":2.0,"uri":"/","body":"",
        "headers":{"host":"example.com"}},"response":{"http_code":403,
        "headers":{...}},
      "messages":[{"message":"Endpoint blocked","details":{
        "ruleId":"1","severity":"0","match":"Matched ...","data":""}}]}}

Each transaction is serialised to a single line in `Serial` mode, so the
watcher's "tail a line + advance offset" loop maps 1:1. We use
`JSONDecoder.raw_decode` so that a partially-flushed trailing record is
left for the next pass, exactly mirroring the old `--boundary-Z--`
detection without any regex.

External surface intentionally kept identical to the previous parser:
`WAFAuditEvent`, `parse_log`, `events_from_file` are the same names and
shapes the watcher (`agent.waf_log_watcher`) and tests import — only the
*parsing mechanism* changed. No caller of these entrypoints needs an edit.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Iterator


# ModSecurity numeric severities (per Reference Manual) → the human words
# our `WAF Log` doctype stores. The audit JSON reports `details.severity` as
# a numeric string ("0".."5"), not a word.
_SEVERITY_BY_NUM = {
	"0": "CRITICAL",
	"1": "ERROR",
	"2": "WARNING",
	"3": "NOTICE",
	"4": "INFO",
	"5": "DEBUG",
}

# Status codes ModSecurity produces when it intercepts in phase 1/2. Used
# only to derive the "Intercepted"/"Passed" action label from
# `response.http_code` in the JSON (the JSON has no explicit action field).
_INTERCEPTED_STATUS = {403, 404, 413, 429, 431, 500}


@dataclass
class WAFAuditEvent:
	"""One normalised audit event for a single ModSecurity transaction.

	Unchanged shape — every field the watcher and Press `WAF Log` doctype
	rely on is preserved.
	"""

	transaction_id: str
	timestamp: str  # raw ModSecurity date string; Press re-parses
	site: str = ""
	server: str = ""
	client_ip: str = ""
	request_method: str = ""
	request_uri: str = ""
	rule_id: str = ""
	rule_msg: str = ""
	severity: str = ""
	action: str = ""
	matched_data: str = ""
	raw_log: str = ""

	# Free-form extras carried through to the watcher for debugging.
	extras: dict = field(default_factory=dict)


def parse_log(text: str) -> Iterator[WAFAuditEvent]:
	"""Yield a WAFAuditEvent for every complete JSON transaction in `text`.

	Robust to embedded newlines inside JSON string values (ModSecurity
	escapes them as `\n` in the serialised output since v3.0.13 / per issue
	#3463's fix; older buggy builds may emit raw newlines in bodies). We
	mode each transaction with `JSONDecoder.raw_decode`, which reads exactly
	up to the matching closing brace and reports the offset consumed, so
	partial trailing writes are left untouched for the next pass.
	"""
	decoder = json.JSONDecoder()
	pos = 0
	length = len(text)
	while pos < length:
		# Skip whitespace and stray separators between records. Audit logs
		# generally newline-separate records, but tolerating other
		# whitespace keeps the parser resilient to a trailing `\n` at EOF.
		while pos < length and text[pos].isspace():
			pos += 1
		if pos >= length:
			break
		try:
			obj, consumed = decoder.raw_decode(text[pos:])
		except json.JSONDecodeError:
			# Not a complete JSON record at `pos`; this is the normal
			# "still being written" tail of the file on the next read.
			break
		if consumed <= 0:
			break
		event = _event_from_obj(obj, raw=text[pos : pos + consumed])
		if event is not None:
			yield event
		pos += consumed


def events_from_file(path: str, offset: int = 0) -> tuple[list[WAFAuditEvent], int]:
	"""Read `path` from `offset`, return (events, new_offset).

	If the file was truncated/rotated (size < offset), start over from 0.
	Only complete JSON transactions are returned; whatever trails the last
	closing brace is left for the next pass (raw_decode refuses to parse a
	partial record). This preserves the exact same `offset` contract the
	watcher's state file relies on across log rotation.
	"""
	try:
		size = os.path.getsize(path)
	except FileNotFoundError:
		return [], 0
	if size < offset:
		# Rotated/truncated — start over.
		offset = 0
	if size == offset:
		return [], offset
	with open(path, "r", errors="replace") as f:
		f.seek(offset)
		text = f.read()

	# Single pass: `_consumed_length` walks the JSON records once and returns
	# both the parsed events and the byte length consumed by complete
	# records, so the new offset leaves any still-being-written tail for the
	# next watcher tick (mirrors the old `\n--boundary-Z--` heuristic but
	# exact, thanks to JSON grammar via `raw_decode`).
	consumed, events = _consumed_length(text)
	# If no complete record was found at all (e.g. logrotate just truncated
	# the file or ModSecurity is mid-write of the very first record), drop
	# the partial bytes so we don't loop forever on the same chunk. This
	# matches the old serial parser's documented "drop incomplete head"
	# trade-off (it advanced `partial_start` to len(text) when no Z marker
	# was found). A subsequent re-read will pick up wherever ModSecurity
	# resumes writing — at worst one partial record is lost, just as before.
	if consumed == 0 and text:
		consumed = len(text)
	new_offset = offset + consumed
	return events, new_offset


def _consumed_length(text: str) -> tuple[int, list[WAFAuditEvent]]:
	"""Return (bytes_consumed_by_complete_records, events) in one pass.

	Trailing incomplete bytes (e.g. a record ModSecurity is still flushing)
	are left for the next watcher tick — `raw_decode` refuses to parse them,
	so `pos` stops at the start of the partial and the caller preserves it.
	"""
	decoder = json.JSONDecoder()
	pos = 0
	length = len(text)
	events: list[WAFAuditEvent] = []
	while pos < length:
		while pos < length and text[pos].isspace():
			pos += 1
		if pos >= length:
			break
		try:
			obj, consumed = decoder.raw_decode(text[pos:])
		except json.JSONDecodeError:
			# Trailing partial record — leave it for the next read.
			break
		if consumed <= 0:
			break
		event = _event_from_obj(obj, raw=text[pos : pos + consumed])
		if event is not None:
			events.append(event)
		pos += consumed
	return pos, events


def _event_from_obj(obj: dict, raw: str) -> WAFAuditEvent | None:
	"""Translate one decoded audit-JSON object into a WAFAuditEvent.

	Defensive: a malformed object (missing top-level `transaction`) is
	dropped, returning None — exactly the contract the previous parser
	used for unusable A/Z blocks.
	"""
	tx = obj.get("transaction")
	if not isinstance(tx, dict):
		return None

	unique_id = str(tx.get("unique_id", ""))
	timestamp = str(tx.get("time_stamp", ""))
	client_ip = str(tx.get("client_ip", ""))

	# X-Real-IP carries the real upstream client on bench nginx; the bench
	# template sets it via `proxy_set_header X-Real-IP $remote_addr;`.
	# Prefer it over the connection-level client_ip when present (and
	# differ from it, as in the cloud LB case).
	headers = (tx.get("request") or {}).get("headers") or {}
	x_real_ip = ""
	for key, value in headers.items():
		if key.lower() == "x-real-ip" and value:
			x_real_ip = str(value)
			break
	if x_real_ip:
		client_ip = x_real_ip

	request = tx.get("request") or {}
	request_method = str(request.get("method", ""))
	request_uri = str(request.get("uri", ""))

	response = tx.get("response") or {}
	http_code = response.get("http_code")
	try:
		status_code = int(http_code) if http_code is not None else None
	except (TypeError, ValueError):
		status_code = None
	action = ""
	if status_code is not None:
		action = "Intercepted" if status_code in _INTERCEPTED_STATUS else "Passed"

	# messages[] carries the matched rules. First one is the headline
	# rule; remaining matches are folded into matched_data so multi-rule
	# transactions stay auditable (matches the previous parser's contract).
	messages = tx.get("messages") or []
	rule_id = ""
	rule_msg = ""
	severity = ""
	matched_bits: list[str] = []
	for i, msg in enumerate(messages):
		if not isinstance(msg, dict):
			continue
		details = msg.get("details") or {}
		msg_text = str(msg.get("message", ""))
		if i == 0:
			rule_id = str(details.get("ruleId", ""))
			rule_msg = msg_text or str(details.get("msg", ""))
			severity = _coerce_severity(details.get("severity"))
			# `match` carries the operator + matched variable summary; this
			# is the closest JSON analogue to the old `Matched Data:` line.
			match_text = str(details.get("match", "")).strip()
			if match_text:
				matched_bits.append(f"Matched: {match_text}")
		else:
			matched_bits.append(
				f"Rule {details.get('ruleId', '?')}: {msg_text}".strip()
			)

	if not unique_id:
		return None

	event = WAFAuditEvent(
		transaction_id=unique_id,
		timestamp=timestamp,
		client_ip=client_ip,
		request_method=request_method,
		request_uri=request_uri,
		rule_id=rule_id,
		rule_msg=rule_msg,
		severity=severity,
		action=action,
		matched_data="\n".join(matched_bits),
		raw_log=raw,
	)
	if status_code is not None:
		event.extras["status_code"] = status_code
	return event


def _coerce_severity(value) -> str:
	"""Map ModSecurity numeric severity → human word used by WAF Log.

	The JSON audit log emits the numeric form ("0".."5"); the old native
	parser emitted words like "WARNING". Press's WAF Log doctype stores the
	word, so we translate here to keep the on-wire payload identical.
	"""
	if value is None:
		return ""
	key = str(value).strip()
	# Already a word (someone pointed a custom rule at a string severity):
	# pass it through uppercased if it matches one of our known words.
	if key.upper() in _SEVERITY_BY_NUM.values():
		return key.upper()
	return _SEVERITY_BY_NUM.get(key, "")
