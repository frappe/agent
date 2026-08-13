"""Backup Site jobs read back out of the agent's job database.

Press keeps its own Site Backup records, but a site that has moved between servers
leaves its older jobs behind on the server it ran them on. This is how those are
read back, per site and bounded to a date range.

The parsing mirrors frappe/fc-scripts/create_backup_log.py, which does the same
thing by opening jobs.sqlite3 directly.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta

from agent.job import JobModel, StepModel

JOB_NAME = "Backup Site"
UPLOAD_STEP = "Upload Site Backup to S3"

# An audit asks about a year at a time, and the cap keeps one call from walking the
# whole job database
MAX_RANGE_DAYS = 366
MAX_JOBS = 2000

ARTIFACTS = ("database", "public", "private", "site_config")

# Backups live at <bench>/sites/<site>/private/backups/, and their url is the site itself
SITE_FROM_PATH = re.compile(r"(?:/sites/|^\./)([^/]+)/private/backups/")
SITE_FROM_URL = re.compile(r"^https?://([^/]+)/")

# "FileNotFoundError: [Errno 2] ...", "subprocess.CalledProcessError: ..."
EXCEPTION_LINE = re.compile(r"^(?:[A-Za-z_][\w.]*\.)?[A-Z]\w*(?:Error|Exception|Exit|Interrupt|Warning)\b.*")

TRACEBACK_NOISE = ('File "', "Traceback", "^", "~", "During handling", "The above exception")


class InvalidRange(Exception):
    """The caller asked for a range that cannot be served."""


def get_backup_jobs(site: str, start: str, end: str) -> list[dict]:
    """Every Backup Site job this server ran for one site, newest first."""
    start_at, end_at = parse_range(start, end)

    jobs = (
        JobModel.select()
        .where(
            (JobModel.name == JOB_NAME)
            # A LIKE narrows the scan; site_of below is what actually decides
            & (JobModel.data.contains(site))
            & (JobModel.start >= start_at)
            & (JobModel.start < end_at)
        )
        .order_by(JobModel.id.desc())
        .limit(MAX_JOBS)
    )

    jobs = list(jobs)
    steps = steps_of(jobs)
    return [
        summarise(job, steps.get(job.id, []))
        for job in jobs
        if site_of(load_json(job.data), steps.get(job.id, [])) == site
    ]


def parse_range(start: str, end: str) -> tuple[datetime, datetime]:
    """Both ends as datetimes, with end pushed to the following midnight so it counts."""
    try:
        start_at = datetime.strptime(start, "%Y-%m-%d")
        end_at = datetime.strptime(end, "%Y-%m-%d")
    except (TypeError, ValueError):
        raise InvalidRange("start and end must be dates formatted YYYY-MM-DD")

    if start_at > end_at:
        raise InvalidRange("start must be on or before end")
    if (end_at - start_at).days >= MAX_RANGE_DAYS:
        raise InvalidRange(f"range must be {MAX_RANGE_DAYS} days or less")

    return start_at, end_at + timedelta(days=1)


def steps_of(jobs: list[JobModel]) -> dict[int, list[tuple]]:
    if not jobs:
        return {}

    steps: dict[int, list[tuple]] = {}
    query = StepModel.select(StepModel.job, StepModel.name, StepModel.status, StepModel.data).where(
        StepModel.job << [job.id for job in jobs]
    )
    for step in query.order_by(StepModel.id):
        steps.setdefault(step.job_id, []).append((step.name, step.status, load_json(step.data)))
    return steps


def summarise(job: JobModel, steps: list[tuple]) -> dict:
    data = load_json(job.data)
    backups = data.get("backups") if isinstance(data, dict) else None

    return {
        "id": job.id,
        "agent_job_id": job.agent_job_id,
        "status": job.status,
        "type": backup_type(job.status, data, steps),
        "start": str(job.start) if job.start else None,
        "end": str(job.end) if job.end else None,
        "duration": str(job.duration) if job.duration else None,
        "sizes": {artifact: size_of(backups, artifact) for artifact in ARTIFACTS},
        # Only the one interpreted line, never the raw traceback
        "failure_reason": failure_reason(data, steps) if job.status != "Success" else "",
    }


def site_of(data: dict | None, steps: list[tuple]) -> str | None:
    """The site a job ran for, read off its backup paths.

    Deliberately not read from the bench command: that is caller-supplied text, and a
    path under the site's own backup directory is the agent's own record of the run.
    """
    backups = data.get("backups") if isinstance(data, dict) else None
    if not isinstance(backups, dict):
        return None

    for entry in backups.values():
        if not isinstance(entry, dict):
            continue
        match = SITE_FROM_PATH.search(entry.get("path") or "")
        if match:
            return match.group(1)
        match = SITE_FROM_URL.match(entry.get("url") or "")
        if match:
            return match.group(1)
    return None


def backup_type(status: str, data: dict | None, steps: list[tuple]) -> str:
    if status != "Success":
        return ""
    if isinstance(data, dict) and data.get("offsite"):
        return "offsite"
    for step_name, step_status, _ in steps:
        if step_name == UPLOAD_STEP and step_status == "Success":
            return "offsite"
    return "onsite"


def size_of(backups: dict | None, artifact: str) -> int:
    entry = (backups or {}).get(artifact)
    if isinstance(entry, dict) and isinstance(entry.get("size"), int):
        return entry["size"]
    return 0


def failure_reason(data: dict | None, steps: list[tuple]) -> str:
    """Why a backup failed, preferring the innermost python exception.

    The step traceback is usually the agent-side CalledProcessError wrapper, while the
    real cause is the last exception in the command output.
    """
    sources = [
        (step_data.get("output"), step_data.get("traceback"))
        for _, step_status, step_data in steps
        if step_status != "Success" and isinstance(step_data, dict)
    ]
    if isinstance(data, dict) and "output" in data:
        sources.append((data.get("output"), data.get("traceback")))

    for output, tb in sources:
        reason = exception_line(output) or exception_line(tb)
        if reason:
            return reason
    for output, tb in sources:
        reason = last_meaningful_line(output) or last_meaningful_line(tb)
        if reason:
            return reason
    return ""


def exception_line(text: str | None) -> str | None:
    for line in meaningful_lines(text):
        if EXCEPTION_LINE.match(line):
            return line
    return None


def last_meaningful_line(text: str | None) -> str | None:
    for line in meaningful_lines(text):
        return line
    return None


def meaningful_lines(text: str | None):
    for line in reversed((text or "").splitlines()):
        line = line.strip()
        if line and not line.startswith(TRACEBACK_NOISE):
            yield line


def load_json(raw: str | None):
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None
