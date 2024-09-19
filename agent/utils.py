from __future__ import annotations

import os
from datetime import datetime, timedelta
from math import ceil
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import requests

if TYPE_CHECKING:
    from typing import Literal, TypedDict

    ExecutionStatus = Literal[
        "Pending",
        "Running",
        "Success",
        "Failure",
    ]

    class ExecutionResult(TypedDict):
        command: str
        directory: str
        start: datetime
        status: ExecutionStatus
        end: datetime | None
        duration: timedelta | None
        output: str | None
        returncode: int | None
        traceback: str | None


def download_file(url, prefix):
    """Download file locally under path prefix and return local path"""
    filename = urlparse(url).path.split("/")[-1]
    local_filename = os.path.join(prefix, filename)

    with requests.get(url, stream=True) as r:
        total_size = int(r.headers.get("content-length", 0))
        chunk_size = 1024 * 1024 if total_size > (100 * 1024 * 1024) else 8192
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                f.write(chunk)

    return local_filename


def get_size(folder):
    """Returns the size of the folder in bytes. Ignores symlinks"""
    total_size = os.path.getsize(folder)

    for item in os.listdir(folder):
        itempath = os.path.join(folder, item)

        if not os.path.islink(itempath):
            if os.path.isfile(itempath):
                total_size += os.path.getsize(itempath)
            elif os.path.isdir(itempath):
                total_size += get_size(itempath)

    return total_size


def cint(x):
    """Convert to integer"""
    try:
        num = int(float(x))
    except Exception:
        num = 0
    return num


def b2mb(x):
    """Return B value in MiB"""
    return ceil(cint(x) / (1024**2))


def get_timestamp():
    try:
        from datetime import UTC, datetime

        return str(datetime.now(UTC))
    except Exception:
        from datetime import datetime

        return str(datetime.utcnow())


def get_execution_result(
    command: str = "",
    directory: str = "",
    start: datetime | None = None,
    status: ExecutionStatus | None = None,
) -> ExecutionResult:
    """returns an ExecutionResult object to manage the output of a Job Step"""
    return {
        "command": command,
        "directory": directory,
        "start": start or datetime.now(),
        "status": status or "Running",
        "output": "",
    }


def end_execution(
    res: ExecutionResult,
    output: str = "",
    status: ExecutionStatus | None = None,
):
    """updates ExecutionResult object `res` fields and returns it"""
    assert res["start"] is not None

    res["end"] = datetime.now()
    res["duration"] = res["end"] - res["start"]
    res["status"] = status or "Success"
    res["output"] = output or res["output"]
    return res
