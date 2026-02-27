from __future__ import annotations

import hashlib
import os
import re
import struct
import subprocess
from collections import defaultdict
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


def format_size(bytes_val):
    thresholds = [(1024**3, "GB"), (1024**2, "MB"), (1024, "KB")]

    for factor, suffix in thresholds:
        if bytes_val >= factor:
            value = bytes_val / factor
            return f"{value:.2f}{suffix}"

    return f"{bytes_val}B"


def to_bytes(size_str: str) -> float:
    size_str = size_str.strip().upper()
    units = [("GB", 1024**3), ("MB", 1024**2), ("KB", 1024), ("B", 1)]
    for suffix, factor in units:
        if size_str.endswith(suffix):
            return float(size_str.replace(suffix, "").strip()) * factor
    return 0


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


def get_size(folder, ignore_dirs=None):
    """Returns the size of the folder in bytes. Ignores symlinks"""
    total_size = os.path.getsize(folder)

    if ignore_dirs is None:
        ignore_dirs = []

    for item in os.listdir(folder):
        itempath = os.path.join(folder, item)

        if item in ignore_dirs:
            continue

        if not os.path.islink(itempath):
            if os.path.isfile(itempath):
                total_size += os.path.getsize(itempath)
            elif os.path.isdir(itempath):
                total_size += get_size(itempath)

    return total_size


def is_registry_healthy(url: str, username: str, password: str) -> bool:
    """Check if production registry (only) is healthy in the push cycle"""
    headers = {"Accept": "application/vnd.docker.distribution.manifest.v2+json"}

    if url != "registry.frappe.cloud":
        return True

    response = requests.get(f"https://{url}/v2", auth=(username, password), headers=headers)

    return response.ok


def cint(x):
    """Convert to integer"""
    if x is None:
        return 0
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


def compute_file_hash(file_path, algorithm="sha256", raise_exception=True):
    try:
        """Compute the hash of a file using the specified algorithm."""
        hash_func = hashlib.new(algorithm)

        with open(file_path, "rb") as file:
            # read in 10MB chunks
            while chunk := file.read(10000000):
                hash_func.update(chunk)

        return hash_func.hexdigest()
    except FileNotFoundError:
        if raise_exception:
            raise
        return "File does not exist"
    except Exception:
        if raise_exception:
            raise
        return "Failed to compute hash"


def decode_mariadb_filename(filename: str) -> str:
    """
    Decode MariaDB encoded filenames that use @XXXX format for special characters.
    """

    def _hex_to_char(match: re.Match) -> str:
        # Convert the hex value after @ to its character representation
        hex_value = match.group(1)
        return chr(int(hex_value, 16))

    # Find @XXXX patterns and replace them with their character equivalents
    return re.sub(r"@([0-9A-Fa-f]{4})", _hex_to_char, filename)


def get_mariadb_table_name_from_path(path: str) -> str:
    """
    Extract the table name from a MariaDB table file path.
    """
    # Extract the filename from the path
    filename = os.path.basename(path)
    if not filename:
        return ""
    # Remove the extension
    filename = os.path.splitext(filename)[0]
    # Decode the filename
    return decode_mariadb_filename(filename)


def check_installed_pyspy(server_dir: str) -> bool:
    return os.path.exists(os.path.join(server_dir, "env/bin/py-spy"))


def get_supervisor_processes_status() -> dict[str, str | dict[str, str]]:
    try:
        output = subprocess.check_output("sudo supervisorctl status all", shell=True)
        lines = output.decode("utf-8").strip().split("\n")

        flat_status = {}

        for line in lines:
            parts = line.split()
            if len(parts) < 2:
                continue
            name = parts[0].strip()
            state = parts[1].strip()

            if not name.startswith("agent:"):
                continue

            # Strip `agent:` prefix if present
            name = name[len("agent:") :]

            flat_status[name] = state

        nested_status = defaultdict(dict)

        for name, state in flat_status.items():
            # Match pattern like worker-1, worker-2, etc.
            if "-" in name:
                group, sub = name.split("-", 1)
                nested_status[group][sub] = state
            else:
                nested_status[name] = state

        return dict(nested_status)
    except Exception:
        return {}


def parse_fts_index_prefixlen_from_cfg(file_path: str) -> dict[str, int]:  # noqa: C901
    try:
        with open(file_path, "rb") as f:
            data = f.read()
    except OSError as e:
        raise RuntimeError(f"Failed to read FTS index config file '{file_path}': {e}") from e

    current_pos = 0

    def _u32():
        nonlocal current_pos
        v = struct.unpack_from(">I", data, current_pos)[0]
        current_pos += 4
        return v

    def _skip(n):
        nonlocal current_pos
        current_pos += n

    def _read_string():
        length = _u32()  # length includes NUL byte
        nonlocal current_pos
        s = data[current_pos : current_pos + length].rstrip(b"\x00").decode()
        current_pos += length
        return s

    _skip(4)  # cfg version (IB_EXPORT_CFG_VERSION_V1 = 1)
    _read_string()  # hostname where tablespace was exported
    _read_string()  # table name
    _skip(8)  # autoinc value (u64)
    _skip(4)  # logical page size
    _skip(4)  # table flags

    # skip column definitions: row_import_read_columns()
    # each column is 7 x u32 (prtype, mtype, len, mbminmaxlen, ind, ord_part, max_prefix) + name (string)
    n_cols = _u32()
    for _ in range(n_cols):
        _skip(7 * 4)
        _read_string()  # column name

    # read index definitions: row_import_read_index_data()
    no_of_indexes = _u32()
    result = {}

    for _ in range(no_of_indexes):
        _skip(8)  # index id (u64), mach_read_from_8()
        _skip(4)  # space
        _skip(4)  # page_no
        index_type = _u32()  # type  (32 = FTS, 3 = clustered, etc.)
        _skip(4)  # trx_id_offset
        _skip(4)  # n_user_defined_cols
        _skip(4)  # n_uniq
        _skip(4)  # n_nullable
        n_fields = _u32()  # n_fields
        index_name = _read_string()

        # read fields: row_import_cfg_read_index_fields()
        # each field: prefix_len (u32), fixed_len (u32), name string
        for _ in range(n_fields):
            # lower 12 bits are the actual prefix_len (field->prefix_len & ((1U<<12)-1))
            prefix_len = _u32() & ((1 << 12) - 1)
            _skip(4)  # fixed_len: bit31=descending flag, lower 10 bits=fixed_len
            _read_string()  # field name

        # prefix_len of the last field - for FTS indexes there is only one user field
        if index_type == 32 and n_fields == 1:
            result[index_name] = prefix_len

    return result
