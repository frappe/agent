import os
from urllib.parse import urlparse
from math import ceil

import requests


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
