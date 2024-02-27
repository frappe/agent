import os
from urllib.parse import urlparse
from math import ceil

import requests


def download_file(url, prefix):
    """Download file locally under path prefix and return local path"""
    filename = urlparse(url).path.split("/")[-1]
    local_filename = os.path.join(prefix, filename)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
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
