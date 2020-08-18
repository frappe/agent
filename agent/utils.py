import os
from urllib.parse import urlparse

import requests


def download_file(url, prefix):
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
                total_size += getFolderSize(itempath)

    return total_size
