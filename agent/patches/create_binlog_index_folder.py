from __future__ import annotations

import os


def execute():
    """create the folder to store binlog indexes"""
    path = os.path.join(os.getcwd(), "binlog-indexes")

    if not os.path.exists(path):
        os.mkdir(path)
