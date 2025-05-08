from __future__ import annotations

import os
import sys
import traceback
from datetime import datetime

import filelock

from agent.proxy import Proxy


def cstr(text, encoding="utf-8"):
    """Similar to frappe.utils.cstr"""
    if isinstance(text, str):
        return text
    if text is None:
        return ""
    if isinstance(text, bytes):
        return str(text, encoding)
    return str(text)


def get_traceback():
    """Returns the traceback of the Exception"""
    exc_type, exc_value, exc_tb = sys.exc_info()
    trace_list = traceback.format_exception(exc_type, exc_value, exc_tb)
    return "".join(cstr(t) for t in trace_list)


if __name__ == "__main__":
    proxy = Proxy()

    with filelock.FileLock(proxy.nginx_defer_reload_lock_file):
        if not os.path.exists(proxy.nginx_defer_reload_file):
            sys.exit(0)

        # check if file has 1 as content
        reload_required = False
        with open(proxy.nginx_defer_reload_file, "r") as f:
            content = f.read().strip()
            if content == "1":
                reload_required = True

        if reload_required:
            try:
                proxy._generate_proxy_config()
                if not proxy.is_nginx_worker_shutting_down():
                    proxy._reload_nginx()
            except Exception:
                error_log = f"ERROR [{proxy.name}:{datetime.utcnow()}]: {get_traceback()}"
                print(error_log, file=sys.stderr)

        os.remove(proxy.nginx_defer_reload_file)
