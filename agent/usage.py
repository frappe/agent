import json
import os
import sys
import traceback
from datetime import datetime

from agent.server import Server


def cstr(text, encoding="utf-8"):
    """Similar to frappe.utils.cstr"""
    if isinstance(text, str):
        return text
    elif text is None:
        return ""
    elif isinstance(text, bytes):
        return str(text, encoding)
    else:
        return str(text)


def get_traceback():
    """Returns the traceback of the Exception"""
    exc_type, exc_value, exc_tb = sys.exc_info()
    trace_list = traceback.format_exception(exc_type, exc_value, exc_tb)
    body = "".join(cstr(t) for t in trace_list)
    return body


if __name__ == "__main__":
    info = []
    server = Server()
    time = datetime.utcnow().isoformat()
    target_file = os.path.join(
        server.directory,
        "logs",
        f"{server.name}-usage-{time}.json.log",
    )

    for bench in server.benches.values():
        for site in bench.sites.values():
            try:
                info.append(
                    {
                        "site": site.name,
                        "timestamp": str(datetime.utcnow()),
                        "timezone": site.timezone,
                        **site.get_usage(),
                    }
                )
            except Exception:
                error_log = f"ERROR [{site.name}:{time}]: {get_traceback()}"
                print(error_log, file=sys.stderr)

    with open(target_file, "w") as f:
        json.dump(info, f, indent=1)
