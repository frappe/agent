import json
import sys
import traceback
from datetime import datetime

from agent.server import Server

if __name__ == "__main__":
    info = []
    server = Server()
    for bench in server.benches.values():
        for site in bench.sites.values():
            try:
                timestamp = str(datetime.utcnow())
                info = {
                    "timestamp": timestamp,
                    "analytics": site.get_analytics(),
                }
                with open(site.analytics_file, "w") as f:
                    json.dump(info, f, indent=1)
            except Exception:
                exception = traceback.format_exc()
                error_log = f"ERROR [{site.name}:{timestamp}]: {exception}"
                print(error_log, file=sys.stderr)
