import json
import os
from agent.server import Server
from datetime import datetime


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
            info.append(
                {
                    "site": site.name,
                    "timestamp": str(datetime.utcnow()),
                    "timezone": site.timezone,
                    **site.get_usage(),
                }
            )

    with open(target_file, "w") as f:
        json.dump(info, f, indent=1)
