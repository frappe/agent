import os
from agent.server import Server
from pathlib import Path
from datetime import datetime
import re


class DatabaseServer(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]

        self.mariadb_directory = "/var/lib/mysql"

    def search_binary_log(
        self,
        log,
        database,
        start_datetime,
        stop_datetime,
        search_pattern,
        max_lines,
    ):
        log = os.path.join(self.mariadb_directory, log)
        command = (
            f"mysqlbinlog --short-form --database {database} "
            f"--start-datetime '{start_datetime}' "
            f"--stop-datetime '{stop_datetime}' "
            f" {log} | grep '{search_pattern}' "
            f"| head -n {max_lines}"
        )

        DELIMITER = "/*!*/;"
        LINES_TO_SKIP = ["SET", "USE", "COMMIT", "START TRANSACTION", "/*!"]

        events = []
        timestamp = 0
        for line in self.execute(command)["output"].split(DELIMITER):
            line = line.strip()
            if line.startswith("SET TIMESTAMP"):
                timestamp = int(line.split("=")[-1].split(".")[0])
            else:
                if any(
                    line.upper().startswith(skip) for skip in LINES_TO_SKIP
                ):
                    continue
                else:
                    events.append(
                        {
                            "query": line,
                            "timestamp": str(
                                datetime.utcfromtimestamp(timestamp)
                            ),
                        }
                    )
        return events

    @property
    def binary_logs(self):
        BINARY_LOG_FILE_PATTERN = r"mysql-bin.\d+"
        BINARY_LOG_FILE_PATTERN = r"mariadb-bin.\d+"
        files = []
        for file in Path(self.mariadb_directory).iterdir():
            if re.match(BINARY_LOG_FILE_PATTERN, file.name):
                unix_timestamp = int(file.stat().st_mtime)
                files.append(
                    {
                        "name": file.name,
                        "size": file.stat().st_size,
                        "modified": str(
                            datetime.utcfromtimestamp(unix_timestamp)
                        ),
                    }
                )
        return sorted(files, key=lambda x: x["name"])
