import os
import re
from datetime import datetime
from enum import Enum


class LOG_TYPE(Enum):
    SITE = "site"
    BENCH = "bench"


def bench_log_formatter(log_entries: list) -> list:
    """
    Formats bench logs by extracting timestamp, level, and description.

    Args:
        log_entries (list): A list of log entries, where each entry is a string.

    Returns:
        list: A list of dictionaries, where each dictionary represents a formatted log entry.
    """

    if not log_entries:
        return []  # Return empty list if no log entries

    formatted_logs = []
    for entry in log_entries:
        date, time, level, *description_parts = entry.split(" ")
        description = " ".join(description_parts)

        try:
            formatted_time = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S,%f").strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            formatted_time = ""

        formatted_logs.append({"level": level, "time": formatted_time, "description": description})

    return formatted_logs


def worker_log_formatter(log_entries: list) -> list:
    """
    Formats worker logs by extracting timestamp, level, and description.

    Args:
        log_entries (list): A list of log entries, where each entry is a string.

    Returns:
        list: A list of dictionaries, where each dictionary represents a formatted log entry.
    """

    if not log_entries:
        return []  # Return empty list if no log entries

    formatted_logs = []
    for entry in log_entries:
        date, time, *description_parts = entry.split(" ")
        description = " ".join(description_parts)

        try:
            formatted_time = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S,%f").strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            formatted_time = ""

        formatted_logs.append({"time": formatted_time, "description": description})

    return formatted_logs


def database_log_formatter(log_entries: list) -> list:
    """
    Formats database logs by extracting timestamp, level, and description.

    Args:
        log_entries (list): A list of log entries, where each entry is a string.

    Returns:
        list: A list of dictionaries, where each dictionary represents a formatted log entry.
    """

    if not log_entries:
        return []  # Return empty list if no log entries

    formatted_logs = []
    for entry in log_entries:
        date, time, level, *description_parts = entry.split(" ")
        description = " ".join(description_parts)

        try:
            formatted_time = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S,%f").strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            formatted_time = ""

        formatted_logs.append({"level": level, "time": formatted_time, "description": description})

    return formatted_logs


def scheduler_log_formatter(log_entries: list) -> list:
    """
    Formats scheduler logs by extracting timestamp, level, and description.

    Args:
        log_entries (list): A list of log entries, where each entry is a string.

    Returns:
        list: A list of dictionaries, where each dictionary represents a formatted log entry.
    """

    if not log_entries:
        return []  # Return empty list if no log entries

    formatted_logs = []
    for entry in log_entries:
        date, time, level, *description_parts = entry.split(" ")
        description = " ".join(description_parts)

        try:
            formatted_time = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S,%f").strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            formatted_time = ""

        formatted_logs.append({"level": level, "time": formatted_time, "description": description})

    return formatted_logs


def redis_log_formatter(log_entries: list) -> list:
    """
    Formats redis logs by extracting timestamp, level, and description.

    Args:
        log_entries (list): A list of log entries, where each entry is a string.

    Returns:
        list: A list of dictionaries, where each dictionary represents a formatted log entry.
    """

    if not log_entries:
        return []  # Return empty list if no log entries

    formatted_logs = []
    for entry in log_entries:
        _, day, month, year, time, *description_parts = entry.split(" ")
        description = " ".join(description_parts)

        try:
            formatted_time = datetime.strptime(
                f"{year}-{month}-{day} {time}", "%Y-%b-%d %H:%M:%S.%f"
            ).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            formatted_time = ""

        formatted_logs.append({"time": formatted_time, "description": description})

    return formatted_logs


def web_error_log_formatter(log_entries: list) -> list:
    """
    Formats web error logs by extracting timestamp, level, and description.

    Args:
        log_entries (list): A list of log entries, where each entry is a string.

    Returns:
        list: A list of dictionaries, where each dictionary represents a formatted log entry.
    """

    if not log_entries:
        return []  # Return empty list if no log entries

    # Regular expression pattern to match log entries specific to web.error logs
    regex = r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4})\] \[(\d+)\] \[(\w+)\] (.*)"

    formatted_logs = []
    for entry in log_entries:
        match = re.match(regex, entry)
        if not match:
            formatted_logs.append({"description": entry})  # Unparsable entry
            continue

        # Extract groups from the match
        date, _, level, description_parts = match.groups()
        description = "".join(description_parts)

        try:
            formatted_time = datetime.strptime(date, "%Y-%m-%d %H:%M:%S %z").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            formatted_time = ""

        formatted_logs.append({"level": level, "time": formatted_time, "description": description})

    return formatted_logs


def monitor_json_log_formatter(log_entries: list) -> list:
    """
    Formats monitor.json logs by extracting timestamp, level, and description.

    Args:
        log_entries (list): A list of log entries, where each entry is a string.

    Returns:
        list: A list of dictionaries, where each dictionary represents a formatted log entry.
    """

    import json

    if not log_entries:
        return []  # Return empty list if no log entries

    formatted_logs = []
    for entry in log_entries:
        # parse the json log entry
        try:
            log_entry = json.loads(entry)
            time = log_entry.get("timestamp")
            formatted_time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f%z").strftime("%Y-%m-%d %H:%M:%S")

            formatted_logs.append({"time": formatted_time, "description": entry})
        except json.JSONDecodeError:
            formatted_logs.append({"description": entry})

    return formatted_logs


def ipython_log_formatter(log_entries: list) -> list:
    """
    Formats ipython logs by extracting timestamp, level, and description.

    Args:
        log_entries (list): A list of log entries, where each entry is a string.

    Returns:
        list: A list of dictionaries, where each dictionary represents a formatted log entry.
    """

    if not log_entries:
        return []  # Return empty list if no log entries

    formatted_logs = []
    for entry in log_entries:
        date, time, level, *description_parts = entry.split(" ")
        description = " ".join(description_parts)

        try:
            formatted_time = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S,%f").strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            formatted_time = ""

        formatted_logs.append({"level": level, "time": formatted_time, "description": description})

    return formatted_logs


def fallback_log_formatter(log_entries: list) -> list:
    """
    Fallback formatter for logs that don't have a specific formatter.

    Args:
        log_entries (list): A list of log entries, where each entry is string.

    Returns:
        list: A list of dictionaries, where each dictionary represents a formatted log entry.
    """

    formatted_logs = []
    for entry in log_entries:
        formatted_logs.append({"description": entry})

    return formatted_logs


class LogBrowser:
    from typing import ClassVar

    FORMATTER_MAP: ClassVar[dict] = {
        "bench.log": bench_log_formatter,
        "worker.log": worker_log_formatter,
        "ipython.log": ipython_log_formatter,
        "database.log": database_log_formatter,
        "redis-cache.log": redis_log_formatter,
        "redis-queue.log": redis_log_formatter,
        "scheduler.log": scheduler_log_formatter,
        "web.error.log": web_error_log_formatter,
        "worker.error.log": worker_log_formatter,
        "monitor.json.log": monitor_json_log_formatter,
    }

    LOGS_WITH_MULTI_LINE_ENTRIES = ("database.log", "scheduler.log", "worker", "ipython")

    def get_log_key(self, log_name: str) -> str:
        # if the log file has a number at the end, it's a rotated log
        # and we don't need to consider the number for formatter mapping
        if log_name[-1].isdigit():
            log_name = log_name.rsplit(".", 1)[0]

        return log_name

    def format_log(self, log_name: str, log_entries: list) -> list:
        log_key = self.get_log_key(log_name)
        if log_key in self.FORMATTER_MAP:
            return self.FORMATTER_MAP[log_key](log_entries)
        return fallback_log_formatter(log_entries)

    def __init__(self, logs_directory):
        self.logs_directory = logs_directory

    def retrieve_log(self, name):
        if name not in {x["name"] for x in self.logs}:
            return ""
        log_file = os.path.join(self.logs_directory, name)
        with open(log_file) as lf:
            return lf.read()

    def retrieve_merged_log(
        self, name, page_start=0, page_length=10, log_level=None, search_query=None, order_by=None
    ):
        log_files = self._get_log_files(name)
        if not log_files:
            return ""

        # Sort log files to ensure correct order of rotated logs
        log_files.sort(key=self._sort_by_number_suffix)
        log_entries = self._process_log_files(log_files)
        log_entries = self._apply_filters(log_entries, log_level, search_query)
        log_entries = list(log_entries)

        if order_by and order_by == "desc":
            log_entries.reverse()

        return self._paginate_entries(name, log_entries, page_start, page_length)

    def _get_log_files(self, name):
        # get all log files including rotated logs
        return [x["name"] for x in self.logs if x["name"].startswith(name)]

    def _sort_by_number_suffix(self, log_file):
        suffix = log_file.split(".")[-1]
        return int(suffix) if suffix.isdigit() else 0

    def _process_log_files(self, log_files):
        for log in log_files:
            yield from self._read_log_file(log)

    def _read_log_file(self, log):
        log_file = os.path.join(self.logs_directory, log)
        with open(log_file) as lf:
            if log.startswith(self.LOGS_WITH_MULTI_LINE_ENTRIES):
                buffer = []
                for line in lf:
                    if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", line) and buffer:
                        yield "".join(buffer)
                        buffer = []
                    buffer.append(line)
                if buffer:
                    yield "".join(buffer)
            else:
                for line in lf:
                    yield line.strip()

    def _apply_filters(self, log_entries, log_level=None, search_query=None):
        if log_level:
            log_entries = (entry for entry in log_entries if log_level in entry)

        if search_query:
            log_entries = (entry for entry in log_entries if search_query in entry)

        return log_entries

    def _paginate_entries(self, name, log_entries, page_start, page_length):
        page_end = page_start + page_length
        log_entries = self.format_log(name, log_entries)
        return log_entries[page_start:page_end]

    @property
    def logs(self):
        def path(file):
            return os.path.join(self.logs_directory, file)

        def modified_time(file):
            return os.path.getctime(path(file))

        try:
            log_files = sorted(
                os.listdir(self.logs_directory),
                key=modified_time,
                reverse=True,
            )
            payload = []

            for x in log_files:
                stats = os.stat(path(x))
                payload.append(
                    {
                        "name": x,
                        "size": stats.st_size / 1000,
                        "created": str(datetime.fromtimestamp(stats.st_ctime)),
                        "modified": str(datetime.fromtimestamp(stats.st_mtime)),
                    }
                )

            return payload

        except FileNotFoundError:
            return []
