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

    if not log_entries:
        return []  # Return empty list if no log entries

    formatted_logs = []
    for entry in log_entries:
        # Extract the timestamp using string operations
        try:
            timestamp_key = '"timestamp":"'
            timestamp_start = entry.index(timestamp_key) + len(timestamp_key)
            timestamp_end = entry.index('"', timestamp_start)
            time = entry[timestamp_start:timestamp_end]
            formatted_time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f%z").strftime("%Y-%m-%d %H:%M:%S")

            formatted_logs.append({"time": formatted_time, "description": entry})
        except ValueError:
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
        log_files.sort(key=self._sort_by_number_suffix, reverse=True)
        # return log_files
        log_entries = self._process_log_files(log_files, page_start, page_length, log_level, search_query)
        # return list([*log_entries, page_start, page_length, log_level, search_query])
        # log_entries = self._apply_filters(log_entries, log_level, search_query)
        log_entries = list(log_entries)

        # if order_by and order_by == "desc":
        #     log_entries.reverse()

        return self.format_log(name, log_entries)
        return self._paginate_entries(name, log_entries, page_start, page_length)

    def _get_log_files(self, name):
        # get all log files including rotated logs
        return [x["name"] for x in self.logs if x["name"].startswith(name)]

    def _sort_by_number_suffix(self, log_file):
        suffix = log_file.split(".")[-1]
        return int(suffix) if suffix.isdigit() else 0

    def _process_log_files(self, log_files, page_start, page_length, log_level=None, search_query=None):
        entries_read = 0
        for log in log_files:
            for entry in self._read_log_file(log, page_start, page_length, log_level, search_query):
                if entries_read >= page_length:
                    break
                entries_read += 1
                yield entry

            page_start = max(0, page_start - entries_read)

    def _read_log_file(self, log, page_start=0, page_length=10, log_level=None, search_query=None):
        LOGS_WITH_MULTI_LINE_ENTRIES = ("database.log", "scheduler.log", "worker", "ipython")

        log_file = os.path.join(self.logs_directory, log)
        with open(log_file) as lf:
            if log.startswith(LOGS_WITH_MULTI_LINE_ENTRIES):
                yield from self._read_multi_line_log(lf, page_start, page_length, log_level, search_query)
            else:
                yield from self._read_single_line_log(lf, page_start, page_length, log_level, search_query)

    def apply_search_or_filter(self, entry, log_level=None, search_query=None):
        if (log_level and log_level not in entry) or (search_query and search_query not in entry):  # noqa: SIM103
            return False
        return True

    def _read_multi_line_log(self, file, page_start, page_length, log_level, search_query):
        """
        Read a log file with multi-line entries.

        If an entry looks like this:
        ```
        2021-09-15 14:48:46,608 ERROR [site] Exception on /api/method/frappe.desk.form.load.getdoc
        Traceback (most recent call last):
        File "/home/frappe/frappe-bench/apps/frappe/frappe/app.py", line 68, in application
        ...
        ```
        This function will read the entire entry as a single log entry.

        """

        entry_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
        entry_regex = re.compile(entry_pattern)

        current_entry = []
        line_count = -1  # start at -1 to account for the first line

        for line in file:
            if line_count >= page_start + page_length:
                break

            # if matches regex pattern, it's a new entry
            # else it's a continuation of the previous entry
            if entry_regex.match(line):
                if current_entry and line_count >= page_start and line_count < page_start + page_length:
                    log_entry = " ".join(current_entry)
                    if self.apply_search_or_filter(log_entry, log_level, search_query):
                        yield log_entry

                current_entry = []
                line_count += 1

            current_entry.append(line.strip())

        # Handle the last entry if it exists
        if current_entry and line_count >= page_start and line_count < page_start + page_length:
            log_entry = " ".join(current_entry)
            if self.apply_search_or_filter(log_entry, log_level, search_query):
                yield log_entry

    def _read_single_line_log(self, lf, page_start=0, page_length=10, log_level=None, search_query=None):
        line_count = 0
        for line in lf:
            if (log_level and log_level not in line) or (search_query and search_query not in line):
                continue
            if line_count >= page_start:
                line_count += 1
                yield line
            else:
                line_count += 1
            if line_count >= page_start + page_length:
                break

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
