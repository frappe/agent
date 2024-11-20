# log_browser.py
import os
from datetime import datetime


class LogBrowser:
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

        return self._paginate_entries(log_entries, page_start, page_length)

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
            yield from lf

    def _apply_filters(self, log_entries, log_level=None, search_query=None):
        if log_level:
            log_entries = (entry for entry in log_entries if log_level in entry)

        if search_query:
            log_entries = (entry for entry in log_entries if search_query in entry)

        return log_entries

    def _paginate_entries(self, log_entries, page_start, page_length):
        page_end = page_start + page_length
        return "".join(log_entries[page_start:page_end])

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
