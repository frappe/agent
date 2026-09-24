from __future__ import annotations

import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import requests

from agent.bench import Bench
from agent.utils import download_file, format_progress

BODY = b"x" * (256 * 1024)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", str(len(BODY)))
        self.end_headers()
        if self.path.startswith("/stall"):
            self.wfile.write(BODY[:1024])
            self.wfile.flush()
            time.sleep(5)
            return
        self.wfile.write(BODY)

    def log_message(self, *args):
        pass


class TestDownloadFile(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.directory = tempfile.mkdtemp()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_download_that_stops_sending_bytes_raises_after_read_timeout(self):
        start = time.monotonic()
        with patch("agent.utils.DOWNLOAD_TIMEOUT", (1, 1)), self.assertRaises(requests.ConnectionError):
            download_file(f"{self.base_url}/stall.sql.gz", self.directory)
        self.assertLess(time.monotonic() - start, 4)

    def test_download_writes_the_whole_body(self):
        path = download_file(f"{self.base_url}/backup.sql.gz", self.directory)
        with open(path, "rb") as f:
            self.assertEqual(f.read(), BODY)

    def test_download_reports_progress_while_running_and_the_full_size_at_the_end(self):
        reports = []
        with patch("agent.utils.DOWNLOAD_PROGRESS_INTERVAL", 0):
            download_file(
                f"{self.base_url}/backup.sql.gz", self.directory, lambda *args: reports.append(args)
            )
        self.assertGreater(len(reports), 2)
        self.assertEqual(reports[-1], (len(BODY), len(BODY)))

    def test_bench_download_publishes_one_progress_line_per_file(self):
        bench = Bench.__new__(Bench)
        lines = []
        with patch.object(Bench, "publish_data") as publish_data:
            bench.download_with_progress(f"{self.base_url}/backup.sql.gz", self.directory, "Database", lines)
            bench.download_with_progress("", self.directory, "Private files", lines)
            bench.download_with_progress(f"{self.base_url}/public.tar", self.directory, "Public files", lines)
        database, public = publish_data.call_args.args[0].split("\n")
        self.assertRegex(database, r"^Database: 256\.00KB .* 100% ETA 0:00:00$")
        self.assertRegex(public, r"^Public files: 256\.00KB .* 100% ETA 0:00:00$")


class TestFormatProgress(unittest.TestCase):
    def test_progress_shows_size_time_rate_bar_percent_and_eta_like_pv(self):
        self.assertEqual(
            format_progress(750 * 1024**2, 1000 * 1024**2, 250, width=10),
            "750.00MB 0:04:10 [3.00MB/s] [=======>  ] 75% ETA 0:01:23",
        )

    def test_progress_without_content_length_has_no_bar(self):
        self.assertEqual(format_progress(5 * 1024**2, 0, 5), "5.00MB 0:00:05 [1.00MB/s]")

    def test_progress_before_first_byte_does_not_divide_by_zero(self):
        self.assertTrue(format_progress(0, 1024, 0).endswith("0% ETA ?"))
