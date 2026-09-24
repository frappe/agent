from __future__ import annotations

import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import requests

from agent.utils import download_file

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
