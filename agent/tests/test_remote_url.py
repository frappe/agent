from __future__ import annotations

import io
import os
import shutil
import socket
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

import requests
from urllib3.connectionpool import HTTPSConnectionPool

from agent import remote_url
from agent.remote_url import RemoteURLError, _PinnedIPAdapter
from agent.utils import download_file


def resolve_to(*ips):
    """Fake socket.getaddrinfo that resolves any host to `ips`."""
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0)) for ip in ips]


def resolve_by_host(hosts):
    """Fake socket.getaddrinfo that resolves each host in `hosts` to its IP."""
    return lambda hostname, *args, **kwargs: resolve_to(hosts[hostname])


def fake_responses(*responses):
    """Fake HTTPAdapter.send that answers with (status, Location) in turn, and records each Host."""
    hosts = []
    queue = list(responses)

    def send(self, request, *args, **kwargs):
        hosts.append(request.headers["Host"])
        status, location = queue.pop(0)
        response = requests.Response()
        response.status_code = status
        response.raw = io.BytesIO(b"backup")
        if location:
            response.headers["Location"] = location
        return response

    return send, hosts


class TestDownloadFile(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.directory)

    def test_download_follows_redirect_to_a_public_host(self):
        send, hosts = fake_responses((302, "https://bucket.example.com/backup.sql.gz"), (200, None))
        with patch(
            "agent.remote_url.socket.getaddrinfo",
            resolve_by_host({"site.example.com": "93.184.216.34", "bucket.example.com": "93.184.216.35"}),
        ), patch("requests.adapters.HTTPAdapter.send", send):
            path = download_file("https://site.example.com/backup.sql.gz", prefix=self.directory)

        self.assertEqual(hosts, ["site.example.com", "bucket.example.com"])
        with open(path, "rb") as f:
            self.assertEqual(f.read(), b"backup")

    def test_download_refuses_redirect_that_fails_the_check_before_requesting_it(self):
        send, hosts = fake_responses((302, "http://files.example.com/backup.sql.gz"), (200, None))
        with patch(
            "agent.remote_url.socket.getaddrinfo",
            resolve_by_host({"site.example.com": "93.184.216.34", "files.example.com": "10.0.0.5"}),
        ), patch("requests.adapters.HTTPAdapter.send", send), self.assertRaisesRegex(
            RemoteURLError, "not public"
        ):
            download_file("https://site.example.com/backup.sql.gz", prefix=self.directory)

        self.assertEqual(hosts, ["site.example.com"])
        self.assertEqual(os.listdir(self.directory), [])

    def test_download_gives_up_after_too_many_redirects(self):
        send, hosts = fake_responses(*[(302, "/again")] * (remote_url.MAX_REDIRECTS + 1))
        with patch("agent.remote_url.socket.getaddrinfo", return_value=resolve_to("93.184.216.34")), patch(
            "requests.adapters.HTTPAdapter.send", send
        ), self.assertRaisesRegex(RemoteURLError, "redirects too many times"):
            download_file("https://site.example.com/backup.sql.gz", prefix=self.directory)

        self.assertEqual(len(hosts), remote_url.MAX_REDIRECTS + 1)

    def test_download_refuses_url_that_is_not_http(self):
        for url in ("file:///tmp/backup.sql.gz", "ftp://example.com/backup.sql.gz"):
            with self.subTest(url=url), self.assertRaisesRegex(RemoteURLError, "http and https"):
                download_file(url, prefix=self.directory)

    def test_download_refuses_host_with_any_address_that_is_not_public(self):
        for ips in (("10.0.0.5",), ("93.184.216.34", "127.0.0.1"), ("::ffff:127.0.0.1",)):
            with self.subTest(ips=ips), patch(
                "agent.remote_url.socket.getaddrinfo", return_value=resolve_to(*ips)
            ), patch("requests.adapters.HTTPAdapter.send") as send, self.assertRaisesRegex(
                RemoteURLError, "not public"
            ):
                download_file("https://files.example.com/backup.sql.gz", prefix=self.directory)
            send.assert_not_called()

    def test_download_refuses_host_that_does_not_resolve(self):
        with patch(
            "agent.remote_url.socket.getaddrinfo", side_effect=socket.gaierror
        ), self.assertRaisesRegex(RemoteURLError, "Could not resolve"):
            download_file("https://missing.example.com/backup.sql.gz", prefix=self.directory)

    def test_download_connects_to_the_checked_address_and_keeps_the_hostname(self):
        """A real request: only the check can resolve the name, so it arrives only through the pinned IP."""
        received = {}
        getaddrinfo = socket.getaddrinfo

        def resolve(host, port, *args, **kwargs):
            if host == "files.example.com" and port is None:
                return resolve_to("127.0.0.1")
            if host == "127.0.0.1":
                return getaddrinfo(host, port, *args, **kwargs)
            raise socket.gaierror

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                received["host"] = self.headers["Host"]
                self.send_response(200)
                self.send_header("Content-Length", "6")
                self.end_headers()
                self.wfile.write(b"backup")

            def log_message(self, format, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        port = server.server_address[1]
        try:
            with patch("agent.remote_url.socket.getaddrinfo", resolve), patch(
                "agent.remote_url._is_public", return_value=True
            ):
                path = download_file(f"http://files.example.com:{port}/backup.sql.gz", prefix=self.directory)
        finally:
            server.shutdown()
            server.server_close()

        self.assertEqual(received["host"], f"files.example.com:{port}")
        with open(path, "rb") as f:
            self.assertEqual(f.read(), b"backup")


class TestPinnedIPAdapter(unittest.TestCase):
    def test_https_request_goes_to_the_ip_and_checks_the_certificate_of_the_hostname(self):
        adapter = _PinnedIPAdapter("93.184.216.34", "bucket.example.com")
        captured = {}

        def capture(self, request, *args, **kwargs):
            captured["url"] = request.url
            captured["host"] = request.headers["Host"]
            pool = adapter.get_connection(request.url)
            assert isinstance(pool, HTTPSConnectionPool)
            captured["assert_hostname"] = pool.assert_hostname
            captured["server_hostname"] = pool.conn_kw["server_hostname"]
            response = requests.Response()
            response.status_code = 200
            return response

        with requests.Session() as session, patch("requests.adapters.HTTPAdapter.send", capture):
            session.mount("https://", adapter)
            session.get("https://user:secret@bucket.example.com/backup.sql.gz", allow_redirects=False)

        self.assertTrue(captured["url"].startswith("https://93.184.216.34/"))
        self.assertEqual(captured["host"], "bucket.example.com")
        self.assertEqual(captured["assert_hostname"], "bucket.example.com")
        self.assertEqual(captured["server_hostname"], "bucket.example.com")
