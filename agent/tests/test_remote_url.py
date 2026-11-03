from __future__ import annotations

import datetime
import io
import ipaddress
import os
import shutil
import socket
import ssl
import tempfile
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from agent import remote_url
from agent.remote_url import RemoteURLError
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


def resolve_only_for_the_check(hostname, ip="127.0.0.1"):
    """Fake socket.getaddrinfo where only the check can resolve `hostname`, so a request reaches `ip`
    only through the pinned address."""
    getaddrinfo = socket.getaddrinfo

    def resolve(host, port, *args, **kwargs):
        if host == hostname and port is None:
            return resolve_to(ip)
        if host == ip:
            return getaddrinfo(host, port, *args, **kwargs)
        raise socket.gaierror

    return resolve


@contextmanager
def local_server(received, tls_context=None):
    """Serve b"backup" on 127.0.0.1, record the Host header in `received`, and yield the port."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            received["host"] = self.headers["Host"]
            self.send_response(200)
            self.send_header("Content-Length", "6")
            self.end_headers()
            self.wfile.write(b"backup")

        def log_message(self, format, *args):
            pass

    class Server(HTTPServer):
        def handle_error(self, request, client_address):
            pass  # A client that rejects the certificate drops the connection

    server = Server(("127.0.0.1", 0), Handler)
    if tls_context:
        server.socket = tls_context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()


def _certificate(subject, issuer, public_key, signing_key, *, ca, names=()):
    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
    )
    if names:
        builder = builder.add_extension(x509.SubjectAlternativeName(list(names)), critical=False)
    return builder.sign(signing_key, hashes.SHA256())


def write_certificates(directory, name):
    """Write a CA and a server certificate for `name`. Return the CA path and a server context."""
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test CA")])
    ca = _certificate(ca_name, ca_name, ca_key.public_key(), ca_key, ca=True)

    try:
        san = x509.IPAddress(ipaddress.ip_address(name))
    except ValueError:
        san = x509.DNSName(name)
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    certificate = _certificate(subject, ca_name, key.public_key(), ca_key, ca=False, names=[san])

    ca_path = os.path.join(directory, "ca.pem")
    with open(ca_path, "wb") as f:
        f.write(ca.public_bytes(serialization.Encoding.PEM))
    chain_path = os.path.join(directory, "server.pem")
    with open(chain_path, "wb") as f:
        f.write(certificate.public_bytes(serialization.Encoding.PEM))
        f.write(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(chain_path)
    return ca_path, context


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
        with local_server(received) as port, patch(
            "agent.remote_url.socket.getaddrinfo", resolve_only_for_the_check("files.example.com")
        ), patch("agent.remote_url._is_public", return_value=True):
            path = download_file(f"http://files.example.com:{port}/backup.sql.gz", prefix=self.directory)

        self.assertEqual(received["host"], f"files.example.com:{port}")
        with open(path, "rb") as f:
            self.assertEqual(f.read(), b"backup")


class TestDownloadFileOverTLS(unittest.TestCase):
    """Real TLS handshakes on 127.0.0.1, with a test CA the client trusts."""

    def setUp(self):
        self.directory = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.directory)

    def download(self, certificate_name, received, sni):
        ca_path, context = write_certificates(self.directory, certificate_name)
        context.sni_callback = lambda ssl_socket, server_name, ssl_context: sni.append(server_name)
        with local_server(received, context) as port, patch(
            "agent.remote_url.socket.getaddrinfo", resolve_only_for_the_check("bucket.example.com")
        ), patch("agent.remote_url._is_public", return_value=True), patch.dict(
            os.environ, {"REQUESTS_CA_BUNDLE": ca_path}
        ):
            return download_file(
                f"https://user:secret@bucket.example.com:{port}/backup.sql.gz", prefix=self.directory
            ), port

    def test_download_accepts_a_trusted_certificate_for_the_hostname(self):
        received, sni = {}, []
        path, port = self.download("bucket.example.com", received, sni)

        self.assertEqual(sni, ["bucket.example.com"])
        self.assertEqual(received["host"], f"bucket.example.com:{port}")
        with open(path, "rb") as f:
            self.assertEqual(f.read(), b"backup")

    def test_download_rejects_a_trusted_certificate_for_another_name(self):
        # 127.0.0.1 is the pinned address: its certificate must not pass for the hostname either
        for name in ("other.example.com", "127.0.0.1"):
            with self.subTest(name=name), self.assertRaisesRegex(requests.exceptions.SSLError, "match"):
                self.download(name, {}, [])
