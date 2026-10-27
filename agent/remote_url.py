"""Open URLs that Press passes on from a customer, such as backup files to restore.

Each request goes only to a public address. The host is resolved once, and the
connection is pinned to the address that was checked. Redirects are followed by
hand, and each target is checked the same way.
"""

from __future__ import annotations

import ipaddress
import socket
from contextlib import contextmanager
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.connectionpool import HTTPSConnectionPool

if TYPE_CHECKING:
    from collections.abc import Iterator

MAX_REDIRECTS = 3


class RemoteURLError(Exception):
    """The URL is not http(s), cannot be resolved, or resolves to an address that is not public."""


@contextmanager
def open_remote_url(url: str) -> Iterator[requests.Response]:
    """Stream a GET of `url`, following up to MAX_REDIRECTS redirects."""
    for _ in range(MAX_REDIRECTS + 1):
        hostname = _hostname(url)
        ip = _resolve_public_ip(hostname)
        with requests.Session() as session:
            session.mount(f"{urlsplit(url).scheme}://", _PinnedIPAdapter(ip, hostname))
            with session.get(url, stream=True, allow_redirects=False) as response:
                if not response.is_redirect:
                    yield response
                    return
                location = response.headers["Location"]
        url = urljoin(url, location)
    raise RemoteURLError("The URL redirects too many times.")


def _hostname(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise RemoteURLError("Only http and https URLs are allowed.")
    if not parts.hostname:
        raise RemoteURLError("The URL has no host.")
    return parts.hostname


def _resolve_public_ip(hostname: str) -> str:
    """Resolve `hostname`, refusing if any address it maps to is not public."""
    try:
        addresses = socket.getaddrinfo(hostname, None)
    except socket.gaierror as error:
        raise RemoteURLError(f"Could not resolve the host '{hostname}'.") from error

    ips = [str(address[4][0]) for address in addresses]
    for ip in ips:
        if not _is_public(ip):
            raise RemoteURLError("The URL resolves to an address that is not public.")
    # Prefer IPv4, as the server may not have IPv6 egress
    return next((ip for ip in ips if ipaddress.ip_address(ip).version == 4), ips[0])


def _is_public(ip: str) -> bool:
    address = ipaddress.ip_address(ip)
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        return address.ipv4_mapped.is_global
    return address.is_global


def _authority(host: str, port: int | None) -> str:
    """Format host and port as a URL authority, bracketing an IPv6 literal."""
    host = f"[{host}]" if ":" in host else host
    return f"{host}:{port}" if port else host


class _PinnedIPAdapter(HTTPAdapter):
    """Connect to the checked IP, and keep the Host header and the TLS check on the hostname."""

    def __init__(self, ip: str, hostname: str, **kwargs):
        self.ip = ip
        self.hostname = hostname
        self.is_https = False
        super().__init__(**kwargs)

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        parts = urlsplit(str(request.url))
        self.is_https = parts.scheme == "https"
        # Build both authorities from host and port only, so userinfo does not go into the Host header
        request.headers["Host"] = _authority(self.hostname, parts.port)
        request.url = urlunsplit(parts._replace(netloc=_authority(self.ip, parts.port)))
        return super().send(request, stream, timeout, verify, cert, proxies)

    def get_connection(self, url, proxies=None):
        pool = super().get_connection(url, proxies)
        if self.is_https and isinstance(pool, HTTPSConnectionPool):
            # urllib3 sends this name for SNI and checks the certificate against it
            pool.conn_kw["server_hostname"] = self.hostname
        return pool
