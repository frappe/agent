from __future__ import annotations

import ipaddress
import os
import posixpath
import stat


MAIN_DOCKER_SOCKETS = {"/var/run/docker.sock", "/run/docker.sock"}
BUILD_ONLY_ENDPOINTS = {
    ("ping", "GET"),
    ("get_version", "GET"),
    ("build_image", "POST"),
    ("jobs", "GET"),
    ("cancel_job", "POST"),
}


def validate_build_only_bind_address(address: str | None) -> str:
    """Accept only a specific IPv4 address from Tailscale's CGNAT range."""
    try:
        parsed = ipaddress.ip_address(address or "")
    except ValueError as exc:
        raise ValueError("Build-only mode requires a specific Tailnet IPv4 bind address.") from exc
    if (
        parsed.version != 4
        or parsed.is_unspecified
        or parsed.is_loopback
        or parsed not in ipaddress.ip_network("100.64.0.0/10")
    ):
        raise ValueError("Build-only mode must bind to a specific Tailscale IPv4 address.")
    return str(parsed)


def is_build_only_request(endpoint: str | None, method: str) -> bool:
    return (endpoint, method.upper()) in BUILD_ONLY_ENDPOINTS


def validate_docker_target(
    docker_host: str | None,
    expected_docker_root: str | None,
    *,
    stat_fn=os.stat,
    exists_fn=os.path.exists,
    realpath_fn=posixpath.realpath,
) -> tuple[bool, str]:
    """Fail closed unless DOCKER_HOST is a dedicated Unix socket and root."""
    if not docker_host or not docker_host.startswith("unix://"):
        return False, "Build-only mode requires DOCKER_HOST to be a Unix socket."

    socket_path = docker_host.removeprefix("unix://")
    if not posixpath.isabs(socket_path) or socket_path in MAIN_DOCKER_SOCKETS:
        return False, "Build-only mode refuses the default Docker socket."
    if realpath_fn(socket_path) != socket_path:
        return False, "Build-only Docker socket must not be a symlink."

    try:
        socket_stat = stat_fn(socket_path)
    except OSError:
        return False, "Configured build Docker socket is unavailable."
    if not stat.S_ISSOCK(socket_stat.st_mode):
        return False, "Configured build Docker endpoint is not a Unix socket."

    for main_socket in MAIN_DOCKER_SOCKETS:
        if not exists_fn(main_socket):
            continue
        try:
            main_stat = stat_fn(main_socket)
        except OSError:
            continue
        if (socket_stat.st_dev, socket_stat.st_ino) == (main_stat.st_dev, main_stat.st_ino):
            return False, "Build-only Docker socket resolves to the host Docker daemon."

    if not expected_docker_root or not posixpath.isabs(expected_docker_root):
        return False, "Build-only mode requires an absolute Docker data-root allowlist."
    return True, realpath_fn(expected_docker_root)


def validate_docker_daemon(
    docker_host: str | None,
    expected_docker_root: str | None,
    *,
    client_factory=None,
    stat_fn=os.stat,
    exists_fn=os.path.exists,
    realpath_fn=posixpath.realpath,
) -> tuple[bool, str]:
    """Verify the daemon identity via Docker API; no fallback is attempted."""
    valid, expected_root = validate_docker_target(
        docker_host,
        expected_docker_root,
        stat_fn=stat_fn,
        exists_fn=exists_fn,
        realpath_fn=realpath_fn,
    )
    if not valid:
        return valid, expected_root

    try:
        if client_factory is None:
            import docker

            client_factory = docker.DockerClient
        client = client_factory(base_url=docker_host, timeout=5)
        info = client.info()
        client.close()
    except Exception:
        return False, "Configured build Docker daemon is unreachable."

    actual_root = realpath_fn(info.get("DockerRootDir", ""))
    if not actual_root or actual_root != expected_root:
        return False, "Docker daemon data-root does not match the configured build-only root."
    return True, actual_root


def require_build_only_docker() -> None:
    """Require an explicitly configured isolated daemon in build-only workers."""
    if os.environ.get("PRESS_AGENT_BUILD_ONLY") != "1":
        return

    if any(os.environ.get(name) for name in ("DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH")):
        raise RuntimeError("Build-only mode refuses Docker context or TLS overrides.")

    valid, message = validate_docker_daemon(
        os.environ.get("DOCKER_HOST"), os.environ.get("BUILD_DOCKER_ROOT")
    )
    if not valid:
        raise RuntimeError(message)
