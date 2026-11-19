from __future__ import annotations

import os

from filelock import FileLock


def execute():
    """Regenerate the nginx configs from the templates without http2, then reload nginx.

    An agent update ships the new templates, but nothing regenerates the agent
    vhost by itself, and it listens on 443 on every server. A reload is enough:
    worker_shutdown_timeout closes the old HTTP/2 connections within 30s.
    """
    from agent.proxy import Proxy
    from agent.server import Server

    server = Server()
    server._generate_agent_nginx_config()

    if os.path.exists(os.path.join(server.nginx_directory, "proxy.conf")):
        Proxy()._generate_proxy_config()

    if server.config.get("standalone"):
        for bench in server.benches.values():
            with FileLock(os.path.join(bench.directory, "nginx.config.lock")):
                bench.generate_nginx_config(ignore_missing_site_configs=True)

    server._reload_nginx()
