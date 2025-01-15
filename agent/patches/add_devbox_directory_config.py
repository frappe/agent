from __future__ import annotations

import os


def execute():
    """Add devbox config key for old app servers"""
    from agent.server import Server
    server = Server()
    config = server.config
    config["devboxes_directory"] = os.path.join(os.path.dirname(config["benches_directory"]),"devboxes")
    server.setconfig(config)