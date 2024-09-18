from __future__ import annotations

import sys

from agent.cli import cli

if __name__ == "__main__":
    if getattr(sys, "frozen", False):
        cli(sys.argv[1:])
