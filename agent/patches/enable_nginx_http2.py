from __future__ import annotations

# Regenerating the nginx configs now adds http2 where nginx is 1.31.9 or later.
from agent.patches.disable_nginx_http2 import execute  # noqa: F401
