from __future__ import annotations

import sys

from agent.job import connection as redis_connection

if __name__ == "__main__":
    try:
        conn = redis_connection()
        if not conn.ping():
            raise Exception("Redis is not running")

        conn.bgrewriteaof()
    except Exception as e:
        print(f"Failed to rewrite redis AOF : {e!s}", file=sys.stderr)
