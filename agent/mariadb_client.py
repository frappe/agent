from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager


@contextmanager
def password_env(password: str | None):
    """Env that gives the MariaDB clients the password through an option file, not argv"""
    with tempfile.TemporaryDirectory(prefix="agent-mariadb-") as home:
        # Clients read ~/.my.cnf last, so this file wins over the other option files, like -p did
        _write_option_file(os.path.join(home, ".my.cnf"), password or "")
        # MYSQL_PWD is not used: clients from 11.4 then skip server certificate checks and warn
        yield {"HOME": home}


def _write_option_file(path: str, password: str):
    escaped = password.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    with open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as option_file:
        option_file.write(f'[client]\npassword="{escaped}"\n')
