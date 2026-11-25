from __future__ import annotations

import os
import stat
import unittest

from agent.mariadb_client import password_env


def read_option_file(env) -> str:
    with open(os.path.join(env["HOME"], ".my.cnf")) as option_file:
        return option_file.read()


class TestPasswordEnv(unittest.TestCase):
    def test_option_file_is_private_and_removed_after_use(self):
        with password_env("fake-password") as env:
            home = env["HOME"]
            self.assertEqual(stat.S_IMODE(os.stat(home).st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(os.stat(os.path.join(home, ".my.cnf")).st_mode), 0o600)

        self.assertFalse(os.path.exists(home))

    def test_quotes_and_backslashes_in_password_are_escaped(self):
        with password_env('a"b\\c#d') as env:
            self.assertEqual(read_option_file(env), '[client]\npassword="a\\"b\\\\c#d"\n')

    def test_missing_password_writes_an_empty_one(self):
        with password_env(None) as env:
            self.assertEqual(read_option_file(env), '[client]\npassword=""\n')
