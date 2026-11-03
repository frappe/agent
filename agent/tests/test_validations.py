from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

from agent.build_utils.validations import check_python_syntax


def find_python_with_optimize_flag() -> str | None:
    """compileall's -o flag needs Python 3.9 or later, newer than the tests may run on."""
    if sys.version_info >= (3, 9):
        return sys.executable
    for name in ("python3.14", "python3.13", "python3.12", "python3.11", "python3.10", "python3.9"):
        path = shutil.which(name)
        if path:
            return path
    return None


class TestCheckPythonSyntax(unittest.TestCase):
    def setUp(self):
        python = find_python_with_optimize_flag()
        if not python:
            self.skipTest("needs Python 3.9 or later")
        self.app = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.app)
        patcher = patch("agent.build_utils.validations._get_server_python_path", return_value=python)
        patcher.start()
        self.addCleanup(patcher.stop)

    def write(self, name: str, content: str):
        with open(os.path.join(self.app, name), "w") as f:
            f.write(content)

    def test_check_leaves_no_bytecode_in_the_app(self):
        self.write("hooks.py", "app_name = 'builder'\n")

        self.assertEqual(check_python_syntax(self.app), "")
        for _, dirs, files in os.walk(self.app):
            self.assertNotIn("__pycache__", dirs)
            self.assertFalse([name for name in files if name.endswith(".pyc")])

    def test_syntax_error_is_reported(self):
        self.write("broken.py", "def broken(:\n")

        self.assertIn("broken.py", check_python_syntax(self.app))
