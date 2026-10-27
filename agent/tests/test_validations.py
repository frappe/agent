from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

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

    @patch.dict(os.environ, {"PRESS_AGENT_BUILD_ONLY": "1"})
    @patch("agent.build_only.require_build_only_docker")
    @patch("docker.from_env")
    @patch("agent.build_utils.validations.subprocess.run", side_effect=FileNotFoundError)
    def test_missing_python314_uses_only_isolated_build_docker(
        self, _run, from_env, require_build_only_docker
    ):
        self.write("hooks.py", "app_name = 'builder'\n")
        self.write("pyproject.toml", '[project]\nrequires-python = ">=3.14"\n')
        client = MagicMock()
        from_env.return_value = client

        result = check_python_syntax(self.app, "frappe/bench:v5.31.0")

        self.assertEqual(result, "")
        require_build_only_docker.assert_called_once_with()
        from_env.assert_called_once_with(timeout=5 * 60)
        client.containers.run.assert_called_once()
        args, kwargs = client.containers.run.call_args
        self.assertEqual(args[0], "frappe/bench:v5.31.0")
        self.assertEqual(args[1][:3], ["python3.14", "-m", "compileall"])
        self.assertTrue(kwargs["network_disabled"])
        self.assertTrue(kwargs["read_only"])
        self.assertEqual(kwargs["volumes"][os.path.abspath(self.app)]["mode"], "ro")
        client.close.assert_called_once_with()

    @patch.dict(os.environ, {}, clear=True)
    @patch("agent.build_utils.validations.subprocess.run", side_effect=FileNotFoundError)
    def test_missing_python314_fails_closed_outside_build_only(self, _run):
        self.write("hooks.py", "app_name = 'builder'\n")

        with self.assertRaises(FileNotFoundError):
            check_python_syntax(self.app, "frappe/bench:v5.31.0")
