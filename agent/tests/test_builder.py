from __future__ import annotations

import os
import tempfile
import unittest
from types import SimpleNamespace

import agent


class TestCheckVersion(unittest.TestCase):
    def test_check_version(self):
        from agent.builder import ValidationManager

        cases = [
            # python requires-python (SimpleSpec syntax)
            ("3.11.0", ">=3.10", True),
            ("3.9.0", ">=3.10", False),
            ("3.11", ">=3.10,<3.13", True),
            ("3.13", ">=3.10,<3.13", False),
            # node engines (npm syntax)
            ("18.16.0", ">=18", True),
            ("16.20.0", ">=18", False),
            ("18.16.0", "^18.0.0", True),
            ("20.1.0", "^18.0.0", False),
            ("18.16.0", ">=18 <21", True),
            ("22.0.0", ">=18 <21", False),
            ("18.16.0", "18.x", True),
            ("20.0.0", "18.x", False),
            ("20.0.0", "20 || 22", True),
            ("21.0.0", "20 || 22", False),
            ("18.16.0", "*", True),
        ]

        for actual, expected, want in cases:
            with self.subTest(actual=actual, expected=expected):
                self.assertEqual(ValidationManager.check_version(actual, expected), want)


class TestStampStepEnd(unittest.TestCase):
    def test_done_line_gets_the_utc_time_the_step_ended(self):
        from agent.builder import stamp_step_end

        stamped = stamp_step_end("#12 DONE 3.2s\n")
        self.assertRegex(stamped, r"^#12 DONE 3\.2s \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\n$")

    def test_other_build_lines_are_left_unchanged(self):
        from agent.builder import stamp_step_end

        for line in ["#12 CACHED\n", "#12 [stage-1 4/9] RUN bench build\n", "#12 0.512 DONE in 3s\n"]:
            self.assertEqual(stamp_step_end(line), line)


class TestBuildContext(unittest.TestCase):
    def test_build_context_includes_the_asset_store_script(self):
        from agent.builder import ContextManager

        build_config_path = os.path.join(os.path.dirname(agent.__file__), "build_configs")
        with tempfile.TemporaryDirectory() as build_directory:
            context = SimpleNamespace(build_config_path=build_config_path, build_directory=build_directory)
            ContextManager._copy_build_config_files(context)
            self.assertTrue(os.path.isfile(os.path.join(build_directory, "get_cached_app.py")))
