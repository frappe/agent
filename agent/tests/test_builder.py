from __future__ import annotations

import importlib
import os
import sys
import tarfile
import tempfile
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

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


def import_get_cached_app():
    """Import the build script without bench installed or build arguments."""
    bench_cli = ModuleType("bench.cli")
    bench_cli.change_working_directory = lambda: None
    stubs = {"bench": ModuleType("bench"), "bench.cli": bench_cli}
    argv = ["get_cached_app.py", "app", "hash", "token", "url"]
    with patch.dict(sys.modules, stubs), patch.object(sys, "argv", argv):
        return importlib.import_module("agent.build_configs.get_cached_app")


class TestGetCachedAppNodeModules(unittest.TestCase):
    def setUp(self):
        self.script = import_get_cached_app()
        self.bench = tempfile.mkdtemp()
        self.app_node_modules = os.path.join(self.bench, "apps", "builder", "node_modules")
        os.makedirs(os.path.join(self.app_node_modules, "vue"))
        self.public = os.path.join(self.bench, "apps", "builder", "builder", "public")
        os.makedirs(os.path.join(self.public, "frontend"))
        with open(os.path.join(self.public, "frontend", "index.html"), "w") as f:
            f.write("<html></html>")

    def test_stored_assets_leave_out_the_node_modules_bench_links_into_them(self):
        os.symlink(self.app_node_modules, os.path.join(self.public, "node_modules"))
        assets = os.path.join(self.bench, "builder")
        os.symlink(self.public, assets)

        tar_file = self.script.tar_and_compress_folder(assets, os.path.join(self.bench, "builder.tar.gz"))
        with tarfile.open(tar_file) as tar:
            names = tar.getnames()

        self.assertIn("builder/frontend/index.html", names)
        self.assertFalse([name for name in names if name.startswith("builder/node_modules")])

    def test_extracted_assets_get_the_node_modules_link_back(self):
        self.script.link_node_modules(self.bench, "builder", self.public)

        link = os.path.join(self.public, "node_modules")
        self.assertEqual(os.readlink(link), self.app_node_modules)
