from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from click.testing import CliRunner

from agent.base import AgentException
from agent.bench import Bench
from agent.cli import cli
from agent.server import Server

BENCH_NAME = "bench-0001-000001-f1"


class TestSingleContainerBenchStart(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.write_bench_files()
        with patch.object(Server, "__init__", new=lambda x: None):
            server = Server()
        server.benches_directory = self.directory
        self.bench = Bench(BENCH_NAME, server)
        self.commands = []

    def tearDown(self):
        shutil.rmtree(self.directory)

    def write_bench_files(self):
        bench_directory = os.path.join(self.directory, BENCH_NAME)
        os.makedirs(os.path.join(bench_directory, "sites"))
        with open(os.path.join(bench_directory, "sites", "common_site_config.json"), "w") as f:
            json.dump({}, f)
        with open(os.path.join(bench_directory, "config.json"), "w") as f:
            json.dump(
                {
                    "single_container": True,
                    "docker_image": "image",
                    "web_port": 18000,
                    "socketio_port": 19000,
                    "codeserver_port": 16000,
                },
                f,
            )

    def execute_with_stop_timeout(self, command, **kwargs):
        self.commands.append(command)
        if command.startswith("docker stop"):
            raise AgentException({"output": "tried to kill container, but did not receive an exit event"})

    def test_container_is_force_removed_before_the_run_when_the_stop_times_out(self):
        with patch.object(Bench, "execute", side_effect=self.execute_with_stop_timeout):
            self.bench.start()

        self.assertEqual(self.commands[1], f"docker rm --force {BENCH_NAME}")
        self.assertTrue(self.commands[2].startswith("docker run"))


class TestBenchStartCommand(unittest.TestCase):
    def run_start(self, benches: dict[str, Mock]):
        # agent/__init__.py re-exports the cli group as agent.cli, which hides the module
        with patch.object(sys.modules["agent.cli"], "Server", return_value=Mock(benches=benches)):
            return CliRunner().invoke(cli, ["bench", "start", *benches])

    def test_a_failing_bench_does_not_stop_the_benches_after_it_from_starting(self):
        failing = Mock(start=Mock(side_effect=Exception("name conflict")))
        healthy = Mock()

        result = self.run_start({"bench-a": failing, "bench-b": healthy})

        healthy.start.assert_called_once()
        self.assertEqual(result.exit_code, 1)
        self.assertIn("Failed to start benches: bench-a", result.output)

    def test_exits_cleanly_when_every_bench_starts(self):
        benches = {"bench-a": Mock(), "bench-b": Mock()}

        result = self.run_start(benches)

        self.assertEqual(result.exit_code, 0)
        for bench in benches.values():
            bench.start.assert_called_once()
