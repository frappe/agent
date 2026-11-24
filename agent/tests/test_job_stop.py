from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

from agent import job
from agent.bench import Bench
from agent.server import Server

BENCH_NAME = "bench-0001-000001-f1"


class TestStoppedJobKillsContainerProcesses(unittest.TestCase):
    """Docker does not kill an exec'd process when its client dies, so stop() must."""

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.directory, BENCH_NAME, "sites"))
        with open(os.path.join(self.directory, BENCH_NAME, "config.json"), "w") as f:
            json.dump({"single_container": True}, f)
        with open(os.path.join(self.directory, BENCH_NAME, "sites", "common_site_config.json"), "w") as f:
            json.dump({}, f)
        with patch.object(Server, "__init__", new=lambda x: None):
            server = Server()
        server.benches_directory = self.directory
        server.job = Mock(model=Mock(id=12))
        self.bench = Bench(BENCH_NAME, server)

        self.redis = Mock()
        patcher = patch.object(job, "connection", return_value=self.redis)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        shutil.rmtree(self.directory)

    def test_docker_execute_tags_the_command_with_the_job_and_records_its_container(self):
        with patch.object(Bench, "execute") as execute:
            self.bench.docker_execute("bench --site a.com migrate")

        self.assertIn("-e AGENT_JOB_ID=12 ", execute.call_args.args[0])
        self.redis.sadd.assert_called_once_with("agent:job:12:containers", BENCH_NAME)

    def test_kills_processes_tagged_with_the_job_in_its_containers(self):
        self.redis.smembers.return_value = {BENCH_NAME.encode()}
        with patch.object(job.subprocess, "run") as run:
            job.kill_container_processes(12)

        command = run.call_args.args[0]
        self.assertEqual(command[:6], ["docker", "exec", "--privileged", "-u", "root", BENCH_NAME])
        self.assertIn("grep -lsxz AGENT_JOB_ID=12 ", command[-1])
        self.assertIn("kill -9", command[-1])


if __name__ == "__main__":
    unittest.main()
