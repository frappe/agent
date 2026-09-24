import json
import os
import tempfile
import unittest
from pathlib import Path

from agent.workload import ENVIRONMENT_FILE_MAX_AGE, InvalidWorkload, Workload, WorkloadConfig


class TestWorkloadConfig(unittest.TestCase):
    def valid_config(self):
        return {
            "name": "xassida-search",
            "image": "ghcr.io/jvrlc/xassida-search:abc123",
            "container_port": 3000,
            "host_port": 13000,
            "health_path": "/api/health",
        }

    def test_valid_config_builds_loopback_only_docker_command(self):
        config = WorkloadConfig(self.valid_config(), {"SUPABASE_URL": "safe"})
        config.validate()

        command = config.run_command("xassida-search-candidate", Path("/secure/environment"))

        self.assertIn("127.0.0.1:13000:3000", command)
        self.assertNotIn("safe", command)

    def test_public_config_contains_secret_names_but_not_values(self):
        config = WorkloadConfig(self.valid_config(), {"SUPABASE_SERVICE_ROLE_KEY": "secret"})
        config.validate()

        public = config.public_config()

        self.assertEqual(public["environment_keys"], ["SUPABASE_SERVICE_ROLE_KEY"])
        self.assertNotIn("secret", str(public))

    def test_queued_config_does_not_contain_environment_values(self):
        config = WorkloadConfig(self.valid_config(), {"SUPABASE_SERVICE_ROLE_KEY": "secret"})
        config.validate()

        queued = config.public_config()

        self.assertNotIn("secret", str(queued))

    def test_rejects_shell_metacharacters_in_workload_name(self):
        config = self.valid_config()
        config["name"] = "xassida;rm"

        with self.assertRaisesRegex(InvalidWorkload, "Workload name"):
            WorkloadConfig(config, {}).validate()

    def test_rejects_environment_values_with_newlines(self):
        with self.assertRaisesRegex(InvalidWorkload, "SUPABASE_URL"):
            WorkloadConfig(self.valid_config(), {"SUPABASE_URL": "safe\nINJECTED=yes"}).validate()

    def test_rejects_public_privileged_port(self):
        config = self.valid_config()
        config["host_port"] = 443

        with self.assertRaisesRegex(InvalidWorkload, "host_port"):
            WorkloadConfig(config, {}).validate()


class TestWorkloadEnvironmentFiles(unittest.TestCase):
    def test_queued_environment_files_are_distinct_and_encrypted(self):
        workload = object.__new__(Workload)
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            workload.directory = temporary_directory

            first = workload._write_environment(directory, {"TOKEN": "first"})
            second = workload._write_environment(directory, {"TOKEN": "second"})

            self.assertNotEqual(first, second)
            self.assertNotIn(b"first", first.read_bytes())
            self.assertNotIn(b"second", second.read_bytes())
            self.assertEqual(first.stat().st_mode & 0o777, 0o600)
            self.assertEqual(second.stat().st_mode & 0o777, 0o600)

    def test_plaintext_exists_only_while_worker_uses_it(self):
        workload = object.__new__(Workload)
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            workload.directory = temporary_directory
            queued = workload._write_environment(directory, {"TOKEN": "secret"})

            with workload._decrypted_environment(queued) as runtime:
                self.assertEqual(runtime.read_text(), "TOKEN=secret\n")
                self.assertEqual(runtime.stat().st_mode & 0o777, 0o600)

            self.assertFalse(runtime.exists())

    def test_expired_encrypted_file_from_cancelled_job_is_removed(self):
        workload = object.__new__(Workload)
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            workload.directory = temporary_directory
            expired = workload._write_environment(directory, {"TOKEN": "expired"})
            fresh = workload._write_environment(directory, {"TOKEN": "fresh"})
            expired_time = expired.stat().st_mtime - ENVIRONMENT_FILE_MAX_AGE - 1
            os.utime(expired, (expired_time, expired_time))

            workload._remove_expired_environments(directory)

            self.assertFalse(expired.exists())
            self.assertTrue(fresh.exists())

    def test_cancelled_job_removes_its_encrypted_environment(self):
        workload = object.__new__(Workload)
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            workload.directory = temporary_directory
            queued = workload._write_environment(directory, {"TOKEN": "secret"})
            job_data = json.dumps({"function": "deploy", "args": [{}, str(queued)], "kwargs": {}})

            workload.cleanup_cancelled_environment(job_data)

            self.assertFalse(queued.exists())


if __name__ == "__main__":
    unittest.main()
