import unittest
from pathlib import Path

from agent.workload import InvalidWorkload, WorkloadConfig


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


if __name__ == "__main__":
    unittest.main()
