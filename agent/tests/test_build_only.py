from __future__ import annotations

import stat
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from agent.build_only import (
	is_build_only_request,
	require_build_only_docker,
	validate_docker_daemon,
	validate_docker_target,
)


class TestBuildOnlyAccess(unittest.TestCase):
	def test_only_read_and_build_routes_are_allowed(self):
		self.assertTrue(is_build_only_request("ping", "GET"))
		self.assertTrue(is_build_only_request("get_version", "GET"))
		self.assertTrue(is_build_only_request("build_image", "POST"))
		self.assertTrue(is_build_only_request("jobs", "GET"))
		self.assertTrue(is_build_only_request("cancel_job", "POST"))
		self.assertFalse(is_build_only_request("restart_nginx", "POST"))
		self.assertFalse(is_build_only_request("get_server", "GET"))
		self.assertFalse(is_build_only_request("get_metrics", "GET"))
		self.assertFalse(is_build_only_request("build_image", "GET"))

	def test_missing_or_non_unix_endpoint_fails_closed(self):
		for docker_host in (None, "", "tcp://127.0.0.1:2375", "unix:///var/run/docker.sock"):
			with self.subTest(docker_host=docker_host):
				valid, _ = validate_docker_target(docker_host, "/srv/build/docker")
				self.assertFalse(valid)

	def test_dedicated_socket_and_root_are_accepted(self):
		dedicated = "/run/albasar-build/docker.sock"
		root = "/srv/albasar-build/mnt/docker"
		stat_fn = Mock(return_value=SimpleNamespace(st_mode=stat.S_IFSOCK, st_dev=1, st_ino=10))
		exists_fn = Mock(return_value=False)
		realpath_fn = lambda path: path

		valid, actual_root = validate_docker_target(
			f"unix://{dedicated}",
			root,
			stat_fn=stat_fn,
			exists_fn=exists_fn,
			realpath_fn=realpath_fn,
		)

		self.assertTrue(valid)
		self.assertEqual(actual_root, root)

	def test_socket_aliasing_main_docker_fails_closed(self):
		dedicated = "/run/albasar-build/docker.sock"
		stat_fn = Mock(return_value=SimpleNamespace(st_mode=stat.S_IFSOCK, st_dev=1, st_ino=10))
		exists_fn = Mock(return_value=True)
		realpath_fn = lambda path: path

		valid, _ = validate_docker_target(
			f"unix://{dedicated}",
			"/srv/albasar-build/mnt/docker",
			stat_fn=stat_fn,
			exists_fn=exists_fn,
			realpath_fn=realpath_fn,
		)

		self.assertFalse(valid)

	def test_symlink_socket_fails_closed(self):
		valid, _ = validate_docker_target(
			"unix:///run/albasar-build/docker.sock",
			"/srv/albasar-build/mnt/docker",
			stat_fn=Mock(return_value=SimpleNamespace(st_mode=stat.S_IFSOCK, st_dev=1, st_ino=10)),
			exists_fn=Mock(return_value=False),
			realpath_fn=lambda path: "/var/run/docker.sock" if path.endswith("docker.sock") else path,
		)
		self.assertFalse(valid)

	def test_daemon_api_is_bound_to_the_exact_configured_socket(self):
		dedicated = "unix:///run/albasar-build/docker.sock"
		factory = Mock(return_value=SimpleNamespace(info=Mock(return_value={"DockerRootDir": "/srv/build/docker"}), close=Mock()))
		stat_fn = Mock(return_value=SimpleNamespace(st_mode=stat.S_IFSOCK, st_dev=1, st_ino=10))

		valid, root = validate_docker_daemon(
			dedicated,
			"/srv/build/docker",
			client_factory=factory,
			stat_fn=stat_fn,
			exists_fn=Mock(return_value=False),
			realpath_fn=lambda path: path,
		)

		self.assertTrue(valid)
		self.assertEqual(root, "/srv/build/docker")
		factory.assert_called_once_with(base_url=dedicated, timeout=5)

	def test_failed_daemon_never_retries_with_the_default_socket(self):
		factory = Mock(side_effect=OSError("socket unavailable"))
		stat_fn = Mock(return_value=SimpleNamespace(st_mode=stat.S_IFSOCK, st_dev=1, st_ino=10))
		exists_fn = Mock(return_value=False)

		valid, _ = validate_docker_daemon(
			"unix:///run/albasar-build/docker.sock",
			"/srv/build/docker",
			client_factory=factory,
			stat_fn=stat_fn,
			exists_fn=Mock(return_value=False),
			realpath_fn=lambda path: path,
		)

		self.assertFalse(valid)
		factory.assert_called_once_with(
			base_url="unix:///run/albasar-build/docker.sock", timeout=5
		)

	@patch.dict(
		"os.environ",
		{
			"PRESS_AGENT_BUILD_ONLY": "1",
			"DOCKER_HOST": "unix:///run/albasar-build/docker.sock",
			"BUILD_DOCKER_ROOT": "/srv/albasar-build/mnt/docker",
		},
	)
	@patch("agent.build_only.validate_docker_daemon", return_value=(False, "daemon unavailable"))
	def test_build_worker_fails_closed_when_its_socket_is_unavailable(self, validate_daemon):
		with self.assertRaisesRegex(RuntimeError, "daemon unavailable"):
			require_build_only_docker()
		validate_daemon.assert_called_once_with(
			"unix:///run/albasar-build/docker.sock", "/srv/albasar-build/mnt/docker"
		)

	@patch.dict(
		"os.environ",
		{
			"PRESS_AGENT_BUILD_ONLY": "1",
			"DOCKER_HOST": "unix:///run/albasar-build/docker.sock",
			"BUILD_DOCKER_ROOT": "/srv/albasar-build/mnt/docker",
			"DOCKER_CONTEXT": "default",
		},
	)
	@patch("agent.build_only.validate_docker_daemon")
	def test_build_worker_rejects_docker_context_override(self, validate_daemon):
		with self.assertRaisesRegex(RuntimeError, "context or TLS overrides"):
			require_build_only_docker()
		validate_daemon.assert_not_called()


if __name__ == "__main__":
	unittest.main()
