from __future__ import annotations

import io
import pickle
import unittest
from unittest.mock import MagicMock, patch

from agent.base import PUBLISH_INTERVAL_SECONDS, Base, Throttle
from agent.exceptions import AgentException


class FinishedProcess:
    def __init__(self, output: bytes):
        self.stdout = io.BytesIO(output)

    def poll(self):
        return 0


@patch("agent.base.time.monotonic", return_value=1000.0)
class TestParseOutput(unittest.TestCase):
    def setUp(self):
        self.base = Base()
        self.base.update_redis = MagicMock()

    def test_lines_within_two_seconds_are_published_once_plus_the_final_output(self, _):
        output = b"".join(f"line {i}\n".encode() for i in range(1000))

        self.base.parse_output(FinishedProcess(output))

        self.assertEqual(self.base.update_redis.call_count, 2)
        self.assertEqual(self.base.data["output"], "\n".join(f"line {i}" for i in range(1000)))

    def test_progress_bar_updates_within_two_seconds_are_published_once_plus_the_final_output(self, _):
        output = b"".join(f"\r{percent}%".encode() for percent in range(101)) + b"\n"

        self.base.parse_output(FinishedProcess(output))

        self.assertEqual(self.base.update_redis.call_count, 2)
        self.assertEqual(self.base.data["output"], "100%")

    def test_output_is_published_again_after_two_seconds(self, monotonic):
        monotonic.side_effect = [1000.0, 1000.0, 1001.0, 1002.0, 1002.0]

        self.base.parse_output(FinishedProcess(b"a\nb\nc\n"))

        # a (first), b skipped at +1s, c at +2s, then the final output
        self.assertEqual(self.base.update_redis.call_count, 3)


class TestExecuteKeepsPasswordsOutOfJobData(unittest.TestCase):
    def setUp(self):
        self.base = Base()
        self.base.update_redis = MagicMock()

    def test_env_reaches_the_subprocess_but_not_the_recorded_command(self):
        command = ["sh", "-c", 'test "$AGENT_TEST_SECRET" = fake-secret && echo ok']

        with patch("builtins.print"):
            data = self.base.execute(command, directory="/tmp", env={"AGENT_TEST_SECRET": "fake-secret"})

        self.assertEqual(data["output"], "ok")
        self.assertEqual(data["command"], command)

    def test_failed_command_records_masked_command_and_traceback(self):
        with patch("builtins.print"), self.assertRaises(AgentException) as raised:
            self.base.execute("false --admin-password fake-admin", directory="/tmp")

        data = raised.exception.data
        self.assertEqual(data["command"], "false --admin-password ********")
        self.assertIn("--admin-password ********", data["traceback"])
        self.assertNotIn("fake-admin", data["traceback"])

    def test_logged_line_has_masked_command(self):
        with patch("builtins.print") as printed:
            self.base.execute("true --admin-password fake-admin", directory="/tmp")

        self.assertNotIn("fake-admin", "".join(str(call) for call in printed.call_args_list))


class TestThrottle(unittest.TestCase):
    def test_object_holding_a_throttle_can_be_pickled_for_rq(self):
        base = Base()
        base.publish_output = Throttle(base.publish_data, PUBLISH_INTERVAL_SECONDS)

        restored = pickle.loads(pickle.dumps(base))

        self.assertEqual(restored.publish_output.seconds, PUBLISH_INTERVAL_SECONDS)
