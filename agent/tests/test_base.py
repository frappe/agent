from __future__ import annotations

import io
import unittest
from unittest.mock import MagicMock, patch

from agent.base import Base


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
