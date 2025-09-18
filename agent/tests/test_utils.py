from __future__ import annotations

import unittest

from agent.utils import escape_bash_string


class TestUtils(unittest.TestCase):
    def test_escape_bash_string(self):
        test_cases = {
            "simple": "simple",
            "with space": "with\\ space",
            "special!chars$&*()": "special\\!chars\\$\\&\\*\\(\\)",
            "quotes\"'": "quotes\\\"\\'",
            "": "",
            "   ": "\\ \\ \\ ",
        }

        for input_str, expected in test_cases.items():
            with self.subTest(input_str=input_str):
                self.assertEqual(escape_bash_string(input_str), expected)
