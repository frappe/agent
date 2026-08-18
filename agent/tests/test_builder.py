from __future__ import annotations

import unittest


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
