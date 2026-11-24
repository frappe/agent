from __future__ import annotations

import unittest

from agent.redact import redact_call_arguments, redact_command, redact_text


class TestRedactText(unittest.TestCase):
    def test_masks_value_after_password_flags(self):
        self.assertEqual(
            redact_text("bench restore --mariadb-root-password fake1 --admin-password 'fake 2' x.sql"),
            "bench restore --mariadb-root-password ******** --admin-password ******** x.sql",
        )
        self.assertEqual(redact_text("tool --password=fake"), "tool --password=********")

    def test_masks_set_admin_password_value(self):
        self.assertEqual(
            redact_text("bench --site a.test set-admin-password fake"),
            "bench --site a.test set-admin-password ********",
        )

    def test_masks_attached_mariadb_password(self):
        self.assertEqual(
            redact_text("mariadb-dump -h db -u _a1 -pfake _a1 | gzip > a.gz"),
            "mariadb-dump -h db -u _a1 -p******** _a1 | gzip > a.gz",
        )

    def test_masks_password_in_list_command_shown_in_traceback(self):
        self.assertEqual(
            redact_text("Command '['bench', '--admin-password', 'fake']' returned non-zero exit status 1."),
            "Command '['bench', '--admin-password', ********]' returned non-zero exit status 1.",
        )

    def test_keeps_text_without_password_values(self):
        for text in (
            "docker login --password-stdin registry.test",
            "mariadb -h db -P 3306 -u _a1 _a1 -e 'SHOW TABLES'",
            "ssh -p 22 host",
            "Access denied for user '_a1'@'10.0.0.1' (using password: YES)",
        ):
            self.assertEqual(redact_text(text), text)


class TestRedactCommand(unittest.TestCase):
    def test_masks_list_item_after_password_flag_and_keeps_other_items(self):
        self.assertEqual(
            redact_command(["bench", "new-site", "--admin-password", "fake", "-P", 3306]),
            ["bench", "new-site", "--admin-password", "********", "-P", 3306],
        )

    def test_returns_other_values_unchanged(self):
        self.assertIsNone(redact_command(None))


class TestRedactCallArguments(unittest.TestCase):
    def test_masks_positional_and_keyword_arguments_named_like_secrets(self):
        def reinstall(site, mariadb_root_password, admin_password=None, aws_secret=None):
            pass

        args, kwargs = redact_call_arguments(reinstall, ("a.test", "fake-root"), {"aws_secret": "fake-key"})

        self.assertEqual(args, ["a.test", "********"])
        self.assertEqual(kwargs, {"aws_secret": "********"})
