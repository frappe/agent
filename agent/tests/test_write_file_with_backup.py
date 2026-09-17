from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from agent.base import Base


class TestWriteFileWithBackup(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "redis-cache.conf")
        with open(self.path, "w") as f:
            f.write("old")

    def tearDown(self):
        shutil.rmtree(self.dir)

    def test_keeps_previous_version_as_bak(self):
        Base().write_file_with_backup(self.path, "new")
        with open(self.path) as f:
            self.assertEqual(f.read(), "new")
        with open(self.path + ".bak") as f:
            self.assertEqual(f.read(), "old")

    def test_leaves_original_untouched_when_write_fails(self):
        with patch("agent.base.os.replace", side_effect=OSError("disk full")), self.assertRaises(OSError):
            Base().write_file_with_backup(self.path, "new")
        with open(self.path) as f:
            self.assertEqual(f.read(), "old")
        self.assertEqual(sorted(os.listdir(self.dir)), ["redis-cache.conf", "redis-cache.conf.bak"])

    def test_keeps_file_mode(self):
        os.chmod(self.path, 0o644)
        Base().write_file_with_backup(self.path, "new")
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o644)
