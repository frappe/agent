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

    def test_restores_original_when_copy_fails(self):
        with patch("agent.base.shutil.copy2", side_effect=OSError("disk full")), self.assertRaises(OSError):
            Base().write_file_with_backup(self.path, "new")
        with open(self.path) as f:
            self.assertEqual(f.read(), "old")
        self.assertFalse(os.path.exists(self.path + ".bak"))
