from __future__ import annotations

import os
import subprocess
import tempfile
import unittest

from agent.utils import get_size


def write_file(path: str, size: int):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(b"\0" * size)


class TestGetSize(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.mkdtemp()
        write_file(os.path.join(self.folder, "backups", "top.sql.gz"), 1000)
        write_file(os.path.join(self.folder, "files", "backups", "nested.pdf"), 300)
        write_file(os.path.join(self.folder, "files", "invoice.pdf"), 50)

    def tearDown(self):
        subprocess.run(["rm", "-rf", self.folder], check=True)

    def test_ignore_dirs_skips_only_the_top_level_folder_not_nested_ones_with_the_same_name(self):
        total = get_size(self.folder)
        without_backups = get_size(self.folder, ignore_dirs=["backups"])
        top_level_backups = get_size(os.path.join(self.folder, "backups"))

        self.assertEqual(total - without_backups, top_level_backups)
        self.assertGreaterEqual(without_backups, 350)
