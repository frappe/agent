from __future__ import annotations

import os
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

from agent.database_physical_backup import (
    DatabasePhysicalBackup,
    get_path_of_physical_backup_metadata,
)

STEPS = [
    "fetch_table_info",
    "flush_tables",
    "flush_changes_to_disk",
    "validate_exportable_files",
    "export_table_schemas",
    "collect_files_metadata",
    "store_backup_metadata",
    "create_snapshot",
    "unlock_all_tables",
]


class TestPhysicalBackupMetadataCleanup(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.backup = DatabasePhysicalBackup.__new__(DatabasePhysicalBackup)
        self.backup.databases = ["_first", "_second"]
        self.backup._db_base_path = self.directory.name
        for db_name in self.backup.databases:
            os.makedirs(os.path.join(self.directory.name, db_name))

    def tearDown(self):
        self.directory.cleanup()

    def metadata_path(self, db_name: str) -> str:
        return get_path_of_physical_backup_metadata(self.directory.name, db_name)

    def write_metadata(self):
        for db_name in self.backup.databases:
            with open(self.metadata_path(db_name), "w") as f:
                f.write("{}")

    def run_job(self, failing_step: str | None = None):
        patches = []
        for name in STEPS:
            side_effect = self.write_metadata if name == "store_backup_metadata" else None
            if name == failing_step:
                side_effect = RuntimeError(name)
            patches.append(patch.object(DatabasePhysicalBackup, name, side_effect=side_effect))

        for p in patches:
            p.start()
        try:
            DatabasePhysicalBackup.create_backup_job.__wrapped__(self.backup)
        finally:
            for p in patches:
                p.stop()

    def assert_no_metadata_left(self):
        for db_name in self.backup.databases:
            self.assertFalse(os.path.exists(self.metadata_path(db_name)))

    def test_removes_metadata_after_a_successful_backup(self):
        self.run_job()
        self.assert_no_metadata_left()

    def test_removes_metadata_when_the_snapshot_fails(self):
        with self.assertRaises(RuntimeError):
            self.run_job(failing_step="create_snapshot")
        self.assert_no_metadata_left()

    def test_removes_metadata_of_every_database_when_one_file_is_missing(self):
        self.write_metadata()
        os.remove(self.metadata_path("_first"))

        self.backup.remove_backups_metadata()

        self.assert_no_metadata_left()
