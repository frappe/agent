from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from agent.database_server import (
    AUDIT_LOG_ROTATED_RE,
    AUDIT_LOG_STAGED_RE,
    DatabaseServer,
    list_audit_logs,
    parse_audit_log_time_range,
)

RECORD = "20250811 12:00:{second},dbserver,root,localhost,42,1337,QUERY,mydb,'select 1',0\n"


def write(path: Path, content: str) -> str:
    path.write_text(content)
    return str(path)


class TestParseAuditLogTimeRange(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.path = Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def test_returns_first_and_last_record_timestamps(self):
        log = write(
            self.path / "audit.log",
            RECORD.format(second="01") + RECORD.format(second="02") + RECORD.format(second="03"),
        )

        self.assertEqual(
            parse_audit_log_time_range(log),
            ("2025-08-11T12:00:01", "2025-08-11T12:00:03"),
        )

    def test_ignores_continuation_lines_of_a_query_containing_newlines(self):
        log = write(
            self.path / "audit.log",
            RECORD.format(second="01")
            + "20250811 12:00:05,dbserver,root,localhost,42,1338,QUERY,mydb,'select\n"
            + "1 from t',0\n",
        )

        # The trailing "1 from t',0" line is part of the previous record, so the end of
        # the range is 12:00:05 and not something parsed out of that fragment.
        self.assertEqual(
            parse_audit_log_time_range(log),
            ("2025-08-11T12:00:01", "2025-08-11T12:00:05"),
        )

    def test_returns_none_for_an_empty_file(self):
        self.assertEqual(parse_audit_log_time_range(write(self.path / "audit.log", "")), (None, None))

    def test_returns_none_when_no_line_looks_like_a_record(self):
        log = write(self.path / "audit.log", "not an audit record\nnor is this\n")

        self.assertEqual(parse_audit_log_time_range(log), (None, None))


class TestListAuditLogs(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.path = Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def test_lists_only_rotated_logs_sorted_by_name(self):
        for name in ["server_audit.log", "server_audit.log.2", "server_audit.log.1", "mysql-error.log"]:
            write(self.path / name, "")

        # The live server_audit.log is excluded: the plugin still holds it open.
        self.assertEqual(
            [log["name"] for log in list_audit_logs(str(self.path), AUDIT_LOG_ROTATED_RE)],
            ["server_audit.log.1", "server_audit.log.2"],
        )

    def test_returns_empty_list_when_directory_is_missing(self):
        self.assertEqual(list_audit_logs(str(self.path / "nope"), AUDIT_LOG_ROTATED_RE), [])


class TestStageAuditLogs(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.server = DatabaseServer.__new__(DatabaseServer)
        self.server.audit_log_directory = self.directory.name
        self.server.audit_log_pending_directory = os.path.join(self.directory.name, "audit_pending")

    def tearDown(self):
        self.directory.cleanup()

    def stage(self):
        # Only the MariaDB connection is mocked; the file moves are real.
        with patch("agent.database_server.Database") as Db, patch.object(DatabaseServer, "db_port", 3306):
            self.queries = Db.return_value.execute_query
            self.queries.return_value = (True, [{"output": {"data": [[1073741824]]}}])
            return DatabaseServer.stage_audit_logs.__wrapped__(self.server, "127.0.0.1", "password")

    def executed(self):
        return [call.args[0] for call in self.queries.call_args_list]

    def test_size_rotation_is_off_only_while_the_files_are_being_moved(self):
        # A rotation midway renumbers every file, so ours must be the only one that can
        # happen while we are moving them. It has to go back on or the log outgrows the cap.
        write(Path(self.directory.name) / "server_audit.log.1", RECORD.format(second="01"))

        self.stage()

        self.assertEqual(
            self.executed()[1:],
            [
                "SET GLOBAL server_audit_file_rotate_size = 0;",
                "SET GLOBAL server_audit_file_rotate_now = 1;",
                "SET GLOBAL server_audit_file_rotate_size = 1073741824;",
            ],
        )

    def test_size_rotation_is_restored_even_when_staging_blows_up(self):
        write(Path(self.directory.name) / "server_audit.log.1", RECORD.format(second="01"))

        with patch.object(DatabaseServer, "move_rotated_audit_logs_to_pending", side_effect=OSError):
            self.assertRaises(OSError, self.stage)

        self.assertEqual(self.executed()[-1], "SET GLOBAL server_audit_file_rotate_size = 1073741824;")

    def test_moves_rotated_logs_out_of_the_plugin_namespace(self):
        for name in ["server_audit.log", "server_audit.log.1", "server_audit.log.2"]:
            write(Path(self.directory.name) / name, RECORD.format(second="01"))

        staged = self.stage()["staged_files"]

        self.assertEqual(len(staged), 2)
        for name in staged:
            self.assertRegex(name, AUDIT_LOG_STAGED_RE)
        # Nothing rotated is left behind for the next rotation to renumber
        self.assertEqual(list_audit_logs(self.directory.name, AUDIT_LOG_ROTATED_RE), [])
        # The live log the plugin is writing to is untouched
        self.assertTrue((Path(self.directory.name) / "server_audit.log").exists())

    def test_refuses_to_overwrite_an_already_staged_log(self):
        source = Path(self.directory.name) / "server_audit.log.1"
        write(source, RECORD.format(second="01"))
        os.makedirs(self.server.audit_log_pending_directory)

        [staged] = self.stage()["staged_files"]
        write(source, RECORD.format(second="02"))
        os.utime(source, (0, os.stat(Path(self.server.audit_log_pending_directory) / staged).st_mtime))

        with self.assertRaises(FileExistsError):
            self.stage()

        # os.rename would have clobbered the earlier log silently; it is still intact
        self.assertTrue(source.exists())
        self.assertEqual(
            (Path(self.server.audit_log_pending_directory) / staged).read_text(),
            RECORD.format(second="01"),
        )


class TestAuditLogLock(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.server = DatabaseServer.__new__(DatabaseServer)
        self.server.audit_log_pending_directory = os.path.join(self.directory.name, "audit_pending")

    def tearDown(self):
        self.directory.cleanup()

    def acquire(self):
        with self.server.audit_log_lock():
            pass

    def test_a_second_run_is_refused_while_the_first_holds_the_lock(self):
        # Overlapping runs rotate and unlink under each other, which loses audit logs
        with self.server.audit_log_lock():
            self.assertRaisesRegex(Exception, "already running", self.acquire)

    def test_the_lock_is_released_when_a_run_fails(self):
        with self.assertRaises(OSError), self.server.audit_log_lock():
            raise OSError

        self.acquire()
