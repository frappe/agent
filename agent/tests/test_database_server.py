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
from agent.exceptions import AgentException

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
        with patch("agent.database_server.Database") as db, patch.object(DatabaseServer, "db_port", 3306):
            self.queries = db.return_value.execute_query
            self.queries.return_value = (True, [{"output": {"data": [[9]]}}])
            return DatabaseServer.stage_audit_logs.__wrapped__(self.server, "127.0.0.1", "password")

    def executed(self):
        return [call.args[0] for call in self.queries.call_args_list]

    def test_rotations_are_frozen_while_the_files_are_moved_and_restored_after(self):
        # A rotation midway renumbers every file, and left at 0 the log never rotates again
        write(Path(self.directory.name) / "server_audit.log.1", RECORD.format(second="01"))

        self.stage()

        self.assertEqual(
            self.executed()[1:],
            [
                "SET GLOBAL server_audit_file_rotate_now = 1;",
                "SET GLOBAL server_audit_file_rotations = 0;",
                "SET GLOBAL server_audit_file_rotations = 9;",
            ],
        )

    def test_rotations_are_restored_even_when_staging_blows_up(self):
        write(Path(self.directory.name) / "server_audit.log.1", RECORD.format(second="01"))

        with patch.object(DatabaseServer, "move_rotated_audit_logs_to_pending", side_effect=OSError):
            self.assertRaises(OSError, self.stage)

        self.assertEqual(self.executed()[-1], "SET GLOBAL server_audit_file_rotations = 9;")

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


class TestPurgeBinlog(unittest.TestCase):
    NOTE = "Binary log 'mysql-bin.000041' is not purged because it is in use by an active XID transaction"

    def setUp(self):
        self.directory = TemporaryDirectory()
        self.server = DatabaseServer.__new__(DatabaseServer)
        self.server.mariadb_directory = self.directory.name
        for name in ["mysql-bin.000041", "mysql-bin.000042", "mysql-bin.000043", "mysql-bin.index"]:
            write(Path(self.directory.name) / name, "")

    def tearDown(self):
        self.directory.cleanup()

    def execute(self, method, to_binlog="mysql-bin.000043", result=None, deletes=()):
        def execute_query(*args, **kwargs):
            for name in deletes:
                os.remove(Path(self.directory.name) / name)
            return result or (True, [{"output": None}, {"output": [{"Message": self.NOTE}]}])

        with patch("agent.database_server.Database") as db, patch.object(DatabaseServer, "db_port", 3306):
            db.return_value.execute_query.side_effect = execute_query
            self.queries = db.return_value.execute_query
            return method(self.server, "127.0.0.1", "password", to_binlog)

    def purge(self, **kwargs):
        return self.execute(DatabaseServer._purge_binlog, **kwargs)

    def test_succeeds_when_mariadb_deletes_the_older_binlogs(self):
        self.assertTrue(self.purge(deletes=["mysql-bin.000041", "mysql-bin.000042"]))

    def test_fails_when_mariadb_keeps_an_older_binlog_without_an_error(self):
        # MariaDB stops at a binlog that crash recovery or a reader still needs, and only adds a note
        self.assertFalse(self.purge(deletes=["mysql-bin.000042"]))

    def test_fails_when_the_purge_query_fails(self):
        self.assertFalse(self.purge(result=(False, "Target log not found in binlog index")))

    def test_rejects_a_name_that_is_not_a_binlog_without_a_query(self):
        self.assertFalse(self.purge(to_binlog="mysql-bin.000043'; DROP DATABASE mysql; --"))
        self.queries.assert_not_called()

    def test_step_fails_with_the_binlogs_left_on_disk_and_the_note_from_mariadb(self):
        with self.assertRaises(AgentException) as context:
            self.execute(DatabaseServer.purge_binlog_step.__wrapped__)

        output = context.exception.data["output"]
        self.assertIn("2 binlogs before mysql-bin.000043 are still on disk", output)
        self.assertIn("from mysql-bin.000041 to mysql-bin.000042", output)
        self.assertIn(f"MariaDB: {self.NOTE}", output)
