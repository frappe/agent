from __future__ import annotations

import json
import unittest
from datetime import datetime
from unittest.mock import patch

from peewee import SqliteDatabase

from agent.backup_log import InvalidRange, get_backup_jobs
from agent.job import JobModel, StepModel

SITE = "audited.frappe.cloud"
OTHER_SITE = "other.frappe.cloud"

BACKUP_DIRECTORY = "/home/frappe/frappe-bench/sites/{site}/private/backups"


def backup_data(site: str, offsite: bool = False, sizes: dict | None = None) -> str:
    """A Backup Site job's data, shaped the way fetch_latest_backup returns it."""
    sizes = sizes or {"database": 2048, "public": 512, "private": 128, "site_config": 16}
    backups = {
        artifact: {
            "path": f"{BACKUP_DIRECTORY.format(site=site)}/20231002_000502-{artifact}",
            "file": f"20231002_000502-{artifact}",
            "size": size,
            "url": f"https://{site}/backups/20231002_000502-{artifact}",
        }
        for artifact, size in sizes.items()
    }
    return json.dumps({"backups": backups, "offsite": {"a-file": "path/to/a-file"} if offsite else {}})


class TestBackupLog(unittest.TestCase):
    def setUp(self):
        self.database = SqliteDatabase(":memory:")
        self.models = [JobModel, StepModel]
        self.database.bind(self.models)
        self.database.connect()
        self.database.create_tables(self.models)

    def tearDown(self):
        self.database.drop_tables(self.models)
        self.database.close()

    def add_job(
        self,
        site: str = SITE,
        name: str = "Backup Site",
        status: str = "Success",
        started_on: str = "2023-10-02 04:00:00",
        offsite: bool = False,
        sizes: dict | None = None,
        data: str | None = None,
    ) -> JobModel:
        return JobModel.create(
            name=name,
            status=status,
            agent_job_id="1",
            data=data if data is not None else backup_data(site, offsite, sizes),
            start=datetime.strptime(started_on, "%Y-%m-%d %H:%M:%S"),
        )

    def test_a_backup_job_is_reported_with_its_artifact_sizes(self):
        self.add_job(sizes={"database": 4096, "public": 8, "private": 16, "site_config": 32})

        jobs = get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"]

        self.assertEqual(len(jobs), 1)
        self.assertEqual(
            jobs[0]["sizes"],
            {"database": 4096, "public": 8, "private": 16, "site_config": 32},
        )

    def test_another_sites_backup_is_not_reported(self):
        self.add_job(site=OTHER_SITE)

        self.assertEqual(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"], [])

    def test_a_site_whose_name_contains_the_asked_for_one_is_not_reported(self):
        self.add_job(site=f"not-{SITE}")

        self.assertEqual(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"], [])

    def test_jobs_outside_the_range_are_not_reported(self):
        self.add_job(started_on="2023-09-30 04:00:00")
        self.add_job(started_on="2023-10-04 04:00:00")

        self.assertEqual(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"], [])

    def test_the_end_date_is_included_whatever_time_the_job_ran(self):
        self.add_job(started_on="2023-10-03 23:59:00")

        self.assertEqual(len(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"]), 1)

    def test_jobs_that_are_not_backups_are_not_reported(self):
        self.add_job(name="Migrate Site")

        self.assertEqual(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"], [])

    def test_an_offsite_backup_is_labelled_offsite(self):
        self.add_job(offsite=True)

        self.assertEqual(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"][0]["type"], "offsite")

    def test_a_local_backup_is_labelled_onsite(self):
        self.add_job(offsite=False)

        self.assertEqual(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"][0]["type"], "onsite")

    def test_a_backup_uploaded_by_its_step_is_labelled_offsite(self):
        job = self.add_job(offsite=False)
        StepModel.create(
            name="Upload Site Backup to S3",
            job=job,
            status="Success",
            data="{}",
            start=datetime.now(),
        )

        self.assertEqual(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"][0]["type"], "offsite")

    def test_a_failure_reports_the_innermost_exception_and_no_traceback(self):
        job = self.add_job(status="Failure")
        StepModel.create(
            name="Backup Site",
            job=job,
            status="Failure",
            data=json.dumps(
                {
                    "output": (
                        "Traceback (most recent call last):\n"
                        '  File "x.py", line 1\n'
                        "MySQLdb.OperationalError: gone away"
                    ),
                    "traceback": "subprocess.CalledProcessError: command failed",
                }
            ),
            start=datetime.now(),
        )

        reported = get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"][0]

        self.assertEqual(reported["failure_reason"], "MySQLdb.OperationalError: gone away")
        self.assertNotIn("traceback", reported)

    def test_a_successful_job_reports_no_failure_reason(self):
        self.add_job()

        self.assertEqual(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"][0]["failure_reason"], "")

    def test_a_job_with_unreadable_data_is_skipped_rather_than_raising(self):
        self.add_job(data="not json")

        self.assertEqual(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"], [])

    def test_newest_job_is_reported_first(self):
        self.add_job(started_on="2023-10-01 04:00:00")
        self.add_job(started_on="2023-10-03 04:00:00")

        jobs = get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["jobs"]

        self.assertEqual([job["start"] for job in jobs], ["2023-10-03 04:00:00", "2023-10-01 04:00:00"])

    def test_a_similarly_named_site_does_not_consume_the_row_limit(self):
        # Newer decoys would fill a limit applied before the exact site filter, and the
        # older real job is the one an audit of a past date is asking about
        self.add_job(site=SITE, started_on="2023-10-01 04:00:00")
        for hour in range(2, 5):
            self.add_job(site=f"not-{SITE}", started_on=f"2023-10-02 0{hour}:00:00")

        with patch("agent.backup_log.MAX_JOBS", 2):
            jobs = get_backup_jobs(SITE, "2023-10-01", "2023-10-03")

        self.assertEqual(len(jobs["jobs"]), 1)
        self.assertEqual(jobs["jobs"][0]["start"], "2023-10-01 04:00:00")

    def test_hitting_the_row_limit_is_reported_rather_than_passed_off_as_complete(self):
        self.add_job(started_on="2023-10-01 04:00:00")
        self.add_job(started_on="2023-10-02 04:00:00")

        with patch("agent.backup_log.MAX_JOBS", 1):
            result = get_backup_jobs(SITE, "2023-10-01", "2023-10-03")

        self.assertTrue(result["truncated"])
        self.assertEqual(len(result["jobs"]), 1)

    def test_a_result_that_fits_is_not_reported_as_truncated(self):
        self.add_job()

        self.assertFalse(get_backup_jobs(SITE, "2023-10-01", "2023-10-03")["truncated"])

    def test_a_malformed_date_is_rejected(self):
        self.assertRaisesRegex(InvalidRange, "YYYY-MM-DD", get_backup_jobs, SITE, "02-10-2023", "2023-10-03")

    def test_a_missing_date_is_rejected(self):
        self.assertRaisesRegex(InvalidRange, "YYYY-MM-DD", get_backup_jobs, SITE, None, None)

    def test_a_reversed_range_is_rejected(self):
        self.assertRaisesRegex(
            InvalidRange, "on or before", get_backup_jobs, SITE, "2023-10-03", "2023-10-01"
        )

    def test_a_range_wider_than_a_year_is_rejected(self):
        self.assertRaisesRegex(
            InvalidRange, "366 days or less", get_backup_jobs, SITE, "2023-01-01", "2024-12-31"
        )


if __name__ == "__main__":
    unittest.main()
