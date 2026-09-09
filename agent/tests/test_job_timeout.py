from __future__ import annotations

import unittest

from flask import Flask

from agent.job import DEFAULT_TIMEOUT, MAX_TIMEOUT, _requested_job_timeout, resolve_job_timeout


class TestResolveJobTimeout(unittest.TestCase):
    """resolve_job_timeout picks the RQ timeout for an enqueued job.

    Candidates are (request param, decorator default, configured job_timeout);
    press sends the first one as `agent_job_timeout` in the request body.
    """

    def test_falls_back_to_default_without_candidates(self):
        self.assertEqual(resolve_job_timeout(None, None, None), DEFAULT_TIMEOUT)

    def test_first_usable_candidate_wins(self):
        self.assertEqual(resolve_job_timeout(18000, 900, 600), 18000)
        self.assertEqual(resolve_job_timeout(None, 900, 600), 900)
        self.assertEqual(resolve_job_timeout(None, None, 600), 600)

    def test_accepts_numeric_string(self):
        # Request bodies and config.json are untyped; a numeric string is usable.
        self.assertEqual(resolve_job_timeout("18000", None, None), 18000)

    def test_skips_non_numeric_candidate(self):
        self.assertEqual(resolve_job_timeout("as long as it takes", 900, None), 900)
        self.assertEqual(resolve_job_timeout({"hours": 5}, None, None), DEFAULT_TIMEOUT)

    def test_skips_candidate_outside_bounds(self):
        self.assertEqual(resolve_job_timeout(MAX_TIMEOUT + 1, 900, None), 900)
        self.assertEqual(resolve_job_timeout(-1, None, None), DEFAULT_TIMEOUT)

    def test_skips_zero_because_rq_would_kill_the_horse(self):
        self.assertEqual(resolve_job_timeout(0, 900, None), 900)

    def test_accepts_the_upper_bound(self):
        self.assertEqual(resolve_job_timeout(MAX_TIMEOUT, None, None), MAX_TIMEOUT)

    def test_callable_candidate_is_evaluated_only_when_reached(self):
        calls = []

        def provider():
            calls.append(1)
            return 600

        self.assertEqual(resolve_job_timeout(18000, None, provider), 18000)
        self.assertEqual(calls, [], "an earlier candidate settled it; config must not be read")

        self.assertEqual(resolve_job_timeout(None, None, provider), 600)
        self.assertEqual(calls, [1])

    def test_raising_callable_cannot_fail_the_enqueue(self):
        def provider():
            # Base.set_config renames config.json aside while rewriting it.
            raise FileNotFoundError("config.json")

        self.assertEqual(resolve_job_timeout(None, provider, 900), 900)
        self.assertEqual(resolve_job_timeout(provider), DEFAULT_TIMEOUT)


if __name__ == "__main__":
    unittest.main()


class TestRequestedJobTimeout(unittest.TestCase):
    """_requested_job_timeout reads the timeout off the request body, if there is one.

    Every agent job is enqueued through this, including from endpoints whose body
    is not a JSON object - so it has to tolerate any body rather than raise.
    """

    def setUp(self):
        self.app = Flask(__name__)

    def test_reads_the_timeout_from_an_object_body(self):
        with self.app.test_request_context(json={"agent_job_timeout": 18000}):
            self.assertEqual(_requested_job_timeout(), 18000)

    def test_object_body_without_the_key(self):
        with self.app.test_request_context(json={"with_files": True}):
            self.assertIsNone(_requested_job_timeout())

    def test_array_body_does_not_raise(self):
        # /proxy/wildcards POSTs a JSON array. `request.json.get(...)` raised
        # AttributeError here, 500ing the endpoint before "Add Wildcard Hosts to
        # Proxy" could be enqueued, which took wildcard TLS renewals with it.
        with self.app.test_request_context(json=[{"domain": "example.com"}]):
            self.assertIsNone(_requested_job_timeout())

    def test_unparseable_body_does_not_abort(self):
        # `request.json` aborts with 400 on this; get_json(silent=True) must not.
        with self.app.test_request_context(data="{not json", content_type="application/json"):
            self.assertIsNone(_requested_job_timeout())

    def test_empty_and_non_json_bodies(self):
        with self.app.test_request_context():
            self.assertIsNone(_requested_job_timeout())
        with self.app.test_request_context(data="plain", content_type="text/plain"):
            self.assertIsNone(_requested_job_timeout())

    def test_outside_a_request_context(self):
        self.assertIsNone(_requested_job_timeout())
