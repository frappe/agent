from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from agent.ai_control.protocols import ProtocolTestError, _headers


class AIProtocolAuthenticationTests(unittest.TestCase):
    def integration(self, **overrides):
        values = {
            "auth_type": "none",
            "secret_ref": None,
            "config": "{}",
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_bearer_secret_is_resolved_from_environment(self):
        integration = self.integration(auth_type="bearer", secret_ref="AI_TEST_BEARER")
        with patch.dict(os.environ, {"AI_TEST_BEARER": "token-value"}, clear=False):
            headers = _headers(integration)
        self.assertEqual(headers["Authorization"], "Bearer token-value")

    def test_api_key_uses_configured_header_name(self):
        integration = self.integration(
            auth_type="api_key",
            secret_ref="AI_TEST_KEY",
            config='{"auth_header": "X-Test-Key"}',
        )
        with patch.dict(os.environ, {"AI_TEST_KEY": "key-value"}, clear=False):
            headers = _headers(integration)
        self.assertEqual(headers["X-Test-Key"], "key-value")

    def test_missing_secret_reference_fails_closed(self):
        integration = self.integration(auth_type="bearer", secret_ref="AI_TEST_MISSING")
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AI_TEST_MISSING", None)
            with self.assertRaises(ProtocolTestError):
                _headers(integration)

    def test_none_auth_never_requires_secret(self):
        headers = _headers(self.integration(auth_type="none"))
        self.assertEqual(headers["Accept"], "application/json")
        self.assertNotIn("Authorization", headers)


if __name__ == "__main__":
    unittest.main(verbosity=2)
