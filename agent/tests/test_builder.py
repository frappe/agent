from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch


class TestCheckVersion(unittest.TestCase):
    def test_check_version(self):
        from agent.builder import ValidationManager

        cases = [
            # python requires-python (SimpleSpec syntax)
            ("3.11.0", ">=3.10", True),
            ("3.9.0", ">=3.10", False),
            ("3.11", ">=3.10,<3.13", True),
            ("3.13", ">=3.10,<3.13", False),
            # node engines (npm syntax)
            ("18.16.0", ">=18", True),
            ("16.20.0", ">=18", False),
            ("18.16.0", "^18.0.0", True),
            ("20.1.0", "^18.0.0", False),
            ("18.16.0", ">=18 <21", True),
            ("22.0.0", ">=18 <21", False),
            ("18.16.0", "18.x", True),
            ("20.0.0", "18.x", False),
            ("20.0.0", "20 || 22", True),
            ("21.0.0", "20 || 22", False),
            ("18.16.0", "*", True),
        ]

        for actual, expected, want in cases:
            with self.subTest(actual=actual, expected=expected):
                self.assertEqual(ValidationManager.check_version(actual, expected), want)


class TestRunRemoteBuilderCleanup(unittest.TestCase):
    def setUp(self):
        from agent.builder import ImageBuilder

        self.cwd = os.getcwd()
        self.directory = tempfile.mkdtemp()
        os.chdir(self.directory)
        self.builder = ImageBuilder(
            image_repository="registry/bench",
            image_tag="tag",
            no_cache=False,
            no_push=True,
            registry={},
            platform="x86_64",
            build_token="token",
            dockerfile="",
            clone_instructions=[],
            group="bench-1",
            build_name="build-1",
            deploy_candidate_params={},
        )

    def tearDown(self):
        os.chdir(self.cwd)
        shutil.rmtree(self.directory)

    def run_builder(self, prepare_error=None, validate_error=None):
        from agent.builder import ImageBuilder

        def prepare_build_context():
            os.makedirs(self.builder.build_directory)
            if prepare_error:
                raise prepare_error

        context_manager = self.builder.context_manager
        with ExitStack() as stack:
            stack.enter_context(patch.object(context_manager, "clone_repositories"))
            stack.enter_context(
                patch.object(context_manager, "prepare_build_context", side_effect=prepare_build_context)
            )
            stack.enter_context(
                patch.object(self.builder.validation_manager, "validate", side_effect=validate_error)
            )
            stack.enter_context(
                patch.object(ImageBuilder, "_cleanup_context", new=ImageBuilder._cleanup_context.__wrapped__)
            )
            ImageBuilder.run_remote_builder.__wrapped__(self.builder)

    def test_failed_validation_removes_the_build_directory(self):
        with self.assertRaisesRegex(Exception, "Missing dependency"):
            self.run_builder(validate_error=Exception("Missing dependency"))

        self.assertFalse(os.path.exists(self.builder.build_directory))

    def test_failure_after_prepare_makes_the_directory_removes_it(self):
        error = Exception("App has invalid pyproject.toml file")
        with self.assertRaisesRegex(Exception, "invalid pyproject.toml"):
            self.run_builder(prepare_error=error)

        self.assertFalse(os.path.exists(self.builder.build_directory))
