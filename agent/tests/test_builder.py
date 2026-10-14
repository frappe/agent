from __future__ import annotations

import importlib
import os
import shutil
import sys
import tarfile
import tempfile
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

import agent


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


class TestBuildContext(unittest.TestCase):
    def test_build_context_includes_the_asset_store_script(self):
        from agent.builder import ContextManager

        build_config_path = os.path.join(os.path.dirname(agent.__file__), "build_configs")
        with tempfile.TemporaryDirectory() as build_directory:
            context = SimpleNamespace(build_config_path=build_config_path, build_directory=build_directory)
            ContextManager._copy_build_config_files(context)
            self.assertTrue(os.path.isfile(os.path.join(build_directory, "get_cached_app.py")))


class TestStoredAssetsInBuildContext(unittest.TestCase):
    def test_stored_assets_are_downloaded_into_the_build_context(self):
        from agent.builder import ContextManager

        def download(url, path):
            with open(path, "wb") as f:
                f.write(b"partial")
            if url == "https://store/insights":
                raise requests.ConnectionError("store unreachable")

        with tempfile.TemporaryDirectory() as build_directory:
            context = SimpleNamespace(
                build_directory=build_directory,
                clone_instructions=[{"app": "frappe"}, {"app": "builder"}, {"app": "insights"}],
                asset_store=[
                    {"app": "builder", "hash": "a", "download_url": "https://store/builder"},
                    {"app": "insights", "hash": "b", "download_url": "https://store/insights"},
                ],
            )
            with patch("agent.builder.download_file", side_effect=download):
                ContextManager._add_stored_assets(context)

            stored = os.path.join(build_directory, "stored_assets")
            self.assertEqual(sorted(os.listdir(stored)), ["builder", "frappe", "insights"])
            self.assertTrue(os.path.isfile(os.path.join(stored, "builder", "assets.tar.gz")))
            # A failed download leaves nothing behind, so the build builds the assets itself
            self.assertEqual(os.listdir(os.path.join(stored, "insights")), [])


class TestAssetUploadAfterBuild(unittest.TestCase):
    def get_builder(self, asset_store: list[dict]) -> SimpleNamespace:
        return SimpleNamespace(asset_store=asset_store, data={}, _get_image_name=lambda: "repo:tag")

    def test_failed_build_uploads_nothing(self):
        from agent.builder import ImageBuilder

        builder = SimpleNamespace(
            _build_image=lambda context_tar_filepath: None,
            build_failed=True,
            data={},
            _upload_built_assets=MagicMock(),
        )
        ImageBuilder._build_and_push(builder, "context.tar.gz")
        builder._upload_built_assets.assert_not_called()

    def test_only_assets_with_an_upload_url_are_uploaded_from_the_image(self):
        from agent.builder import ImageBuilder

        builder = self.get_builder(
            [
                {"app": "builder", "hash": "a", "upload_url": "https://store/put/builder"},
                {"app": "insights", "hash": "b", "upload_url": "https://store/put/insights"},
                {"app": "crm", "hash": "c", "download_url": "https://store/get/crm"},
            ]
        )

        def upload(container, app, upload_url):
            if app == "insights":
                raise requests.ConnectionError("store unreachable")

        client = MagicMock()
        with patch("agent.builder.docker.from_env", return_value=client), patch(
            "agent.builder.upload_app_assets", side_effect=upload
        ) as upload_app_assets:
            ImageBuilder._upload_built_assets(builder)

        client.containers.create.assert_called_once_with("repo:tag", command="true")
        self.assertEqual([c.args[1] for c in upload_app_assets.call_args_list], ["builder", "insights"])
        self.assertEqual(builder.data["asset_store"]["builder"], "Uploaded")
        self.assertTrue(builder.data["asset_store"]["insights"].startswith("Failed"))
        client.containers.create.return_value.remove.assert_called_once_with(force=True)

    def test_public_folder_is_packed_as_the_build_unpacks_it(self):
        from agent.builder import repack_public_folder

        with tempfile.TemporaryDirectory() as directory:
            public = os.path.join(directory, "public")
            os.makedirs(os.path.join(public, "dist", "js"))
            with open(os.path.join(public, "dist", "js", "app.bundle.js"), "w") as f:
                f.write("console.log(1)")
            os.symlink(
                "/home/frappe/frappe-bench/apps/builder/node_modules", os.path.join(public, "node_modules")
            )

            exported = os.path.join(directory, "public.tar")
            with tarfile.open(exported, "w") as tar:
                tar.add(public, arcname="public")

            tarball = os.path.join(directory, "assets.tar.gz")
            repack_public_folder(exported, tarball, "builder")
            with tarfile.open(tarball) as tar:
                names = tar.getnames()

        self.assertIn("builder/dist/js/app.bundle.js", names)
        self.assertNotIn("builder/node_modules", names)
        self.assertFalse([name for name in names if not name.startswith("builder")])


def import_get_cached_app():
    """Import the build script without bench installed or build arguments."""
    bench_cli = ModuleType("bench.cli")
    bench_cli.change_working_directory = lambda: None
    stubs = {"bench": ModuleType("bench"), "bench.cli": bench_cli}
    argv = ["get_cached_app.py", "builder", "hash"]
    with patch.dict(sys.modules, stubs), patch.object(sys, "argv", argv):
        return importlib.import_module("agent.build_configs.get_cached_app")


class TestGetCachedApp(unittest.TestCase):
    def setUp(self):
        self.script = import_get_cached_app()
        self.bench = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.bench)
        # extract_assets changes into the bench, which is deleted after the test
        self.addCleanup(os.chdir, os.getcwd())
        os.makedirs(os.path.join(self.bench, "sites", "assets"))
        self.app_node_modules = os.path.join(self.bench, "apps", "builder", "node_modules")
        os.makedirs(os.path.join(self.app_node_modules, "vue"))
        self.public = os.path.join(self.bench, "apps", "builder", "builder", "public")
        self.write(os.path.join(self.public, "frontend", "index.html"), "old")
        self.assets = os.path.join(self.bench, "sites", "assets", "builder")
        os.symlink(self.public, self.assets)

    def write(self, path: str, content: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)

    def make_tarball(self, files: dict[str, str]) -> str:
        source = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, source)
        tarball = os.path.join(source, "assets.tar.gz")
        with tarfile.open(tarball, "w:gz") as tar:
            for name, content in files.items():
                path = os.path.join(source, "files", name.replace("/", "_"))
                self.write(path, content)
                tar.add(path, arcname=name)
        return tarball

    def extract(self, tarball: str):
        with patch.object(self.script, "change_working_directory", lambda: os.chdir(self.bench)):
            self.script.extract_assets("builder", tarball)

    @unittest.skipUnless(hasattr(tarfile, "data_filter"), "needs tarfile extraction filters")
    def test_stored_assets_replace_the_app_public_folder(self):
        self.extract(self.make_tarball({"builder/frontend/index.html": "stored"}))

        with open(os.path.join(self.public, "frontend", "index.html")) as f:
            self.assertEqual(f.read(), "stored")
        self.assertEqual(os.readlink(self.assets), self.public)
        self.assertEqual(os.readlink(os.path.join(self.public, "node_modules")), self.app_node_modules)

    def test_truncated_stored_assets_are_refused(self):
        tarball = self.make_tarball({"builder/frontend/index.html": "stored" * 10000})
        with open(tarball, "r+b") as f:
            f.truncate(os.path.getsize(tarball) // 2)

        with self.assertRaises(self.script.UNUSABLE_ARCHIVE_ERRORS):
            self.extract(tarball)

        with open(os.path.join(self.public, "frontend", "index.html")) as f:
            self.assertEqual(f.read(), "old")

    def test_stored_assets_that_write_outside_their_folder_are_refused(self):
        tarball = self.make_tarball(
            {"builder/frontend/index.html": "stored", "../../apps/builder/hooks.py": "x"}
        )

        with self.assertRaises(self.script.UNUSABLE_ARCHIVE_ERRORS):
            self.extract(tarball)

        with open(os.path.join(self.public, "frontend", "index.html")) as f:
            self.assertEqual(f.read(), "old")
        self.assertFalse(os.path.exists(os.path.join(self.bench, "apps", "builder", "hooks.py")))
