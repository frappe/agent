"""Install an app in an image build, reusing its built assets if the agent found them in the store.

The agent downloads stored assets into the build context before the build and uploads new ones
from the finished image, so nothing in the build ever gets access to the store.
Usage: get_cached_app.py <app> <commit hash>
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zlib

from bench.cli import change_working_directory

APP_NAME, APP_HASH = sys.argv[1:3]
STORED_ASSETS = f"/home/frappe/context/stored_assets/{APP_NAME}/assets.tar.gz"
# A truncated or corrupt tarball raises these; the build then builds the assets itself
UNUSABLE_ARCHIVE_ERRORS = (OSError, EOFError, RuntimeError, tarfile.TarError, zlib.error)


def _write_assets(file: os.DirEntry, assets_file: str, relative_path: str):
    if os.path.exists(assets_file):
        with open(assets_file, "r+") as f:
            data = json.load(f)
            key_parts = file.name.rsplit(".", maxsplit=2)
            if len(key_parts) > 2:
                key_parts.pop(-2)  # Remove hash part
            key = ".".join(key_parts)

            if key not in data:
                data[key] = relative_path

            f.seek(0)
            json.dump(data, f, indent=4)
            f.truncate()
    else:
        with open(assets_file, "w") as f:
            json.dump({file.name: relative_path}, f, indent=4)


def _write_js_and_css_assets(app: str, dist_folder: str) -> None:
    assets_file = os.path.join(os.getcwd(), "sites", "assets", "assets.json")
    assets_rtl_file = os.path.join(os.getcwd(), "sites", "assets", "assets-rtl.json")

    for assets in ["js", "css"]:
        assets_path = os.path.join(dist_folder, assets)

        if not os.path.exists(assets_path):
            continue

        for file in os.scandir(assets_path):
            if file.name.endswith(".map"):
                continue

            relative_path = f"/assets/{app}/dist/{assets}/{file.name}"
            _write_assets(file, assets_file, relative_path)

    css_rtl_path = os.path.join(dist_folder, "css-rtl")
    if not os.path.exists(css_rtl_path):
        return

    for file in os.scandir(css_rtl_path):
        if file.name.endswith(".map"):
            continue

        relative_path = f"/assets/{app}/dist/css-rtl/{file.name}"
        _write_assets(file, assets_rtl_file, relative_path)


def _update_assets_json(app: str) -> None:
    """Update assets.json to include the current app's assets."""
    change_working_directory()

    base_assets_path = os.path.join(os.getcwd(), "sites", "assets")
    dist_folder = os.path.join(base_assets_path, app, "dist")

    if not os.path.exists(dist_folder):
        # We don't need to update assets.json if dist folder doesn't exist
        return

    _write_js_and_css_assets(app, dist_folder)


def build_assets(app: str) -> str:
    """Build assets for the app using the app name and return the path to assets"""
    change_working_directory()
    env = os.environ.copy()
    env["FRAPPE_DOCKER_BUILD"] = "True"
    completed_process = subprocess.run(
        ["bench", "build", "--app", app, "--production"],
        check=True,
        env=env,
    )
    assets_path = os.path.join(os.getcwd(), "sites", "assets", app)
    print(f"Build Command Completed. Return Code: {completed_process.returncode}.")
    return assets_path


def link_node_modules(bench_path: str, app_name: str, app_public_path: str) -> None:
    """Link the app's node_modules into its assets like bench build does, as the tarball leaves it out."""
    node_modules = os.path.join(bench_path, "apps", app_name, "node_modules")
    link = os.path.join(app_public_path, "node_modules")
    if os.path.isdir(node_modules) and not os.path.lexists(link):
        os.symlink(node_modules, link)


def extract_assets(app_name: str, tar_path: str) -> None:
    """Replace the app's public folder with the stored one, and restore the links bench build makes."""
    if not hasattr(tarfile, "data_filter"):
        raise RuntimeError("this Python can't extract archives safely")

    change_working_directory()
    bench_path = os.getcwd()
    app_public_path = os.path.join(bench_path, "apps", app_name, app_name, "public")
    assets_path = os.path.join(bench_path, "sites", "assets", app_name)

    with tempfile.TemporaryDirectory(dir=os.path.join(bench_path, "sites")) as extract_path:
        # The data filter refuses paths and links that point outside extract_path
        with tarfile.open(tar_path, mode="r:gz") as tar:
            tar.extractall(path=extract_path, filter="data")

        extracted = os.path.join(extract_path, app_name)
        if not os.path.isdir(extracted):
            raise RuntimeError(f"stored assets have no {app_name} folder")

        if os.path.islink(assets_path):
            os.unlink(assets_path)
        elif os.path.exists(assets_path):
            shutil.rmtree(assets_path)
        if os.path.exists(app_public_path):
            shutil.rmtree(app_public_path)
        shutil.move(extracted, app_public_path)

    # Restore symlink since we deploy with cp -LR
    os.symlink(app_public_path, assets_path)
    link_node_modules(bench_path, app_name, app_public_path)

    print(f"Assets moved to {app_public_path} and symlink restored at {assets_path}")


def main():
    """Use the app's stored assets if the agent put them in the build context, else build them"""
    env = os.environ.copy()
    env["FRAPPE_DOCKER_BUILD"] = "True"
    app_path = f"file:///home/frappe/context/apps/{APP_NAME}"

    print("Fetching app without assets...")
    subprocess.run(["bench", "get-app", app_path, "--skip-assets"], check=True, env=env)

    asset_filename = f"{APP_NAME}.{APP_HASH}.tar.gz"
    if os.path.isfile(STORED_ASSETS):
        print(f"Assets {asset_filename} found in store. Extracting and setting up...")
        try:
            extract_assets(APP_NAME, STORED_ASSETS)
            _update_assets_json(APP_NAME)
            return
        except UNUSABLE_ARCHIVE_ERRORS as e:
            print(f"Stored assets could not be used: {e}")

    print(f"Assets {asset_filename} not found in store. Building...")
    build_assets(APP_NAME)


if __name__ == "__main__":
    main()
