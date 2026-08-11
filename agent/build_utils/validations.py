from __future__ import annotations

import contextlib
import json
import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, TypedDict

import semantic_version as sv
import tomli


class PackageManagers(TypedDict):
    repo_path: str
    pyproject: dict[str, Any] | None
    packagejsons: list[dict[str, Any]]


# This is runtime resolutions therefore will have to use <3.9 python versions
PackageManagerFiles = Dict[str, PackageManagers]


def load_pyproject(app: str, pyproject_path: str):
    try:
        from tomli import TOMLDecodeError, load
    except ImportError:
        from tomllib import TOMLDecodeError, load  # type: ignore

    with open(pyproject_path, "rb") as f:
        try:
            return load(f)
        except TOMLDecodeError:
            # Do not edit without updating deploy_notifications.py
            raise Exception("App has invalid pyproject.toml file", app) from None


def load_package_json(app: str, package_json_path: str):
    with open(package_json_path, "rb") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            # Do not edit without updating deploy_notifications.py
            raise Exception("App has invalid package.json file", app, package_json_path) from None


def get_error_key(error_substring: str | list[str]) -> str:
    if isinstance(error_substring, list):
        error_substring = " ".join(error_substring)
    """
	Converts `MatchStrings` into error keys, these are set on
	DeployCandidates on UA Failures for two reasons:
	1. To check if a subsequent deploy will fail for the same reasons.
	2. To track the kind of UA errors the users are facing.
	"""

    return re.sub(
        r"[\"'\[\],:]|\.$",
        "",
        error_substring.lower(),
    )


def get_package_manager_files_from_repo(app: str, repo_path: str):
    pypt, pckjs = _get_package_manager_files_from_repo(
        repo_path,
        True,
    )

    pm: PackageManagers = {
        "repo_path": repo_path,
        "pyproject": None,
        "packagejsons": [],
    }

    if pypt is not None:
        pm["pyproject"] = load_pyproject(app, pypt.absolute().as_posix())

    for pckj in pckjs:
        package_json = load_package_json(
            app,
            pckj.absolute().as_posix(),
        )
        pm["packagejsons"].append(package_json)

    return pm


def _get_package_manager_files_from_repo(
    repo_path: str,
    recursive: bool,
) -> tuple[Path | None, list[Path]]:
    pyproject_toml: Path | None = None
    package_jsons: list[Path] = []  # An app can have multiple

    for p in Path(repo_path).iterdir():
        if p.name == "pyproject.toml":
            pyproject_toml = p
        elif p.name == "package.json":
            package_jsons.append(p)

        if not (recursive and p.is_dir()):
            continue

        pypt, pckjs = _get_package_manager_files_from_repo(p, False)
        if pypt is not None and pyproject_toml is None:
            pyproject_toml = pypt

        package_jsons.extend(pckjs)

    return pyproject_toml, package_jsons


def get_package_manager_files(repo_path_map: dict[str, str]) -> PackageManagerFiles:
    # Return pyproject.toml and package.json files
    pfiles_map = {}
    for app, repo_path in repo_path_map.items():
        pfiles_map[app] = get_package_manager_files_from_repo(app, repo_path)

    return pfiles_map


def check_python_syntax(dirpath: str) -> str:
    """
    Script `compileall` will compile all the Python files
    in the given directory.

    If there are errors then return code will be non-zero.

    Flags:
    - -q: quiet, only print errors (stdout)
    - -o: optimize level, 0 is no optimization
    """
    _python = get_python_path(dirpath)
    command = f"{_python} -m compileall -q -o 0 {dirpath}"
    proc = subprocess.run(
        shlex.split(command),
        text=True,
        capture_output=True,
    )
    if proc.returncode == 0:
        return ""

    if not proc.stdout:
        return proc.stderr

    return proc.stdout


def get_python_path(dirpath: str) -> str:
    """Check for python version in the pyproject.toml file if present else return bench python path"""
    pyproject_path = os.path.join(dirpath, "pyproject.toml")
    if os.path.isfile(pyproject_path):
        # To handle broken toml files or missing fields
        with open(pyproject_path, "rb") as f, contextlib.suppress(Exception):
            pyproject_data = tomli.load(f)
            requires_python = pyproject_data.get("project", {}).get("requires-python")
            if requires_python:
                version_spec = sv.SimpleSpec(requires_python)
                if version_spec.match(sv.Version("3.14.0")):
                    # try to resolve python3.14 path
                    python_path = shutil.which("python3.14")
                    if python_path:
                        return python_path
                    # Temporary hardcoding until python 3.14 until we move to build server
                    return "/usr/bin/python3.14"

    return _get_server_python_path()


def _get_server_python_path() -> str:
    """Get the agents python path"""
    return os.path.join(os.getcwd(), "env", "bin", "python")
