import argparse

import frappe
import subprocess

def commit_exists(commit: str, repo_dir: str | None = None) -> bool:
    """Return True if `commit` exists in the local object database, else False."""
    result = subprocess.run(
        ["git", "show", "--stat", "--oneline", commit],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return True
    elif result.returncode == 128:
        # Missing/unknown commit (or not a git repo)
        return False
    raise RuntimeError(f"git show failed (code {result.returncode}): {result.stderr.strip()}")
parser = argparse.ArgumentParser(description="Process a single app name.")

# Define a single positional argument (or use --app for an optional flag)
parser.add_argument("app", type=str, help="Name of a single app")

args = parser.parse_args()
# Initialize Frappe with an empty site string (exactly how bench build does it)
frappe.init("")
apps = [args.app]
try:
    # Import your function
    from frappe.commands.utils import run_after_build_hook
    # Check if its running a version of frappe which has islands befoe building 
    if commit_exists("06b36b121e8f2a39e98b537a391550dca22a97d2", "/home/frappe/context/apps/frappe"):
        from frappe.bundler import build_framework_islands
        # enable once in release
        build_framework_islands(True)
    # Pass the list of apps you want to simulate as built
    run_after_build_hook(apps)

finally:
    frappe.destroy()
