import argparse

import frappe

parser = argparse.ArgumentParser(description="Process a single app name.")

# Define a single positional argument (or use --app for an optional flag)
parser.add_argument("app", type=str, help="Name of a single app")

args = parser.parse_args()
# Initialize Frappe with an empty site string (exactly how bench build does it)
frappe.init("")
apps = [args.app]
try:
    # Import your function
    # Check if its running a version of frappe which has islands befoe building
    import frappe.bundler
    from frappe.commands.utils import run_after_build_hook

    build_framework_islands = getattr(frappe.bundler, "build_framework_islands", None)
    if build_framework_islands:
        # enable once in release
        frappe.bundler.build_framework_islands(True)
    # Pass the list of apps you want to simulate as built
    run_after_build_hook(apps)

finally:
    frappe.destroy()
