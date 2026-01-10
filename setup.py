from setuptools import find_packages, setup

with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")


setup(
    name="agent",
    version="0.0.0",
    description="Frappe Press Agent",
    url="http://github.com/frappe/agent",
    author="Frappe Technologies",
    author_email="developers@frappe.io",
    packages=find_packages(),
    zip_safe=False,
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            "agent = agent.cli:cli",
        ],
    },
)
