from setuptools import find_packages, setup

with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")

with open("requirements-ai.txt") as f:
    ai_requires = [line.strip() for line in f if line.strip() and not line.startswith("#")]

with open("requirements-channel.txt") as f:
    channel_requires = [line.strip() for line in f if line.strip() and not line.startswith("#")]

with open("requirements-a2a.txt") as f:
    a2a_requires = [line.strip() for line in f if line.strip() and not line.startswith("#")]


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
            "agent-ai-mcp = agent.ai_control.mcp_server:main",
            "agent-ai-a2a = agent.ai_control.a2a_server:main",
            "agent-ai-a2a-check = agent.ai_control.a2a_cli:main",
        ],
    },
    extras_require={
        "ai": ai_requires,
        "channel": channel_requires,
        "ai-channel": ai_requires + channel_requires,
        "a2a": a2a_requires,
        "ai-a2a": ai_requires + a2a_requires,
    },
)
