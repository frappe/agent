import json
import os
import shutil

import click
import requests

from agent.proxy import Proxy
from agent.server import Server


@click.group()
def cli():
    pass


@cli.group()
def setup():
    pass


@cli.command()
def update():
    Server().update_agent_cli()


@cli.command()
@click.option("--password", required=True)
def ping_server(password: str):
    """Ping web api on localhost and check for pong."""
    res = requests.get(
        "http://localhost:25052/ping",
        headers={"Authorization": f"bearer {password}"},
    )
    res = res.json()
    if res["message"] != "pong":
        raise Exception("pong not in response")
    print(res)


@setup.command()
@click.option("--name", required=True)
@click.option("--user", default="frappe")
@click.option("--workers", required=True, type=int)
def config(name, user, workers):
    config = {
        "benches_directory": f"/home/{user}/benches",
        "name": name,
        "tls_directory": f"/home/{user}/agent/tls",
        "nginx_directory": f"/home/{user}/agent/nginx",
        "redis_port": 25025,
        "user": user,
        "workers": workers,
        "gunicorn_workers": 2,
        "web_port": 25052,
    }
    json.dump(config, open("config.json", "w"), sort_keys=True, indent=4)


@setup.command()
@click.option("--password", prompt=True, hide_input=True)
def authentication(password):
    Server().setup_authentication(password)


@setup.command()
def supervisor():
    Server().setup_supervisor()


@setup.command()
def nginx():
    Server().setup_nginx()


@setup.command()
@click.option("--domain")
def proxy(domain=None):
    proxy = Proxy()
    if domain:
        config = proxy.config
        config["domain"] = domain
        proxy.setconfig(config, indent=4)
    proxy.setup_proxy()


@setup.command()
def database():
    from agent.job import JobModel, StepModel
    from agent.job import agent_database as database

    database.create_tables([JobModel, StepModel])


@setup.command()
def usage():
    import os
    import sys

    from crontab import CronTab

    script_directory = os.path.dirname(__file__)
    agent_directory = os.path.dirname(os.path.dirname(script_directory))
    logs_directory = os.path.join(agent_directory, "logs")
    script = os.path.join(script_directory, "usage.py")
    stdout = os.path.join(logs_directory, "usage.log")
    stderr = os.path.join(logs_directory, "usage.error.log")

    cron = CronTab(user=True)
    command = (
        f"cd {agent_directory} && {sys.executable} {script}"
        f" 1>> {stdout} 2>> {stderr}"
    )

    if command not in str(cron):
        job = cron.new(command=command)
        job.every(6).hours()
        job.minute.on(30)
        cron.write()


@cli.group()
def run():
    pass


@run.command()
def web():
    executable = shutil.which("gunicorn")
    port = Server().config["web_port"]
    arguments = [
        executable,
        "--bind",
        f"127.0.0.1:{port}",
        "--reload",
        "--preload",
        "agent.web:application",
    ]
    os.execv(executable, arguments)


@run.command()
def worker():
    executable = shutil.which("rq")
    port = Server().config["redis_port"]
    arguments = [
        executable,
        "worker",
        "--url",
        f"redis://127.0.0.1:{port}",
    ]
    os.execv(executable, arguments)
