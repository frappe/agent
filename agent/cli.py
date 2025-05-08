from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import click
import requests

from agent.proxy import Proxy
from agent.server import Server
from agent.utils import get_timestamp

if TYPE_CHECKING:
    from IPython.terminal.embed import InteractiveShellEmbed


@click.group()
def cli():
    pass


@cli.group()
def setup():
    pass


@cli.command()
@click.option("--restart-web-workers", default=True)
@click.option("--restart-rq-workers", default=True)
@click.option("--restart-redis", default=True)
@click.option("--skip-repo-setup", default=False)
@click.option("--skip-patches", default=False)
def update(restart_web_workers, restart_rq_workers, restart_redis, skip_repo_setup, skip_patches):
    Server().update_agent_cli(
        restart_redis=restart_redis,
        restart_rq_workers=restart_rq_workers,
        restart_web_workers=restart_web_workers,
        skip_repo_setup=skip_repo_setup,
        skip_patches=skip_patches,
    )


@cli.command()
def run_patches():
    from agent.patch_handler import run_patches

    run_patches()


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
@click.option("--proxy-ip", required=False, type=str, default=None)
@click.option("--sentry-dsn", required=False, type=str)
@click.option("--press-url", required=False, type=str)
def config(name, user, workers, proxy_ip=None, sentry_dsn=None, press_url=None):
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
        "press_url": "https://frappecloud.com",
    }
    if press_url:
        config["press_url"] = press_url
    if proxy_ip:
        config["proxy_ip"] = proxy_ip
    if sentry_dsn:
        config["sentry_dsn"] = sentry_dsn

    with open("config.json", "w") as f:
        json.dump(config, f, sort_keys=True, indent=4)


@setup.command()
def pyspy():
    privileges_line = "frappe ALL = (root) NOPASSWD: /home/frappe/agent/env/bin/py-spy"
    with open("/etc/sudoers.d/frappe", "a+") as sudoers:
        sudoers.seek(0)
        lines = sudoers.read().splitlines()

        if privileges_line not in lines:
            sudoers.write(privileges_line + "\n")


@setup.command()
@click.option("--password", prompt=True, hide_input=True)
def authentication(password):
    Server().setup_authentication(password)


@setup.command()
@click.option("--sentry-dsn", required=True)
def sentry(sentry_dsn):
    Server().setup_sentry(sentry_dsn)


@setup.command()
def supervisor():
    Server().setup_supervisor()


@setup.command()
def nginx():
    Server().setup_nginx()


@setup.command()
@click.option("--domain")
@click.option("--press-url")
def proxy(domain=None, press_url=None):
    proxy = Proxy()
    if domain:
        config = proxy.get_config(for_update=True)
        config["domain"] = domain
        config["press_url"] = press_url
        proxy.set_config(config, indent=4)
    proxy.setup_proxy()


@setup.command()
@click.option("--domain")
def standalone(domain=None):
    server = Server()
    if domain:
        config = server.get_config(for_update=True)
        config["domain"] = domain
        config["standalone"] = True
        server.set_config(config, indent=4)


@setup.command()
def database():
    from agent.job import JobModel, PatchLogModel, StepModel
    from agent.job import agent_database as database

    database.create_tables([JobModel, StepModel, PatchLogModel])


@setup.command()
def site_analytics():
    from crontab import CronTab

    script_directory = os.path.dirname(__file__)
    agent_directory = os.path.dirname(os.path.dirname(script_directory))
    logs_directory = os.path.join(agent_directory, "logs")
    script = os.path.join(script_directory, "analytics.py")
    stdout = os.path.join(logs_directory, "analytics.log")
    stderr = os.path.join(logs_directory, "analytics.error.log")

    cron = CronTab(user=True)
    command = f"cd {agent_directory} && {sys.executable} {script} 1>> {stdout} 2>> {stderr}"

    if command in str(cron):
        cron.remove_all(command=command)

    job = cron.new(command=command)
    job.hour.on(23)
    job.minute.on(0)
    cron.write()


@setup.command()
def usage():
    from crontab import CronTab

    script_directory = os.path.dirname(__file__)
    agent_directory = os.path.dirname(os.path.dirname(script_directory))
    logs_directory = os.path.join(agent_directory, "logs")
    script = os.path.join(script_directory, "usage.py")
    stdout = os.path.join(logs_directory, "usage.log")
    stderr = os.path.join(logs_directory, "usage.error.log")

    cron = CronTab(user=True)
    command = f"cd {agent_directory} && {sys.executable} {script} 1>> {stdout} 2>> {stderr}"

    if command not in str(cron):
        job = cron.new(command=command)
        job.every(6).hours()
        job.minute.on(30)
        cron.write()


@setup.command()
def rewrite_redis_aof_weekly():
    from crontab import CronTab

    script_directory = os.path.dirname(__file__)
    agent_directory = os.path.dirname(os.path.dirname(script_directory))
    logs_directory = os.path.join(agent_directory, "logs")
    script = os.path.join(script_directory, "rewrite_redis_aof.py")
    stdout = os.path.join(logs_directory, "rewrite_redis_aof.log")
    stderr = os.path.join(logs_directory, "rewrite_redis_aof.error.log")

    cron = CronTab(user=True)
    command = f"cd {agent_directory} && {sys.executable} {script} 1>> {stdout} 2>> {stderr}"

    if command not in str(cron):
        job = cron.new(command=command)
        job.setall("0 2 1,8,15,22 * *")
        cron.write()


@setup.command()
def nginx_defer_reload():
    from crontab import CronTab

    script_directory = os.path.dirname(__file__)
    agent_directory = os.path.dirname(os.path.dirname(script_directory))
    logs_directory = os.path.join(agent_directory, "logs")
    script = os.path.join(script_directory, "nginx_defer_reload.py")
    stdout = os.path.join(logs_directory, "nginx_defer_reload.log")
    stderr = os.path.join(logs_directory, "nginx_defer_reload.error.log")

    cron = CronTab(user=True)
    command = f"cd {agent_directory} && {sys.executable} {script} 1>> {stdout} 2>> {stderr}"

    if command not in str(cron):
        job = cron.new(command=command)
        job.minute.every(2)
        cron.write()


@setup.command()
def registry():
    Server().setup_registry()


@setup.command()
@click.option("--url", required=True)
@click.option("--token", required=True)
def monitor(url, token):
    from agent.monitor import Monitor

    server = Monitor()
    server.update_config({"monitor": True, "press_url": url, "press_token": token})
    server.discover_targets()


@setup.command()
def log():
    Server().setup_log()


@setup.command()
def analytics():
    Server().setup_analytics()


@setup.command()
def trace():
    Server().setup_trace()


@setup.command()
@click.option("--password", prompt=True, hide_input=True)
def proxysql(password):
    Server().setup_proxysql(password)


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


@cli.command()
def discover():
    from agent.monitor import Monitor

    Monitor().discover_targets()


@cli.group()
def bench():
    pass


@bench.command()
@click.argument("bench", required=False)
def start(bench):
    if bench:
        return Server().benches[bench].start()
    return Server().start_all_benches()


@bench.command()
@click.argument("bench", required=False)
def stop(bench):
    if bench:
        return Server().benches[bench].stop()
    return Server().stop_all_benches()


@cli.command(help="Run iPython console.")
@click.option(
    "--config-path",
    required=False,
    type=str,
    help="Path to agent config.json.",
)
def console(config_path):
    from atexit import register

    from IPython.terminal.embed import InteractiveShellEmbed

    terminal = InteractiveShellEmbed.instance()

    config_dir = get_config_dir(config_path)
    if config_dir:
        try:
            locals()["server"] = Server(config_dir)
            print(f"In namespace:\nserver = agent.server.Server('{config_dir}')")
        except Exception:
            print(f"Could not initialize agent.server.Server('{config_dir}')")

    elif config_path:
        print(f"Could not find config.json at '{config_path}'")
    else:
        print("Could not find config.json use --config-path to specify")

    register(store_ipython_logs, terminal, config_dir)

    # ref: https://stackoverflow.com/a/74681224
    try:
        from IPython.core import ultratb

        ultratb.VerboseTB._tb_highlight = "bg:ansibrightblack"
    except Exception:
        pass

    terminal.colors = "neutral"
    terminal.display_banner = False
    terminal()


def get_config_dir(config_path: str | None = None) -> str | None:
    cwd = os.getcwd()
    if config_path is None:
        config_path = cwd

    config_dir = Path(config_path)

    if config_dir.suffix == "json" and config_dir.exists():
        return config_dir.parent.as_posix()

    if config_dir.suffix != "":
        config_dir = config_dir.parent

    potential = [
        Path("/home/frappe/agent/config.json"),
        config_dir / "config.json",
        config_dir / ".." / "config.json",
    ]

    for p in potential:
        if not p.exists():
            continue
        try:
            return p.parent.relative_to(cwd).as_posix()
        except Exception:
            return p.parent.as_posix()
    return None


def store_ipython_logs(terminal: InteractiveShellEmbed, config_dir: str | None):
    if not config_dir:
        config_dir = os.getcwd()

    log_path = Path(config_dir) / "logs" / "agent_console.log"
    log_path.parent.mkdir(exist_ok=True)

    with log_path.open("a") as file:
        timestamp = get_timestamp()

        file.write(f"# SESSION BEGIN {timestamp}\n")
        for line in terminal.history_manager.get_range():
            file.write(f"{line[2]}\n")
        file.write(f"# SESSION END {timestamp}\n\n")
