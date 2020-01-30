import json
import click

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


@setup.command()
@click.option("--name", required=True)
@click.option("--user", default="frappe")
@click.option("--workers", required=True, type=int)
def config(name, user, workers):
    config = {
        "benches_directory": f"/home/{user}/benches",
        "proxy_directory": f"/home/{user}/nginx",
        "name": name,
        "redis_port": 25025,
        "user": user,
        "workers": workers,
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
def database():
    from agent.job import agent_database as database
    from agent.job import JobModel, StepModel

    database.create_tables([JobModel, StepModel])
