import json
import click

from agent.server import Server


@click.group()
def cli():
    pass


@cli.group()
def setup():
    pass


@setup.command()
def config():
    config = {
        "benches_directory": "/home/frappe/benches",
        "name": "x.frappe.cloud",
        "redis_port": 25025,
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
