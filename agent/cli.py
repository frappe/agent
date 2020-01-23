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
@click.option("--name", required=True)
@click.option("--workers", required=True, type=int)
def config(name, workers):
    config = {
        "benches_directory": "/home/frappe/benches",
        "proxy_base_directory": "/home/frappe/nginx",
        "name": name,
        "redis_port": 25025,
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
def ssl():
    Server().setup_ssl()
    