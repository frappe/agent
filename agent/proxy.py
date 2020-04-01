import json
import os
import shutil
from hashlib import sha512 as sha

from agent.server import Server
from agent.job import step, job
from pathlib import Path


class Proxy(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]

        self.nginx_directory = self.config["nginx_directory"]
        self.upstreams_directory = os.path.join(
            self.nginx_directory, "upstreams"
        )
        self.hosts_directory = os.path.join(self.nginx_directory, "hosts")

        self.job = None
        self.step = None

    @job("Add Host to Proxy")
    def add_host_job(self, host, target, certificate):
        self.add_host(host, target, certificate)
        self.generate_proxy_config()
        self.reload_nginx()

    @step("Add Host to Proxy")
    def add_host(self, host, target, certificate):
        if not os.path.exists(self.hosts_directory):
            os.mkdir(self.hosts_directory)

        host_directory = os.path.join(self.hosts_directory, host)
        if not os.path.exists(host_directory):
            os.mkdir(host_directory)

        map_file = os.path.join(host_directory, "map.json")
        json.dump({host: target}, open(map_file, "w"), indent=4)

        for key, value in certificate.items():
            with open(os.path.join(host_directory, key), "w") as f:
                f.write(value)

    @job("Add Site to Upstream")
    def add_site_to_upstream_job(self, upstream, site):
        self.add_site_to_upstream(upstream, site)
        self.generate_upstream_map()
        self.reload_nginx()

    @step("Add Site File to Upstream Directory")
    def add_site_to_upstream(self, upstream, site):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        site_file = os.path.join(upstream_directory, site)
        Path(site_file).touch()

    @job("Add Upstream to Proxy")
    def add_upstream_job(self, upstream):
        self.add_upstream(upstream)
        self.generate_upstream_list()
        self.reload_nginx()

    @step("Add Upstream Directory")
    def add_upstream(self, upstream):
        if not os.path.exists(self.upstreams_directory):
            os.mkdir(self.upstreams_directory)
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        os.mkdir(upstream_directory)

    @job("Remove Host from Proxy")
    def remove_host_job(self, host):
        self.remove_host(host)
        self.generate_proxy_config()
        self.reload_nginx()

    @step("Remove Host from Proxy")
    def remove_host(self, host):
        host_directory = os.path.join(self.hosts_directory, host)
        shutil.rmtree(host_directory)

    @job("Remove Site from Upstream")
    def remove_site_from_upstream_job(self, upstream, site):
        self.remove_site_from_upstream(upstream, site)
        self.generate_upstream_map()
        self.reload_nginx()

    @step("Remove Site File from Upstream Directory")
    def remove_site_from_upstream(self, upstream, site):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        site_file = os.path.join(upstream_directory, site)
        os.remove(site_file)

    @step("Reload NGINX")
    def reload_nginx(self):
        return self.execute("sudo systemctl reload nginx")

    @step("Generate NGINX Root Configuration")
    def generate_nginx_root_config(self):
        nginx_config_file = os.path.join(self.proxy_directory, "nginx.conf")
        self._render_template(
            "proxy/nginx.conf.jinja2", {}, nginx_config_file,
        )

    @step("Generate Hosts Configuration")
    def generate_hosts_config(self):
        hosts_config_file = os.path.join(self.proxy_directory, "hosts.conf")
        self._render_template(
            "proxy/hosts.conf.jinja2",
            {"hosts": self.hosts},
            hosts_config_file,
        )

    def setup_proxy(self):
        self._create_default_host()
        self._generate_proxy_config()
        self._reload_nginx()

    def _create_default_host(self):
        default_host = f"*.{self.config['domain']}"
        default_host_directory = os.path.join(
            self.hosts_directory, default_host
        )
        if not os.path.exists(default_host_directory):
            os.mkdir(default_host_directory)
        map_file = os.path.join(default_host_directory, "map.json")
        json.dump({"default": "$host"}, open(map_file, "w"), indent=4)

        tls_directory = self.config["tls_directory"]
        for f in ["chain.pem", "fullchain.pem", "privkey.pem"]:
            source = os.path.join(tls_directory, f)
            destination = os.path.join(default_host_directory, f)
            os.remove(destination)
            os.symlink(source, destination)

    @property
    def upstreams(self):
        upstreams = {}
        for upstream in os.listdir(self.upstreams_directory):
            upstream_directory = os.path.join(
                self.upstreams_directory, upstream
            )
            if os.path.isdir(upstream_directory):
                hashed_upstream = sha(upstream.encode()).hexdigest()[:16]
                upstreams[upstream] = {"sites": [], "hash": hashed_upstream}
                for site in os.listdir(upstream_directory):
                    upstreams[upstream]["sites"].append(site)
        return upstreams

    @property
    def hosts(self):
        hosts = {}
        for host in os.listdir(self.hosts_directory):
            host_directory = os.path.join(self.hosts_directory, host)
            map_file = os.path.join(host_directory, "map.json")
            if os.path.exists(map_file):
                hosts[host] = json.load(open(map_file))
        return hosts
