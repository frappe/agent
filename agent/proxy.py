import os
from hashlib import sha512 as sha

from agent.server import Server
from agent.job import step, job
from pathlib import Path


class Proxy(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]

        self.proxy_directory = self.config["proxy_directory"]
        self.upstreams_directory = os.path.join(
            self.proxy_directory, "upstreams"
        )
        self.hosts_directory = os.path.join(self.proxy_directory, "hosts")

        self.job = None
        self.step = None

    @job("Add Upstream to Proxy")
    def add_host_job(self, host):
        self.add_host(host)
        self.generate_hosts_config()
        self.reload_nginx()

    @step("Add Host to Proxy")
    def add_host(self, host):
        if not os.path.exists(self.hosts_directory):
            os.mkdir(self.hosts_directory)
        host_file = os.path.join(self.hosts_directory, host)
        Path(host_file).touch()

    @job("Add Site to Upstream")
    def add_site_to_upstream_job(self, upstream, site):
        self.add_site_to_upstream(upstream, site)
        self.generate_upstream_map()
        self.reload_nginx()

    @step("Add Site to Upstream")
    def add_site_to_upstream(self, upstream, site):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        site_file = os.path.join(upstream_directory, site)
        Path(site_file).touch()

    @job("Add Upstream to Proxy")
    def add_upstream_job(self, upstream):
        self.add_upstream(upstream)
        self.generate_upstream_list()
        self.reload_nginx()

    @step("Add Upstream to Proxy")
    def add_upstream(self, upstream):
        if not os.path.exists(self.upstreams_directory):
            os.mkdir(self.upstreams_directory)
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        os.mkdir(upstream_directory)

    @job("Remove Site from Upstream")
    def remove_site_from_upstream_job(self, upstream, site):
        self.remove_site_from_upstream(upstream, site)
        self.generate_upstream_map()
        self.reload_nginx()

    @step("Remove Site from Upstream")
    def remove_site_from_upstream(self, upstream, site):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        site_file = os.path.join(upstream_directory, site)
        os.remove(site_file)

    @step("Reload NGINX")
    def reload_nginx(self):
        return self.execute("sudo service reload nginx")

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

    @step("Generate Upstream List Configuration")
    def generate_upstream_list(self):
        upstream_list_file = os.path.join(
            self.proxy_directory, "upstreams.list"
        )
        self._render_template(
            "proxy/upstreams.list.jinja2",
            {"upstreams": self.upstreams},
            upstream_list_file,
        )

    @step("Generate Upstream Map Configuration")
    def generate_upstream_map(self):
        upstream_map_file = os.path.join(self.proxy_directory, "upstreams.map")
        self._render_template(
            "proxy/upstreams.map.jinja2",
            {"upstreams": self.upstreams},
            upstream_map_file,
        )

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
        hosts = []
        for host in os.listdir(self.hosts_directory):
            hosts.append(host)
        return hosts
