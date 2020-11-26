import json
import os
import shutil
from hashlib import sha512 as sha
from pathlib import Path
from typing import Dict
from collections import defaultdict

from agent.job import job, step
from agent.server import Server


class Proxy(Server):
    def __init__(self, directory=None):
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]
        self.domain = self.config["domain"]

        self.nginx_directory = self.config["nginx_directory"]
        self.upstreams_directory = os.path.join(
            self.nginx_directory, "upstreams"
        )
        self.hosts_directory = os.path.join(self.nginx_directory, "hosts")
        self.error_pages_directory = os.path.join(
            self.directory, "repo", "agent", "pages"
        )

        self.job = None
        self.step = None

    @job("Add Host to Proxy")
    def add_host_job(self, host, target, certificate):
        self.add_host(host, target, certificate)
        self.generate_proxy_config()
        self.reload_nginx()

    @step("Add Host to Proxy")
    def add_host(self, host, target, certificate):
        host_directory = os.path.join(self.hosts_directory, host)
        os.makedirs(host_directory, exist_ok=True)

        map_file = os.path.join(host_directory, "map.json")
        with open(map_file, "w") as m:
            json.dump({host: target}, m, indent=4)

        for key, value in certificate.items():
            with open(os.path.join(host_directory, key), "w") as f:
                f.write(value)

    @job("Add Site to Upstream")
    def add_site_to_upstream_job(self, upstream, site):
        self.add_site_to_upstream(upstream, site)
        self.generate_proxy_config()
        self.reload_nginx()

    @step("Add Site File to Upstream Directory")
    def add_site_to_upstream(self, upstream, site):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        site_file = os.path.join(upstream_directory, site)
        Path(site_file).touch()

    @job("Add Upstream to Proxy")
    def add_upstream_job(self, upstream):
        self.add_upstream(upstream)
        self.generate_proxy_config()
        self.reload_nginx()

    @step("Add Upstream Directory")
    def add_upstream(self, upstream):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        os.makedirs(upstream_directory, exist_ok=True)

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
        self.generate_proxy_config()
        self.reload_nginx()

    @step("Remove Site File from Upstream Directory")
    def remove_site_from_upstream(self, upstream, site):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        site_file = os.path.join(upstream_directory, site)
        os.remove(site_file)

    @job("Update Site Status")
    def update_site_status_job(self, upstream, site, status):
        self.update_site_status(upstream, site, status)
        self.generate_proxy_config()
        self.reload_nginx()

    @step("Update Site File")
    def update_site_status(self, upstream, site, status):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        site_file = os.path.join(upstream_directory, site)
        with open(site_file, "w") as f:
            f.write(status)

    @job("Setup Redirects on Hosts")
    def setup_redirects_job(self, hosts, target):
        self.setup_redirects(hosts, target)
        self.generate_proxy_config()
        self.reload_nginx()

    @step("Setup Redirects on Hosts")
    def setup_redirects(self, hosts, target):
        for host in hosts:
            host_directory = os.path.join(self.hosts_directory, host)
            os.makedirs(host_directory, exist_ok=True)
            redirect_file = os.path.join(host_directory, "redirect.json")
            if os.path.exists(redirect_file):
                with open(redirect_file) as r:
                    redirects = json.load(r)
            else:
                redirects = {}
            redirects[host] = target
            with open(redirect_file, "w") as r:
                json.dump(redirects, r, indent=4)

    @job("Remove Redirect on Host")
    def remove_redirect_job(self, host):
        self.remove_redirect(host)
        self.generate_proxy_config()
        self.reload_nginx()

    @step("Remove Redirect on Host")
    def remove_redirect(self, host):
        host_directory = os.path.join(self.hosts_directory, host)
        redirect_file = os.path.join(host_directory, "redirect.json")
        if os.path.exists(redirect_file):
            os.remove(redirect_file)
        if host.endswith("." + self.domain):
            # default domain
            os.rmdir(host_directory)

    @step("Reload NGINX")
    def reload_nginx(self):
        return self.execute("sudo systemctl reload nginx")

    @step("Generate NGINX Configuration")
    def generate_proxy_config(self):
        return self._generate_proxy_config()

    def _generate_proxy_config(self):
        proxy_config_file = os.path.join(self.nginx_directory, "proxy.conf")
        self._render_template(
            "proxy/nginx.conf.jinja2",
            {
                "hosts": self.hosts,
                "upstreams": self.upstreams,
                "domain": self.config["domain"],
                "nginx_directory": self.config["nginx_directory"],
                "error_pages_directory": self.error_pages_directory,
            },
            proxy_config_file,
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
        os.makedirs(default_host_directory, exist_ok=True)
        map_file = os.path.join(default_host_directory, "map.json")
        json.dump({"default": "$host"}, open(map_file, "w"), indent=4)

        tls_directory = self.config["tls_directory"]
        for f in ["chain.pem", "fullchain.pem", "privkey.pem"]:
            source = os.path.join(tls_directory, f)
            destination = os.path.join(default_host_directory, f)
            if os.path.exists(destination):
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
                    with open(os.path.join(upstream_directory, site)) as f:
                        status = f.read().strip()
                    if status in ("deactivated", "suspended"):
                        actual_upstream = status
                    else:
                        actual_upstream = hashed_upstream
                    upstreams[upstream]["sites"].append(
                        {"name": site, "upstream": actual_upstream}
                    )
        return upstreams

    @property
    def hosts(self) -> Dict[str, Dict[str, str]]:
        hosts = defaultdict(lambda: defaultdict(str))
        for host in os.listdir(self.hosts_directory):
            host_directory = os.path.join(self.hosts_directory, host)

            map_file = os.path.join(host_directory, "map.json")
            if os.path.exists(map_file):
                with open(map_file) as m:
                    hosts[host] = json.load(m)

            redirect_file = os.path.join(host_directory, "redirect.json")
            if os.path.exists(redirect_file):
                with open(redirect_file) as r:
                    redirects = json.load(r)

                for _from, to in redirects.items():
                    if "*" in host:
                        hosts[_from] = {_from: _from}
                    hosts[_from]["redirect"] = to
        return hosts
