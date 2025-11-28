from __future__ import annotations

import json
import os
import shutil
from collections import defaultdict
from contextlib import contextmanager
from functools import wraps
from hashlib import sha512 as sha
from pathlib import Path

import filelock

from agent.job import job, step
from agent.server import Server


def with_proxy_config_lock():
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            with self.proxy_config_modification_lock:
                return func(self, *args, **kwargs)

        return wrapper

    return decorator


class Proxy(Server):
    def __init__(self, directory=None):
        super().__init__(directory)
        self.directory = directory or os.getcwd()
        self.config_file = os.path.join(self.directory, "config.json")
        self.name = self.config["name"]
        self.domain = self.config.get("domain")
        self.nginx_directory = self.config["nginx_directory"]
        self.secondary_config_path = os.path.join(self.nginx_directory, "secondaries.json")
        self.upstreams_directory = os.path.join(self.nginx_directory, "upstreams")
        self.hosts_directory = os.path.join(self.nginx_directory, "hosts")
        self.error_pages_directory = os.path.join(self.directory, "repo", "agent", "pages")
        self._proxy_config_modification_lock = None
        self.job = None
        self.step = None

    def setup_proxy(self):
        self._create_default_host()
        self._generate_proxy_config()
        self.execute("sudo systemctl reload nginx")

    @job("Add Host to Proxy")
    def add_host_job(self, host, target, certificate):
        self.add_host(host, target, certificate)
        self.reload_nginx()

    @step("Add Host to Proxy")
    @with_proxy_config_lock()
    def add_host(self, host, target, certificate):
        host_directory = os.path.join(self.hosts_directory, host)
        os.makedirs(host_directory, exist_ok=True)

        map_file = os.path.join(host_directory, "map.json")
        with open(map_file, "w") as m:
            json.dump({host: target}, m, indent=4)

        for key, value in certificate.items():
            with open(os.path.join(host_directory, key), "w") as f:
                f.write(value)

    @job("Add Wildcard Hosts to Proxy")
    def add_wildcard_hosts_job(self, wildcards):
        self.add_wildcard_hosts(wildcards)
        self.reload_nginx()

    @step("Add Wildcard Hosts to Proxy")
    @with_proxy_config_lock()
    def add_wildcard_hosts(self, wildcards):
        for wildcard in wildcards:
            host = f"*.{wildcard['domain']}"
            host_directory = os.path.join(self.hosts_directory, host)
            os.makedirs(host_directory, exist_ok=True)

            map_file = os.path.join(host_directory, "map.json")
            with open(map_file, "w") as m:
                json.dump({host: "$host"}, m, indent=4)

            for key, value in wildcard["certificate"].items():
                with open(os.path.join(host_directory, key), "w") as f:
                    f.write(value)
            if wildcard.get("code_server"):
                Path(os.path.join(host_directory, "codeserver")).touch()

    def add_site_domain_to_upstream(self, upstream, site):
        """Add site domain(s) to upstream configuration"""
        sites = site if isinstance(site, list) else [site]

        with self.proxy_config_modification_lock:
            for s in sites:
                self.remove_conflicting_site(s)
                self.add_site_to_upstream(upstream, s)

        self.reload_nginx()

    @job("Add Auto Scale Site to Upstream")
    def add_auto_scale_sites_to_upstream(self, primary_upstream: str, secondary_upstreams: list[str]):
        """Add secondary server to nginx upstream"""
        self._add_auto_scale_sites_to_upstream(primary_upstream, secondary_upstreams)
        self.reload_nginx()

    @step("Add Auto Scale Site to Upstream")
    def _add_auto_scale_sites_to_upstream(self, primary_upstream: str, secondary_upstreams: list[str]):
        """Add secondary server to nginx upstream"""
        self.set_secondaries_for_upstream(primary_upstream, secondary_upstreams)

    @job("Add Site to Upstream")
    def add_site_to_upstream_job(self, upstream, site):
        self.add_site_domain_to_upstream(upstream, site)

    @job("Add Domain to Upstream")
    def add_domain_to_upstream_job(self, upstream, domain):
        self.add_site_domain_to_upstream(upstream, domain)

    @step("Remove Conflicting Site")
    @with_proxy_config_lock()
    def remove_conflicting_site(self, site):
        # Go through all upstreams and remove the site file matching the site name
        for upstream in self.upstreams:
            conflict = os.path.join(self.upstreams_directory, upstream, site)
            if os.path.exists(conflict):
                os.remove(conflict)

    @step("Add Site File to Upstream Directory")
    @with_proxy_config_lock()
    def add_site_to_upstream(self, upstream, site):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        os.makedirs(upstream_directory, exist_ok=True)
        site_file = os.path.join(upstream_directory, site)
        Path(site_file).touch()

    @job("Add Upstream to Proxy")
    def add_upstream_job(self, upstream):
        self.add_upstream(upstream)
        self.reload_nginx()

    @step("Add Upstream Directory")
    @with_proxy_config_lock()
    def add_upstream(self, upstream):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        os.makedirs(upstream_directory, exist_ok=True)

    @job("Rename Upstream")
    def rename_upstream_job(self, old, new):
        self.rename_upstream(old, new)
        self.reload_nginx()

    @step("Rename Upstream Directory")
    @with_proxy_config_lock()
    def rename_upstream(self, old, new):
        old_upstream_directory = os.path.join(self.upstreams_directory, old)
        new_upstream_directory = os.path.join(self.upstreams_directory, new)
        shutil.move(old_upstream_directory, new_upstream_directory)

    @job("Remove Host from Proxy")
    def remove_host_job(self, host):
        self.remove_host(host)

    @step("Remove Host from Proxy")
    @with_proxy_config_lock()
    def remove_host(self, host):
        host_directory = os.path.join(self.hosts_directory, host)
        if os.path.exists(host_directory):
            shutil.rmtree(host_directory)

    @job("Remove Site from Upstream")
    def remove_site_from_upstream_job(self, upstream, site, extra_domains=None):
        if not extra_domains:
            extra_domains = []

        with self.proxy_config_modification_lock:
            upstream_directory = os.path.join(self.upstreams_directory, upstream)

            site_file = os.path.join(upstream_directory, site)
            if os.path.exists(site_file):
                self.remove_site_from_upstream(site_file)

            for domain in extra_domains:
                site_file = os.path.join(upstream_directory, domain)
                if os.path.exists(site_file):
                    self.remove_site_from_upstream(site_file)

    @step("Remove Site File from Upstream Directory")
    @with_proxy_config_lock()
    def remove_site_from_upstream(self, site_file):
        os.remove(site_file)

    @job("Rename Site on Upstream")
    def rename_site_on_upstream_job(
        self,
        upstream: str,
        hosts: list[str],
        site: str,
        new_name: str,
    ):
        with self.proxy_config_modification_lock:
            self.remove_conflicting_site(new_name)
            self.rename_site_on_upstream(upstream, site, new_name)
            site_host_dir = os.path.join(self.hosts_directory, site)
            if os.path.exists(site_host_dir):
                self.rename_host_dir(site, new_name)
                self.rename_site_in_host_dir(new_name, site, new_name)
            for host in hosts:
                self.rename_site_in_host_dir(host, site, new_name)
        self.reload_nginx()

    def replace_str_in_json(self, file: str, old: str, new: str):
        """Replace quoted strings in json file."""
        with open(file) as f:
            text = f.read()
        text = text.replace('"' + old + '"', '"' + new + '"')
        with open(file, "w") as f:
            f.write(text)

    @step("Rename Host Directory")
    @with_proxy_config_lock()
    def rename_host_dir(self, old_name: str, new_name: str):
        """Rename site's host directory."""
        old_host_dir = os.path.join(self.hosts_directory, old_name)
        new_host_dir = os.path.join(self.hosts_directory, new_name)
        os.rename(old_host_dir, new_host_dir)

    @step("Rename Site in Host Directory")
    @with_proxy_config_lock()
    def rename_site_in_host_dir(self, host: str, old_name: str, new_name: str):
        host_directory = os.path.join(self.hosts_directory, host)

        map_file = os.path.join(host_directory, "map.json")
        if os.path.exists(map_file):
            self.replace_str_in_json(map_file, old_name, new_name)

        redirect_file = os.path.join(host_directory, "redirect.json")
        if os.path.exists(redirect_file):
            self.replace_str_in_json(redirect_file, old_name, new_name)

    @step("Rename Site File in Upstream Directory")
    @with_proxy_config_lock()
    def rename_site_on_upstream(self, upstream: str, site: str, new_name: str):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        old_site_file = os.path.join(upstream_directory, site)
        new_site_file = os.path.join(upstream_directory, new_name)
        if not os.path.exists(old_site_file) and os.path.exists(new_site_file):
            return
        os.rename(old_site_file, new_site_file)

    @job("Update Site Status")
    def update_site_status_job(self, upstream, site, status, extra_domains=None, skip_reload=False):
        self.update_site_status(upstream, site, status)
        if not extra_domains:
            extra_domains = []
        for domain in extra_domains:
            self.update_site_status(upstream, domain, status)
        if not skip_reload:
            self.reload_nginx()

    @step("Update Site File")
    @with_proxy_config_lock()
    def update_site_status(self, upstream, site, status):
        upstream_directory = os.path.join(self.upstreams_directory, upstream)
        site_file = os.path.join(upstream_directory, site)
        with open(site_file, "w") as f:
            f.write(status)

    @job("Setup Redirects on Hosts")
    def setup_redirects_job(self, hosts, target):
        with self.proxy_config_modification_lock:
            if target in hosts:
                hosts.remove(target)
                self.remove_redirect(target)
            for host in hosts:
                self.setup_redirect(host, target)
        self.reload_nginx()

    @step("Setup Redirect on Host")
    @with_proxy_config_lock()
    def setup_redirect(self, host, target):
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

    @job("Remove Redirects on Hosts")
    def remove_redirects_job(self, hosts):
        with self.proxy_config_modification_lock:
            for host in hosts:
                self.remove_redirect(host)

    @step("Remove Redirect on Host")
    @with_proxy_config_lock()
    def remove_redirect(self, host: str):
        host_directory = os.path.join(self.hosts_directory, host)
        redirect_file = os.path.join(host_directory, "redirect.json")
        if os.path.exists(redirect_file):
            os.remove(redirect_file)
        if host.endswith("." + self.domain):
            # default domain
            os.rmdir(host_directory)

    @step("Reload NGINX")
    def reload_nginx(self):
        return self._reload_nginx()

    @job("Reload NGINX Job")
    def reload_nginx_job(self):
        return self.reload_nginx()

    def _generate_proxy_config(self):
        proxy_config_file = os.path.join(self.nginx_directory, "proxy.conf")
        config = self.get_config()
        with self.proxy_config_modification_lock:
            data = {
                "hosts": self.hosts,
                "upstreams": self.upstreams,
                "domain": config["domain"],
                "wildcards": sorted(self.wildcards, key=lambda x: len(x)),
                "nginx_directory": config["nginx_directory"],
                "error_pages_directory": self.error_pages_directory,
                "tls_protocols": config.get("tls_protocols"),
            }

        self._render_template(
            "proxy/nginx.conf.jinja2",
            data,
            proxy_config_file,
        )

    def _reload_nginx(self):
        from agent.nginx_reload_manager import NginxReloadManager

        if not self.job:
            raise Exception("NGINX Reload should be trigerred by a job")

        return NginxReloadManager().request_reload(request_id=self.job_record.model.agent_job_id)

    def _create_default_host(self):
        default_host = f"*.{self.config['domain']}"
        default_host_directory = os.path.join(self.hosts_directory, default_host)
        os.makedirs(default_host_directory, exist_ok=True)
        map_file = os.path.join(default_host_directory, "map.json")
        with open(map_file, "w") as mf:
            json.dump({"default": "$host"}, mf, indent=4)

        tls_directory = self.config["tls_directory"]
        for f in ["chain.pem", "fullchain.pem", "privkey.pem"]:
            source = os.path.join(tls_directory, f)
            destination = os.path.join(default_host_directory, f)
            if os.path.exists(destination):
                os.remove(destination)
            os.symlink(source, destination)

    @property
    def secondaries(self) -> dict[str, list[str]]:
        """Fetch all the secondaries in this proxy"""
        if not os.path.exists(self.secondary_config_path):
            return {}

        with open(self.secondary_config_path, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}

    def set_secondaries_for_upstream(self, primary_upstream: str, secondary_upstreams: list[str]):
        """
        Updates secondaries config file with new sites for the same upstream.
        """
        os.makedirs(os.path.dirname(self.secondary_config_path), exist_ok=True)

        lock = filelock.SoftFileLock(self.secondary_config_path + ".lock")

        with lock.acquire(timeout=10), open(self.secondary_config_path, "a+") as f:
            f.seek(0)

            try:
                secondaries_config = json.load(f)
            except json.JSONDecodeError:
                secondaries_config = {}

            secondaries_config[primary_upstream] = secondary_upstreams

            # rewriting the whole file again
            f.seek(0)
            f.truncate()
            json.dump(secondaries_config, f, indent=4, sort_keys=True)

    @property
    def upstreams(self):
        upstreams = {}
        for upstream in os.listdir(self.upstreams_directory):  # for each server ip
            upstream_directory = os.path.join(self.upstreams_directory, upstream)
            if not os.path.isdir(upstream_directory):
                continue
            hashed_upstream = sha(upstream.encode()).hexdigest()[:16]
            upstreams[upstream] = {"sites": [], "secondaries": [], "hash": hashed_upstream}
            for site in os.listdir(upstream_directory):
                with open(os.path.join(upstream_directory, site)) as f:
                    status = f.read().strip()
                if status in (
                    "deactivated",
                    "suspended",
                    "suspended_saas",
                ):
                    actual_upstream = status
                else:
                    actual_upstream = hashed_upstream

                upstreams[upstream]["secondaries"].extend(self.secondaries.get("upstream", []))
                upstreams[upstream]["sites"].append({"name": site, "upstream": actual_upstream})

        return upstreams

    @property
    def hosts(self) -> dict[str, dict[str, str]]:
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
            hosts[host]["codeserver"] = os.path.exists(os.path.join(host_directory, "codeserver"))

        return hosts

    @property
    def wildcards(self) -> list[str]:
        wildcards = []
        for host in os.listdir(self.hosts_directory):
            if "*" in host:
                wildcards.append(host.strip("*."))
        return wildcards

    @property
    @contextmanager
    def proxy_config_modification_lock(self):
        if self._proxy_config_modification_lock is None:
            lock_path = os.path.join(self.nginx_directory, "proxy_config.lock")
            self._proxy_config_modification_lock = filelock.FileLock(
                lock_path,
            )

        with self._proxy_config_modification_lock:
            yield
