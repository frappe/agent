from __future__ import annotations

import os
import tempfile

import requests

from agent.server import Server


class Monitor(Server):
    def __init__(self, directory=None):
        super().__init__(directory=directory)
        self.prometheus_directory = "/home/frappe/prometheus"
        self.alertmanager_directory = "/home/frappe/alertmanager"

    def update_rules(self, rules):
        rules_file = os.path.join(self.prometheus_directory, "rules", "agent.yml")
        self._render_template(
            "prometheus/rules.yml",
            {"rules": rules},
            rules_file,
            {
                "variable_start_string": "###",
                "variable_end_string": "###",
            },
        )

        promtool = os.path.join(self.prometheus_directory, "promtool")
        self.execute(f"{promtool} check rules {rules_file}")

        self.execute("sudo systemctl reload prometheus")

    def update_routes(self, routes):
        config_file = os.path.join(self.alertmanager_directory, "alertmanager.yml")
        self._render_template(
            "alertmanager/routes.yml",
            {"routes": routes},
            config_file,
            {
                "variable_start_string": "###",
                "variable_end_string": "###",
            },
        )
        amtool = os.path.join(self.alertmanager_directory, "amtool")
        self.execute(f"{amtool} check-config {config_file}")

        self.execute("sudo systemctl reload alertmanager")

    def discover_targets(self):
        targets = self.fetch_targets()
        for cluster in targets["clusters"]:
            self.generate_prometheus_cluster_config(cluster)

        self.generate_prometheus_tls_config(targets["tls"])
        self.generate_prometheus_sites_config(targets["benches"])
        self.generate_prometheus_domains_config(targets["domains"])

    def fetch_targets(self):
        press_url = self.config.get("press_url")
        press_token = self.config.get("press_token")
        targets = requests.post(
            f"{press_url}/api/method/press.api.monitoring.targets",
            data={"token": press_token},
        ).json()["message"]
        return targets

    def generate_prometheus_sites_config(self, benches):
        prometheus_sites_config = os.path.join(self.prometheus_directory, "file_sd", "sites.yml")
        temp_sites_config = tempfile.mkstemp(prefix="agent-prometheus-sites-", suffix=".yml")[1]
        self._render_template(
            "prometheus/sites.yml",
            {"benches": benches},
            temp_sites_config,
            {"block_start_string": "##", "block_end_string": "##"},
        )
        os.rename(temp_sites_config, prometheus_sites_config)

    def generate_prometheus_tls_config(self, servers):
        prometheus_tls_config = os.path.join(self.prometheus_directory, "file_sd", "tls.yml")
        temp_tls_config = tempfile.mkstemp(prefix="agent-prometheus-tls-", suffix=".yml")[1]
        self._render_template(
            "prometheus/tls.yml",
            {"servers": servers},
            temp_tls_config,
            {"block_start_string": "##", "block_end_string": "##"},
        )
        os.rename(temp_tls_config, prometheus_tls_config)

    def generate_prometheus_domains_config(self, domains):
        prometheus_domains_config = os.path.join(self.prometheus_directory, "file_sd", "domains.yml")
        temp_domains_config = tempfile.mkstemp(prefix="agent-prometheus-domains-", suffix=".yml")[1]
        self._render_template(
            "prometheus/domains.yml",
            {"domains": domains},
            temp_domains_config,
            {"block_start_string": "##", "block_end_string": "##"},
        )
        os.rename(temp_domains_config, prometheus_domains_config)

    def generate_prometheus_cluster_config(self, cluster):
        prometheus_cluster_config = os.path.join(
            self.prometheus_directory,
            "file_sd",
            f"cluster.{cluster['name']}.yml",
        )

        temp_cluster_config = tempfile.mkstemp(prefix="agent-prometheus-cluster-", suffix=".yml")[1]
        self._render_template(
            "prometheus/servers.yml",
            {"cluster": cluster},
            temp_cluster_config,
            {"block_start_string": "##", "block_end_string": "##"},
        )
        os.rename(temp_cluster_config, prometheus_cluster_config)
