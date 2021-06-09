import os
import requests
import tempfile


from agent.server import Server


class Monitor(Server):
    def __init__(self, directory=None):
        super().__init__(directory=directory)
        self.prometheus_directory = "/home/frappe/prometheus"

    def update_rules(self, rules):
        rules_file = os.path.join(
            self.prometheus_directory, "rules", "agent.yml"
        )
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

    def discover_targets(self):
        targets = self.fetch_targets()
        for cluster in targets["clusters"]:
            self.generate_prometheus_cluster_config(cluster)

        self.generate_prometheus_sites_config(targets["benches"])

    def fetch_targets(self):
        press_url = self.config.get("press_url")
        press_token = self.config.get("press_token")
        targets = requests.post(
            f"{press_url}/api/method/press.api.monitoring.targets",
            data={"token": press_token},
        ).json()["message"]
        return targets

    def generate_prometheus_sites_config(self, benches):
        prometheus_sites_config = os.path.join(
            self.prometheus_directory, "file_sd", "sites.yml"
        )
        temp_sites_config = tempfile.mkstemp(
            prefix="agent-prometheus-sites-", suffix=".yml"
        )[1]
        self._render_template(
            "prometheus/sites.yml",
            {"benches": benches},
            temp_sites_config,
            {"block_start_string": "##", "block_end_string": "##"},
        )
        os.rename(temp_sites_config, prometheus_sites_config)

    def generate_prometheus_cluster_config(self, cluster):
        prometheus_cluster_config = os.path.join(
            self.prometheus_directory,
            "file_sd",
            f"cluster.{cluster['name']}.yml",
        )

        temp_cluster_config = tempfile.mkstemp(
            prefix="agent-prometheus-cluster-", suffix=".yml"
        )[1]
        self._render_template(
            "prometheus/servers.yml",
            {"cluster": cluster},
            temp_cluster_config,
            {"block_start_string": "##", "block_end_string": "##"},
        )
        os.rename(temp_cluster_config, prometheus_cluster_config)
