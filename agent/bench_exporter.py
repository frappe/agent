import subprocess
import json
import os
import datetime
import urllib.request
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector
from prometheus_client import CollectorRegistry
from prometheus_client.exposition import generate_latest
from redis import Redis
from rq import Worker


BENCH_DIR = os.path.expanduser("~/frappe-local")


class BenchCollector(Collector):
    def __init__(self, name: str, port: int, bench_dir: str = BENCH_DIR):
        self.name = name
        self.port = port
        self.bench_dir = bench_dir
        self.conn = Redis(port=port, password=self._get_redis_password())
        self.last_metrics = {}
        super().__init__()

    def _get_redis_password(self):
        try:
            config_path = os.path.join(self.bench_dir, "sites", "common_site_config.json")
            with open(config_path) as f:
                config = json.load(f)
            redis_queue = config.get("redis_queue", "")
            if "@" in redis_queue:
                return redis_queue.split(":")[2].split("@")[0]
        except Exception:
            pass
        return None

    def collect(self):
        bench_up = GaugeMetricFamily(
            "bench_up",
            "Bench container running",
            labels=["bench"],
        )
        bench_web_up = GaugeMetricFamily(
            "bench_web_up",
            "Bench gunicorn process running",
            labels=["bench"],
        )
        bench_workers_up = GaugeMetricFamily(
            "bench_workers_up",
            "Bench RQ workers running",
            labels=["bench"],
        )
        bench_mutation = GaugeMetricFamily(
            "bench_mutation",
            "Bench app has uncommitted changes",
            labels=["bench", "app"],
        )

        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Running}}", self.name],
            capture_output=True,
        )
        container_up = 1 if result.stdout.strip() == b"true" else 0
        bench_up.add_metric([self.name], container_up)
        self.last_metrics["bench_up"] = container_up

        result = subprocess.run(
            ["docker", "exec", self.name, "pgrep", "-f", "gunicorn"],
            capture_output=True,
        )
        web_up = 1 if result.returncode == 0 else 0
        bench_web_up.add_metric([self.name], web_up)
        self.last_metrics["bench_web_up"] = web_up

        workers = Worker.all(connection=self.conn)
        workers_up = 1 if len(workers) > 0 else 0
        bench_workers_up.add_metric([self.name], workers_up)
        self.last_metrics["bench_workers_up"] = workers_up

        mutation = {}
        try:
            result = subprocess.run(
                ["docker", "exec", self.name, "bash", "-c",
                 "for app in /home/frappe/frappe-bench/apps/*/; do "
                 "app_name=$(basename $app); "
                 "status=$(git -C $app status --porcelain 2>/dev/null); "
                 "echo \"$app_name:$status\"; done"],
                capture_output=True,
            )
            for line in result.stdout.decode().strip().split("\n"):
                if ":" in line:
                    app, status = line.split(":", 1)
                    val = 1 if status.strip() else 0
                    bench_mutation.add_metric([self.name, app.strip()], val)
                    mutation[app.strip()] = val
        except Exception:
            pass
        self.last_metrics["bench_mutation"] = mutation

        yield bench_up
        yield bench_web_up
        yield bench_workers_up
        yield bench_mutation


def get_bench_metrics(name: str, port: int, bench_dir: str = BENCH_DIR):
    registry = CollectorRegistry()
    collector = BenchCollector(name, port, bench_dir)
    registry.register(collector)
    output = generate_latest(registry)

    gunicorn_metrics = {}
    try:
        response = urllib.request.urlopen("http://localhost:9102/metrics", timeout=2)
        for line in response.read().decode().splitlines():
            if line.startswith("gunicorn") or line.startswith("# HELP gunicorn") or line.startswith("# TYPE gunicorn"):
                output += (line + "\n").encode()
            if line.startswith("gunicorn") and "{" in line:
                metric_name = line.split("{")[0]
                value = line.split(" ")[-1]
                gunicorn_metrics[metric_name] = gunicorn_metrics.get(metric_name, [])
                gunicorn_metrics[metric_name].append(value)
    except Exception:
        pass

    try:
        record = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "bench": name,
            "metrics": {
                **collector.last_metrics,
                "gunicorn": gunicorn_metrics,
            }
        }
        with open("/var/log/bench_metrics.log", "a") as f:
            f.write(json.dumps(record) + "\n")
    except Exception:
        pass

    return output


if __name__ == "__main__":
    print(get_bench_metrics("test-bench", 11000).decode())
