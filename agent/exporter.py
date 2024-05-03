from rq import Queue, Worker
from rq.job import JobStatus
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily
from prometheus_client import REGISTRY, Summary


def get_bench_metrics(bench):
    from prometheus_client.exposition import generate_latest

    res = generate_latest(REGISTRY) or "asa"
    return res


def get_workers_stats(port):
    from redis import Redis

    workers = Worker.all(connection=Redis(port=11000))

    return [
        {
            "name": w.name,
            "queues": w.queue_names(),
            "state": w.get_state(),
            "successful_job_count": w.successful_job_count,
            "failed_job_count": w.failed_job_count,
            "total_working_time": w.total_working_time,
        }
        for w in workers
    ]


def get_jobs_by_queue(port):
    from redis import Redis

    return {
        queue.name: {
            JobStatus.QUEUED: queue.count,
            JobStatus.STARTED: queue.started_job_registry.count,
            JobStatus.FINISHED: queue.finished_job_registry.count,
            JobStatus.FAILED: queue.failed_job_registry.count,
            JobStatus.DEFERRED: queue.deferred_job_registry.count,
            JobStatus.SCHEDULED: queue.scheduled_job_registry.count,
        }
        for queue in Queue.all(connection=Redis(port=port))
    }


class RQCollector:
    def collect(self):
        rq_workers = GaugeMetricFamily(
            "rq_workers",
            "RQ workers",
            labels=["bench", "name", "state", "queues"],
        )
        rq_workers_success = CounterMetricFamily(
            "rq_workers_success",
            "RQ workers success count",
            labels=["bench", "name", "queues"],
        )
        rq_workers_failed = CounterMetricFamily(
            "rq_workers_failed",
            "RQ workers fail count",
            labels=["bench", "name", "queues"],
        )
        rq_workers_working_time = CounterMetricFamily(
            "rq_workers_working_time",
            "RQ workers spent seconds",
            labels=["bench", "name", "queues"],
        )

        rq_jobs = GaugeMetricFamily(
            "rq_jobs", "RQ jobs by state", labels=["bench", "queue", "status"]
        )

        for index, port in enumerate(["25025", "11000"] + [11000] * 100):
            workers = get_workers_stats(port)
            for worker in workers:
                label_queues = ",".join(worker["queues"])
                rq_workers.add_metric(
                    [
                        str(index),
                        worker["name"],
                        worker["state"],
                        label_queues,
                    ],
                    1,
                )
                rq_workers_success.add_metric(
                    [str(index), worker["name"], label_queues],
                    worker["successful_job_count"],
                )
                rq_workers_failed.add_metric(
                    [str(index), worker["name"], label_queues],
                    worker["failed_job_count"],
                )
                rq_workers_working_time.add_metric(
                    [str(index), worker["name"], label_queues],
                    worker["total_working_time"],
                )

            yield rq_workers
            yield rq_workers_success
            yield rq_workers_failed
            yield rq_workers_working_time

            for queue_name, jobs in get_jobs_by_queue(port).items():
                for status, count in jobs.items():
                    rq_jobs.add_metric([str(index), queue_name, status], count)

            yield rq_jobs


REGISTRY.register(RQCollector())
