import time

from agent.callbacks import update_callback
from agent.job import get_updated_jobs, update_job


def run():
    while True:
        try:
            jobs = get_updated_jobs()

            for job_dict, job in jobs:
                success = update_callback(job_dict)

                if not success:
                    update_job(job)

        except Exception as e:
            print(e)

        time.sleep(5)  # Poll every 5 seconds for job updates


if __name__ == "__main__":
    run()
