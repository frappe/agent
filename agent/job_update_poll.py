import time

from agent.callbacks import update_callback
from agent.job import get_updated_jobs


def run():
    while True:
        try:
            jobs = get_updated_jobs()

            for job in jobs:
                update_callback(job)

        except Exception as e:
            print(e)

        time.sleep(5)  # Poll every 5 seconds for job updates


if __name__ == "__main__":
    run()
