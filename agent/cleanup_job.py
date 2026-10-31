import datetime

from agent.job import JobModel, StepModel


def cleanup():
    cutoff = datetime.datetime.now() - datetime.timedelta(days=30)  # 1 month old data

    old_jobs = JobModel.select(JobModel.id).where(JobModel.enqueue < cutoff)

    deleted_steps = StepModel.delete().where(StepModel.job.in_(old_jobs)).execute()

    deleted_jobs = JobModel.delete().where(JobModel.id.in_(old_jobs)).execute()

    print(f"Deleted {deleted_jobs} jobs and {deleted_steps} steps")


if __name__ == "__main__":
    cleanup()
