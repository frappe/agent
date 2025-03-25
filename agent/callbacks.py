import requests


def callback(job, connection, result, *args, **kwargs):
    press_url = "https://frappecloud.com"
    requests.post(f"{press_url}/press.api.callbacks.callback", data={"job_id": job.id})
