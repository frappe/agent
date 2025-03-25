import requests


def callback(job, connection, result, *args, **kwargs):
    # TODO: We might need to address this hard coding!
    press_url = "https://frappecloud.com"
    requests.post(f"{press_url}/press.api.callbacks.callback", data={"job_id": job.id})
