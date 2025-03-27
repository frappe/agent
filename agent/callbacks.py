import requests


def callback(job, connection, result, *args, **kwargs):
    from agent.server import Server

    press_url = Server().press_url
    requests.post(url=f"{press_url}/api/method/press.api.callbacks.callback", data={"job_id": job.id})
