import requests


def callback(job, connection, result, *args, **kwargs):
    # TODO: We might need to address this hard coding!
    # press_url = "https://frappecloud.com"
    press_url = "https://ddee-182-156-21-2.ngrok-free.app"
    requests.post(f"{press_url}/api/method/press.api.callbacks.callback", data={"job_id": job.id})

