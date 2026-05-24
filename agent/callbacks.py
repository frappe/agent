import json

import requests


def callback(job, connection, result, *args, **kwargs):
    from agent.server import Server

    server = Server()
    press_url = server.press_url

    path = "/api/method/press.api.callbacks.callback"
    data = {"job_id": job.id}
    token = server.config["agent_token"]

    requests.post(
        url=f"{press_url}{path}",
        data=data,
        headers={"X-Agent-Token": token},
    )


def update_callback(job):
    from agent.server import Server

    server = Server()
    press_url = server.press_url

    job_string = json.dumps(job, default=str)

    path = "/api/method/press.api.callbacks.update_job"

    data = {
        "job": job_string,
        "server": server.name,
    }

    token = server.config["agent_token"]

    try:
        response = requests.post(
            url=f"{press_url}{path}",
            data=data,
            headers={"X-Agent-Token": token},
            timeout=10,
        )

        return response.ok

    except requests.RequestException:
        return False
