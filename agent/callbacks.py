import json

import requests


def update_callback(job):
    from agent.server import Server

    server = Server()
    token = server.config.get("agent_token")

    if not token:
        print("Agent token not configured")
        return False

    press_url = server.press_url

    job_string = json.dumps(job, default=str)

    path = "/api/method/press.api.callbacks.update_job"

    data = {
        "job": job_string,
        "server": server.name,
    }

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
