import requests

from agent.utils import get_agent_token


def callback(job, connection, result, *args, **kwargs):
    from agent.server import Server

    server = Server()
    press_url = server.press_url

    path = "/api/method/press.api.callbacks.callback"
    data = {"job_id": job.id}
    token = get_agent_token()

    requests.post(
        url=f"{press_url}{path}",
        data=data,
        headers={"X-Agent-Token": token},
    )
