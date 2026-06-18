import base64
import json
import time
from datetime import datetime, timezone

import requests

from agent.callbacks import update_callback
from agent.job import connection, get_updated_jobs


def verify_token_expiry(token):
    """
    Returns True if token expires in less than 7 days.
    Returns False otherwise.
    """

    try:
        parts = token.split(".")

        if len(parts) != 3:
            return True

        payload_b64 = parts[1]

        # Add required padding
        payload_b64 += "=" * (-len(payload_b64) % 4)

        payload_json = base64.urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_json)

        exp = payload.get("exp")

        if not exp:
            return True

        expiry_time = datetime.fromtimestamp(exp, tz=timezone.utc)
        now = datetime.now(timezone.utc)

        remaining = expiry_time - now

        return remaining.total_seconds() < (7 * 24 * 60 * 60)

    except Exception:
        return True


def get_regenerate_token():
    from agent.server import Server

    server = Server()
    press_url = server.press_url

    path = "/api/method/press.api.agent_auth.regenerate_token"

    token = server.config.get("agent_token")

    if not token:
        return False

    try:
        response = requests.post(
            f"{press_url}{path}",
            headers={"X-Agent-Token": token},
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()

        return data.get("message")

    except (requests.RequestException, ValueError):
        return False


def retry_undelivered():
    from agent.server import Server

    server = Server()
    press_url = server.press_url

    path = "/api/method/press.api.callbacks.retry_undelivered"

    token = server.config.get("agent_token")
    if not token:
        return False

    try:
        response = requests.get(url=f"{press_url}{path}", headers={"X-Agent-Token": token}, timeout=10)

        return response.ok
    except requests.RequestException:
        return False


def handle_retry(counter: int) -> int:
    """Retry undelivered jobs every 10 seconds."""
    if counter >= 2:
        retry_undelivered()
        return 0

    return counter


def handle_token_refresh(server, counter: int) -> int:
    """Check and refresh token every 5 minutes."""
    if counter >= 60:
        token = server.config.get("agent_token")
        if not token:
            return 0

        if verify_token_expiry(token):
            new_token = get_regenerate_token()

            if new_token:
                from agent.server import Server

                server = Server()

                server.update_config(
                    {
                        "agent_token": new_token,
                    }
                )

        return 0

    return counter


def recover_processing_jobs():
    redis = connection()

    redis.sunionstore(
        "dirty_jobs",
        "dirty_jobs",
        "processing_jobs",
    )

    redis.delete("processing_jobs")


def process_jobs():
    redis = connection()

    jobs = get_updated_jobs()

    for job_dict, job in jobs:
        success = update_callback(job_dict)

        if success:
            redis.srem("processing_jobs", job.id)
        else:
            redis.smove("processing_jobs", "dirty_jobs", job.id)


def run():
    from agent.server import Server

    recover_processing_jobs()

    retry_counter = 0
    token_check_counter = 0

    while True:
        server = Server()

        if not server.config.get("enable_feature_worker", False):
            time.sleep(5)
            continue

        try:
            retry_counter = handle_retry(retry_counter)

            token_check_counter = handle_token_refresh(
                server,
                token_check_counter,
            )

            process_jobs()

        except Exception as e:
            print(e)

        retry_counter += 1
        token_check_counter += 1

        time.sleep(5)


if __name__ == "__main__":
    run()
