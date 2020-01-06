import json
from flask import Flask, jsonify, request
from playhouse.shortcuts import model_to_dict

from agent.job import JobModel
from agent.server import Server

application = Flask(__name__)


"""
POST /benches
{
    "name": "bench-1",
    "python": "/usr/bin/python3.6",
    "apps": [
        {
            "name": "frappe",
            "repo": "https://github.com/frappe/frappe",
            "branch": "version-12",
            "hash": "ada803c5b57e489bfbc2dee6292a4bcb3ff69aa0"
        },
        {
            "name": "erpnext",
            "repo": "https://github.com/frappe/erpnext",
            "branch": "version-12",
            "hash": "782f45ae5f272173b5daadb493d40cf7ccf131b4"
        }
    ],
    "config": {
        "background_workers": 8,
        "error_report_email": "test@example.com",
        "frappe_user": "frappe",
        "gunicorn_workers": 16,
        "mail_login": "test@example.com",
        "mail_password": "test",
        "mail_server": "smtp.example.com",
        "monitor": 1,
        "redis_cache": "redis://localhost:13000",
        "redis_queue": "redis://localhost:11000",
        "redis_socketio": "redis://localhost:12000",
        "server_script_enabled": true,
        "socketio_port": 9000,
        "webserver_port": 8000
    }
}

"""


@application.route("/benches", methods=["POST"])
def new_bench():
    data = request.json
    job = Server().new_bench(
        data["name"], data["python"], data["config"], data["apps"]
    )
    return {"job": job}


app = Flask(__name__)
app.debug = True


def to_dict(model):
    if isinstance(model, JobModel):
        job = model_to_dict(model, backrefs=True)
        job["data"] = json.loads(job["data"]) or {}
        if "output" in job["data"]:
            job["data"]["output"] = job["data"]["output"].split("\n")
        if "traceback" in job["data"]:
            job["data"]["traceback"] = job["data"]["traceback"].split("\n")
        for step in job["steps"]:
            step["data"] = json.loads(step["data"]) or {}
            if "output" in step["data"]:
                step["data"]["output"] = step["data"]["output"].split("\n")
            if "traceback" in step["data"]:
                step["data"]["traceback"] = step["data"]["traceback"].split(
                    "\n"
                )
    else:
        job = list(map(model_to_dict, model))
    return job


@application.route("/jobs")
@application.route("/jobs/<int:id>")
@application.route("/jobs/status/<string:status>")
def jobs(id=None, status=None):
    choices = [x[1] for x in JobModel._meta.fields["status"].choices]
    if id:
        job = to_dict(JobModel.get(JobModel.id == id))
    elif status in choices:
        job = to_dict(
            JobModel.select(JobModel.id, JobModel.name).where(
                JobModel.status == status
            )
        )
    return jsonify(json.loads(json.dumps(job, default=str)))
