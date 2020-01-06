import json
from flask import Flask, jsonify, request
from playhouse.shortcuts import model_to_dict

from agent.job import JobModel

application = Flask(__name__)



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
