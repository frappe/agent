import json
import logging
from base64 import b64decode

from flask import Flask, jsonify, request
from passlib.hash import pbkdf2_sha256 as pbkdf2
from playhouse.shortcuts import model_to_dict

from agent.job import JobModel
from agent.proxy import Proxy
from agent.server import Server
from agent.monitor import Monitor
from agent.database import DatabaseServer


application = Flask(__name__)


log = logging.getLogger("werkzeug")
log.handlers = []


@application.before_request
def validate_access_token():
    try:
        if application.debug:
            return
        method, access_token = request.headers["Authorization"].split(" ")
        stored_hash = Server().config["access_token"]
        if method.lower() == "bearer" and pbkdf2.verify(
            access_token, stored_hash
        ):
            return
        access_token = b64decode(access_token).decode().split(":")[1]
        if method.lower() == "basic" and pbkdf2.verify(
            access_token, stored_hash
        ):
            return
    except Exception:
        pass

    response = jsonify({"message": "Unauthenticated"})
    response.headers.set("WWW-Authenticate", "Basic")
    return response, 401


@application.route("/authentication", methods=["POST"])
def reset_authentication_token():
    data = request.json
    Server().setup_authentication(data["token"])
    return jsonify({"message": "Success"})


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


@application.route("/ping")
def ping():
    return {"message": "pong"}


@application.route("/server")
def get_server():
    return Server().dump()


@application.route("/server/status", methods=["POST"])
def get_server_status():
    data = request.json
    return Server().status(data["mariadb_root_password"])


@application.route("/server/cleanup", methods=["POST"])
def cleanup_unused_files():
    job = Server().cleanup_unused_files()
    return {"job": job}


@application.route("/benches")
def get_benches():
    return {name: bench.dump() for name, bench in Server().benches.items()}


@application.route("/benches/<string:bench>")
def get_bench(bench):
    return Server().benches[bench].dump()


@application.route("/benches/<string:bench>/info", methods=["POST", "GET"])
def fetch_sites_info(bench):
    data = request.json
    since = data.get("since") if data else None
    return Server().benches[bench].fetch_sites_info(since=since)


@application.route("/benches/<string:bench>/sites")
def get_sites(bench):
    sites = Server().benches[bench].sites
    return {name: site.dump() for name, site in sites.items()}


@application.route("/benches/<string:bench>/apps")
def get_apps(bench):
    apps = Server().benches[bench].apps
    return {name: site.dump() for name, site in apps.items()}


@application.route("/benches/<string:bench>/config")
def get_config(bench):
    return Server().benches[bench].config


@application.route("/benches/<string:bench>/status", methods=["GET"])
def get_bench_status(bench):
    return Server().benches[bench].status()


@application.route("/benches/<string:bench>/sites/<string:site>")
def get_site(bench, site):
    return Server().benches[bench].sites[site].dump()


@application.route("/benches/<string:bench>/sites/<string:site>/logs")
def get_logs(bench, site):
    return jsonify(Server().benches[bench].sites[site].logs)


@application.route(
    "/benches/<string:bench>/sites/<string:site>/logs/<string:log>"
)
def get_log(bench, site, log):
    return {log: Server().benches[bench].sites[site].retrieve_log(log)}


@application.route("/benches/<string:bench>/sites/<string:site>/sid")
def get_site_sid(bench, site):
    return {"sid": Server().benches[bench].sites[site].sid()}


@application.route("/benches", methods=["POST"])
def new_bench():
    data = request.json
    job = Server().new_bench(**data)
    return {"job": job}


@application.route("/benches/<string:bench>/archive", methods=["POST"])
def archive_bench(bench):
    job = Server().archive_bench(bench)
    return {"job": job}


"""
POST /benches/bench-1/sites
{
    "name": "test.frappe.cloud",
    "mariadb_root_password": "root",
    "admin_password": "admin",
    "apps": ["frappe", "press"],
    "config": {
        "monitor": 1,
    }
}

"""


@application.route("/benches/<string:bench>/sites", methods=["POST"])
def new_site(bench):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .new_site(
            data["name"],
            data["config"],
            data["apps"],
            data["mariadb_root_password"],
            data["admin_password"],
        )
    )
    return {"job": job}


@application.route("/benches/<string:bench>/sites/restore", methods=["POST"])
def new_site_from_backup(bench):
    data = request.json

    job = (
        Server()
        .benches[bench]
        .new_site_from_backup(
            data["name"],
            data["config"],
            data["apps"],
            data["mariadb_root_password"],
            data["admin_password"],
            data["site_config"],
            data["database"],
            data["public"],
            data["private"],
        )
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/restore", methods=["POST"]
)
def restore_site(bench, site):
    data = request.json

    job = (
        Server()
        .benches[bench]
        .sites[site]
        .restore_job(
            data["apps"],
            data["mariadb_root_password"],
            data["admin_password"],
            data["database"],
            data["public"],
            data["private"],
        )
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/reinstall", methods=["POST"]
)
def reinstall_site(bench, site):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .sites[site]
        .reinstall_job(data["mariadb_root_password"], data["admin_password"])
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/rename", methods=["POST"]
)
def rename_site(bench, site):
    data = request.json
    job = Server().benches[bench].sites[site].rename_job(data["new_name"])
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/apps", methods=["POST"]
)
def install_app_site(bench, site):
    data = request.json
    job = Server().benches[bench].sites[site].install_app_job(data["name"])
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/apps/<string:app>",
    methods=["DELETE"],
)
def uninstall_app_site(bench, site, app):
    job = Server().benches[bench].sites[site].uninstall_app_job(app)
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/erpnext", methods=["POST"]
)
def setup_erpnext(bench, site):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .sites[site]
        .setup_erpnext(data["user"], data["config"])
    )
    return {"job": job}


@application.route("/benches/<string:bench>/monitor", methods=["POST"])
def fetch_monitor_data(bench):
    return {"data": Server().benches[bench].fetch_monitor_data()}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/status", methods=["GET"]
)
def fetch_site_status(bench, site):
    return {"data": Server().benches[bench].sites[site].fetch_site_status()}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/info", methods=["GET"]
)
def fetch_site_info(bench, site):
    return {"data": Server().benches[bench].sites[site].fetch_site_info()}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/backup", methods=["POST"]
)
def backup_site(bench, site):
    data = request.json or {}
    with_files = data.get("with_files")
    offsite = data.get("offsite")
    job = Server().benches[bench].sites[site].backup_job(with_files, offsite)
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/migrate",
    methods=["POST"],
)
def migrate_site(bench, site):
    job = Server().benches[bench].sites[site].migrate_job()
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/cache",
    methods=["DELETE"],
)
def clear_site_cache(bench, site):
    job = Server().benches[bench].sites[site].clear_cache_job()
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/update/migrate",
    methods=["POST"],
)
def update_site_migrate(bench, site):
    data = request.json
    job = Server().update_site_migrate_job(
        site, bench, data["target"], data.get("activate", True)
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/update/pull", methods=["POST"]
)
def update_site_pull(bench, site):
    data = request.json
    job = Server().update_site_pull_job(
        site, bench, data["target"], data.get("activate", True)
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/update/migrate/recover",
    methods=["POST"],
)
def update_site_recover_migrate(bench, site):
    data = request.json
    job = Server().update_site_recover_migrate_job(
        site, bench, data["target"], data.get("activate", True)
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/update/migrate/restore",
    methods=["POST"],
)
def restore_site_tables(bench, site):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .sites[site]
        .restore_site_tables_job(data.get("activate", True))
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/update/pull/recover",
    methods=["POST"],
)
def update_site_recover_pull(bench, site):
    data = request.json
    job = Server().update_site_recover_pull_job(
        site, bench, data["target"], data.get("activate", True)
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/update/recover",
    methods=["POST"],
)
def update_site_recover(bench, site):
    job = Server().update_site_recover_job(site, bench)
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/archive", methods=["POST"]
)
def archive_site(bench, site):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .archive_site(site, data["mariadb_root_password"])
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/config", methods=["POST"]
)
def site_update_config(bench, site):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .sites[site]
        .update_config_job(data["config"], data["remove"])
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/domains", methods=["POST"]
)
def site_add_domain(bench, site):
    data = request.json
    job = Server().benches[bench].sites[site].add_domain(data["domain"])
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/domains/<string:domain>",
    methods=["DELETE"],
)
def site_remove_domain(bench, site, domain):
    job = Server().benches[bench].sites[site].remove_domain(domain)
    return {"job": job}


@application.route("/benches/<string:bench>/config", methods=["POST"])
def bench_set_config(bench):
    data = request.json
    job = Server().benches[bench].update_config_job(**data)
    return {"job": job}


@application.route("/proxy/hosts", methods=["POST"])
def proxy_add_host():
    data = request.json
    job = Proxy().add_host_job(
        data["name"], data["target"], data["certificate"]
    )
    return {"job": job}


@application.route("/proxy/wildcards", methods=["POST"])
def proxy_add_wildcard_hosts():
    data = request.json
    job = Proxy().add_wildcard_hosts_job(data)
    return {"job": job}


@application.route("/proxy/hosts/redirects", methods=["POST"])
def proxy_setup_redirects():
    data = request.json
    job = Proxy().setup_redirects_job(data["domains"], data["target"])
    return {"job": job}


@application.route("/proxy/hosts/redirects", methods=["DELETE"])
def proxy_remove_redirects():
    data = request.json
    job = Proxy().remove_redirects_job(data["domains"])
    return {"job": job}


@application.route("/proxy/hosts/<string:host>", methods=["DELETE"])
def proxy_remove_host(host):
    job = Proxy().remove_host_job(host)
    return {"job": job}


@application.route("/proxy/upstreams", methods=["POST"])
def proxy_add_upstream():
    data = request.json
    job = Proxy().add_upstream_job(data["name"])
    return {"job": job}


@application.route(
    "/proxy/upstreams/<string:upstream>/rename", methods=["POST"]
)
def proxy_rename_upstream(upstream):
    data = request.json
    job = Proxy().rename_upstream_job(upstream, data["name"])
    return {"job": job}


@application.route(
    "/proxy/upstreams/<string:upstream>/sites", methods=["POST"]
)
def proxy_add_upstream_site(upstream):
    data = request.json
    job = Proxy().add_site_to_upstream_job(upstream, data["name"])
    return {"job": job}


@application.route(
    "/proxy/upstreams/<string:upstream>/sites/<string:site>",
    methods=["DELETE"],
)
def proxy_remove_upstream_site(upstream, site):
    job = Proxy().remove_site_from_upstream_job(upstream, site)
    return {"job": job}


@application.route(
    "/proxy/upstreams/<string:upstream>/sites/<string:site>/rename",
    methods=["POST"],
)
def proxy_rename_upstream_site(upstream, site):
    data = request.json
    job = Proxy().rename_site_on_upstream_job(
        upstream,
        data["domains"],
        site,
        data["new_name"],
    )
    return {"job": job}


@application.route(
    "/proxy/upstreams/<string:upstream>/sites/<string:site>/status",
    methods=["POST"],
)
def update_site_status(upstream, site):
    data = request.json
    job = Proxy().update_site_status_job(upstream, site, data["status"])
    return {"job": job}


@application.route("/monitor/rules", methods=["POST"])
def update_monitor_rules():
    data = request.json
    Monitor().update_rules(data["rules"])
    Monitor().update_routes(data["routes"])
    return {}


@application.route("/database/binary/logs")
def get_binary_logs():
    return jsonify(DatabaseServer().binary_logs)


@application.route("/database/binary/logs/<string:log>", methods=["POST"])
def get_binary_log(log):
    data = request.json
    return jsonify(
        DatabaseServer().search_binary_log(
            log,
            data["database"],
            data["start_datetime"],
            data["stop_datetime"],
            data["search_pattern"],
            data["max_lines"],
        )
    )


def to_dict(model):
    if isinstance(model, JobModel):
        job = model_to_dict(model, backrefs=True)
        job["data"] = json.loads(job["data"]) or {}
        for step in job["steps"]:
            step["data"] = json.loads(step["data"]) or {}
    else:
        job = list(map(model_to_dict, model))
    return job


@application.route("/jobs")
@application.route("/jobs/<int:id>")
@application.route("/jobs/<string:ids>")
@application.route("/jobs/status/<string:status>")
def jobs(id=None, ids=None, status=None):
    choices = [x[1] for x in JobModel._meta.fields["status"].choices]
    if id:
        job = to_dict(JobModel.get(JobModel.id == id))
    elif ids:
        ids = ids.split(",")
        job = list(map(to_dict, JobModel.select().where(JobModel.id << ids)))
    elif status in choices:
        job = to_dict(
            JobModel.select(JobModel.id, JobModel.name).where(
                JobModel.status == status
            )
        )
    return jsonify(json.loads(json.dumps(job, default=str)))


@application.route("/update", methods=["POST"])
def update_agent():
    data = request.json
    Server().update_agent_web(data.get("url"))
    return {"message": "Success"}
