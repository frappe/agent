import json
import logging
import os
import sys
import traceback
from base64 import b64decode
from functools import wraps
from typing import TYPE_CHECKING

from flask import Flask, jsonify, request
from passlib.hash import pbkdf2_sha256 as pbkdf2
from playhouse.shortcuts import model_to_dict

from agent.builder import ImageBuilder, get_image_build_context_directory
from agent.database import DatabaseServer
from agent.exceptions import BenchNotExistsException, SiteNotExistsException
from agent.job import JobModel, connection
from agent.minio import Minio
from agent.monitor import Monitor
from agent.proxy import Proxy
from agent.proxysql import ProxySQL
from agent.security import Security
from agent.server import Server
from agent.ssh import SSHProxy

if TYPE_CHECKING:
    from datetime import datetime, timedelta
    from typing import Optional, TypedDict

    ExecuteReturn = TypedDict(
        "ExecuteReturn",
        {
            "command": str,
            "status": str,
            "start": datetime,
            "end": datetime,
            "duration": timedelta,
            "output": str,
            "directory": Optional[str],
            "traceback": Optional[str],
            "returncode": Optional[int],
        },
    )

application = Flask(__name__)


def validate_bench(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        bench = kwargs.get("bench")

        if bench:
            Server().get_bench(bench)

        return fn(*args, **kwargs)

    return wrapper


def validate_bench_and_site(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        site = kwargs.get("site")
        bench = kwargs.get("bench")

        if bench:
            bench_obj = Server().get_bench(bench)
            bench_obj.get_site(site)

        return fn(*args, **kwargs)

    return wrapper


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


@application.route("/builder/upload/<string:dc_name>", methods=["POST"])
def upload_build_context_for_image_builder(dc_name: str):
    filename = f"{dc_name}.tar.gz"
    filepath = os.path.join(get_image_build_context_directory(), filename)
    if os.path.exists(filepath):
        os.unlink(filepath)

    build_context_file = request.files["build_context_file"]
    build_context_file.save(filepath)
    return {"filename": filename}


@application.route("/builder/build", methods=["POST"])
def build_image():
    data = request.json
    image_builder = ImageBuilder(
        filename=data.get("filename"),
        image_repository=data.get("image_repository"),
        image_tag=data.get("image_tag"),
        no_cache=data.get("no_cache"),
        no_push=data.get("no_push"),
        registry=data.get("registry"),
    )
    job = image_builder.run_remote_builder()
    return {"job": job}


@application.route("/server")
def get_server():
    return Server().dump()


@application.route("/server/reload", methods=["POST"])
def restart_nginx():
    job = Server().restart_nginx()
    return {"job": job}


@application.route("/proxy/reload", methods=["POST"])
def reload_nginx():
    job = Proxy().reload_nginx_job()
    return {"job": job}


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
@validate_bench
def get_bench(bench):
    return Server().benches[bench].dump()


@application.route("/benches/<string:bench>/info", methods=["POST", "GET"])
@validate_bench
def fetch_sites_info(bench):
    data = request.json
    since = data.get("since") if data else None
    return Server().benches[bench].fetch_sites_info(since=since)


@application.route("/benches/<string:bench>/analytics", methods=["GET"])
@validate_bench
def fetch_sites_analytics(bench):
    return Server().benches[bench].fetch_sites_analytics()


@application.route("/benches/<string:bench>/sites")
@validate_bench
def get_sites(bench):
    sites = Server().benches[bench].sites
    return {name: site.dump() for name, site in sites.items()}


@application.route("/benches/<string:bench>/apps")
@validate_bench
def get_apps(bench):
    apps = Server().benches[bench].apps
    return {name: site.dump() for name, site in apps.items()}


@application.route("/benches/<string:bench>/config")
@validate_bench
def get_config(bench):
    return Server().benches[bench].config


@application.route("/benches/<string:bench>/status", methods=["GET"])
@validate_bench
def get_bench_status(bench):
    return Server().benches[bench].status()


@application.route("/benches/<string:bench>/logs")
@validate_bench
def get_bench_logs(bench):
    return jsonify(Server().benches[bench].logs)


@application.route("/benches/<string:bench>/logs/<string:log>")
@validate_bench
def get_bench_log(bench, log):
    return {log: Server().benches[bench].retrieve_log(log)}


@application.route("/benches/<string:bench>/sites/<string:site>")
@validate_bench
def get_site(bench, site):
    return Server().benches[bench].sites[site].dump()


@application.route("/benches/<string:bench>/sites/<string:site>/logs")
@validate_bench_and_site
def get_logs(bench, site):
    return jsonify(Server().benches[bench].sites[site].logs)


@application.route(
    "/benches/<string:bench>/sites/<string:site>/logs/<string:log>"
)
@validate_bench_and_site
def get_log(bench, site, log):
    return {log: Server().benches[bench].sites[site].retrieve_log(log)}


@application.route("/security/ssh_session_logs")
def get_ssh_session_logs():
    return {"logs": Security().ssh_session_logs}


@application.route("/security/retrieve_ssh_session_log/<string:filename>")
def retrieve_ssh_session_log(filename):
    return {"log_details": Security().retrieve_ssh_session_log(filename)}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/sid", methods=["GET", "POST"]
)
@validate_bench_and_site
def get_site_sid(bench, site):
    data = request.json or {}
    user = data.get("user") or "Administrator"
    return {"sid": Server().benches[bench].sites[site].sid(user=user)}


@application.route("/benches", methods=["POST"])
def new_bench():
    data = request.json
    job = Server().new_bench(**data)
    return {"job": job}


@application.route("/benches/<string:bench>/archive", methods=["POST"])
def archive_bench(bench):
    job = Server().archive_bench(bench)
    return {"job": job}


@application.route("/benches/<string:bench>/restart", methods=["POST"])
@validate_bench
def restart_bench(bench):
    data = request.json
    job = Server().benches[bench].restart_job(**data)
    return {"job": job}


@application.route("/benches/<string:bench>/limits", methods=["POST"])
def update_bench_limits(bench):
    data = request.json
    job = Server().benches[bench].force_update_limits(**data)
    return {"job": job}


@application.route("/benches/<string:bench>/rebuild", methods=["POST"])
def rebuild_bench(bench):
    job = Server().benches[bench].rebuild_job()
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
@validate_bench
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
@validate_bench
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
            data.get("public"),
            data.get("private"),
            data.get("skip_failing_patches", False),
        )
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/restore", methods=["POST"]
)
@validate_bench_and_site
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
            data.get("public"),
            data.get("private"),
            data.get("skip_failing_patches", False),
        )
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/reinstall", methods=["POST"]
)
@validate_bench_and_site
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
@validate_bench_and_site
def rename_site(bench, site):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .rename_site_job(site, data["new_name"], data.get("create_user"))
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/optimize", methods=["POST"]
)
def optimize_tables(bench, site):
    job = Server().benches[bench].sites[site].optimize_tables_job()
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/apps", methods=["POST"]
)
@validate_bench_and_site
def install_app_site(bench, site):
    data = request.json
    job = Server().benches[bench].sites[site].install_app_job(data["name"])
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/apps/<string:app>",
    methods=["DELETE"],
)
@validate_bench_and_site
def uninstall_app_site(bench, site, app):
    job = Server().benches[bench].sites[site].uninstall_app_job(app)
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/erpnext", methods=["POST"]
)
@validate_bench_and_site
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
@validate_bench
def fetch_monitor_data(bench):
    return {"data": Server().benches[bench].fetch_monitor_data()}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/status", methods=["GET"]
)
@validate_bench_and_site
def fetch_site_status(bench, site):
    return {"data": Server().benches[bench].sites[site].fetch_site_status()}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/info", methods=["GET"]
)
@validate_bench_and_site
def fetch_site_info(bench, site):
    return {"data": Server().benches[bench].sites[site].fetch_site_info()}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/analytics", methods=["GET"]
)
@validate_bench_and_site
def fetch_site_analytics(bench, site):
    return {"data": Server().benches[bench].sites[site].fetch_site_analytics()}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/backup", methods=["POST"]
)
@validate_bench_and_site
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
@validate_bench_and_site
def migrate_site(bench, site):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .sites[site]
        .migrate_job(
            skip_failing_patches=data.get("skip_failing_patches", False),
            activate=data.get("activate", True),
        )
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/cache",
    methods=["DELETE"],
)
@validate_bench_and_site
def clear_site_cache(bench, site):
    job = Server().benches[bench].sites[site].clear_cache_job()
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/update/migrate",
    methods=["POST"],
)
@validate_bench_and_site
def update_site_migrate(bench, site):
    data = request.json
    job = Server().update_site_migrate_job(
        site,
        bench,
        data["target"],
        data.get("activate", True),
        data.get("skip_failing_patches", False),
        data.get("skip_backups", False),
        data.get("before_migrate_scripts", {}),
        data.get("skip_search_index", True),
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/update/pull", methods=["POST"]
)
@validate_bench_and_site
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
@validate_bench_and_site
def update_site_recover_migrate(bench, site):
    data = request.json
    job = Server().update_site_recover_migrate_job(
        site,
        bench,
        data["target"],
        data.get("activate", True),
        data.get("rollback_scripts", {}),
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/update/migrate/restore",
    methods=["POST"],
)
@validate_bench_and_site
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
@validate_bench_and_site
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
@validate_bench_and_site
def update_site_recover(bench, site):
    job = Server().update_site_recover_job(site, bench)
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/archive", methods=["POST"]
)
@validate_bench
def archive_site(bench, site):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .archive_site(site, data["mariadb_root_password"], data.get("force"))
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/config", methods=["POST"]
)
@validate_bench_and_site
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
    "/benches/<string:bench>/sites/<string:site>/usage", methods=["DELETE"]
)
@validate_bench_and_site
def reset_site_usage(bench, site):
    job = Server().benches[bench].sites[site].reset_site_usage_job()
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/domains", methods=["POST"]
)
@validate_bench_and_site
def site_add_domain(bench, site):
    data = request.json
    job = Server().benches[bench].sites[site].add_domain(data["domain"])
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/domains/<string:domain>",
    methods=["DELETE"],
)
@validate_bench_and_site
def site_remove_domain(bench, site, domain):
    job = Server().benches[bench].sites[site].remove_domain(domain)
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/describe-database-table",
    methods=["POST"],
)
@validate_bench_and_site
def describe_database_table(bench, site):
    data = request.json
    return {
        "data": Server()
        .benches[bench]
        .sites[site]
        .describe_database_table(data["doctype"], data.get("columns"))
    }


@application.route(
    "/benches/<string:bench>/sites/<string:site>/add-database-index",
    methods=["POST"],
)
@validate_bench_and_site
def add_database_index(bench, site):
    data = request.json
    return {
        "data": Server()
        .benches[bench]
        .sites[site]
        .add_database_index(data["doctype"], data.get("columns"))
    }


@application.route(
    "/benches/<string:bench>/sites/<string:site>/credentials",
    methods=["POST"],
)
@validate_bench_and_site
def site_create_database_access_credentials(bench, site):
    data = request.json
    credentials = (
        Server()
        .benches[bench]
        .sites[site]
        .create_database_access_credentials(
            data["mode"], data["mariadb_root_password"]
        )
    )
    return credentials


@application.route(
    "/benches/<string:bench>/sites/<string:site>/credentials/revoke",
    methods=["POST"],
)
@validate_bench_and_site
def site_revoke_database_access_credentials(bench, site):
    data = request.json
    return (
        Server()
        .benches[bench]
        .sites[site]
        .revoke_database_access_credentials(
            data["user"], data["mariadb_root_password"]
        )
    )


@application.route("/benches/<string:bench>/config", methods=["POST"])
@validate_bench
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


@application.route("/proxy/upstreams", methods=["GET"])
def get_upstreams():
    return Proxy().upstreams


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
    data = request.json
    job = Proxy().remove_site_from_upstream_job(
        upstream, site, data.get("skip_reload", False)
    )
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
    job = Proxy().update_site_status_job(
        upstream, site, data["status"], data.get("skip_reload", False)
    )
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


@application.route("/database/processes", methods=["POST"])
def get_database_processes():
    data = request.json
    return jsonify(DatabaseServer().processes(**data))


@application.route("/database/locks", methods=["POST"])
def get_database_locks():
    data = request.json
    return jsonify(DatabaseServer().locks(**data))


@application.route("/database/processes/kill", methods=["POST"])
def kill_database_processes():
    data = request.json
    return jsonify(DatabaseServer().kill_processes(**data))


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


@application.route("/database/stalks")
def get_stalks():
    return jsonify(DatabaseServer().get_stalks())


@application.route("/database/stalks/<string:stalk>")
def get_stalk(stalk):
    return jsonify(DatabaseServer().get_stalk(stalk))


@application.route("/database/deadlocks", methods=["POST"])
def get_database_deadlocks():
    data = request.json
    return jsonify(DatabaseServer().get_deadlocks(**data))


@application.route("/database/column-stats", methods=["POST"])
def fetch_column_stats():
    data = request.json
    return jsonify(DatabaseServer().fetch_column_stats(**data))


@application.route("/database/explain", methods=["POST"])
def explain():
    data = request.json
    return jsonify(DatabaseServer().explain_query(**data))


@application.route("/ssh/users", methods=["POST"])
def ssh_add_user():
    data = request.json

    job = SSHProxy().add_user_job(
        data["name"],
        data["principal"],
        data["ssh"],
        data["certificate"],
    )
    return {"job": job}


@application.route("/ssh/users/<string:user>", methods=["DELETE"])
def ssh_remove_user(user):
    job = SSHProxy().remove_user_job(user)
    return {"job": job}


@application.route("/proxysql/users", methods=["POST"])
def proxysql_add_user():
    data = request.json

    job = ProxySQL().add_user_job(
        data["username"],
        data["password"],
        data["database"],
        data["backend"],
    )
    return {"job": job}


@application.route("/proxysql/backends", methods=["POST"])
def proxysql_add_backend():
    data = request.json

    job = ProxySQL().add_backend_job(data["backend"])
    return {"job": job}


@application.route("/proxysql/users/<string:username>", methods=["DELETE"])
def proxysql_remove_user(username):
    job = ProxySQL().remove_user_job(username)
    return {"job": job}


def to_dict(model):
    redis = connection()
    if isinstance(model, JobModel):
        job = model_to_dict(model, backrefs=True)
        job["data"] = json.loads(job["data"]) or {}
        job_key = f"agent:job:{job['id']}"
        job["commands"] = [
            json.loads(command) for command in redis.lrange(job_key, 0, -1)
        ]
        for step in job["steps"]:
            step["data"] = json.loads(step["data"]) or {}
            step_key = f"{job_key}:step:{step['id']}"
            step["commands"] = [
                json.loads(command)
                for command in redis.lrange(
                    step_key,
                    0,
                    -1,
                )
            ]
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


@application.route("/agent-jobs")
@application.route("/agent-jobs/<int:id>")
@application.route("/agent-jobs/<string:ids>")
def agent_jobs(id=None, ids=None):
    if id:
        job = to_dict(JobModel.get(JobModel.agent_job_id == id))
        return jsonify(json.loads(json.dumps(job, default=str)))
    elif ids:
        ids = ids.split(",")
        job = list(
            map(to_dict, JobModel.select().where(JobModel.agent_job_id << ids))
        )
        return jsonify(json.loads(json.dumps(job, default=str)))


@application.route("/update", methods=["POST"])
def update_agent():
    data = request.json
    Server().update_agent_web(data.get("url"))
    return {"message": "Success"}


@application.route("/version", methods=["GET"])
def get_version():
    return Server().get_agent_version()


@application.route("/minio/users", methods=["POST"])
def create_minio_user():
    data = request.json
    job = Minio().create_subscription(
        data["access_key"],
        data["secret_key"],
        data["policy_name"],
        json.dumps(json.loads(data["policy_json"])),
    )
    return {"job": job}


@application.route(
    "/minio/users/<string:username>/toggle/<string:action>", methods=["POST"]
)
def toggle_minio_user(username, action):
    if action == "disable":
        job = Minio().disable_user(username)
    else:
        job = Minio().enable_user(username)
    return {"job": job}


@application.route("/minio/users/<string:username>", methods=["DELETE"])
def remove_minio_user(username):
    job = Minio().remove_user(username)
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/update/saas",
    methods=["POST"],
)
@validate_bench_and_site
def update_saas_plan(bench, site):
    data = request.json
    job = Server().benches[bench].sites[site].update_saas_plan(data["plan"])
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/run_after_migrate_steps",
    methods=["POST"],
)
@validate_bench_and_site
def run_after_migrate_steps(bench, site):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .sites[site]
        .run_after_migrate_steps_job(data["admin_password"])
    )
    return {"job": job}


@application.route(
    "/benches/<string:bench>/sites/<string:site>/move_to_bench",
    methods=["POST"],
)
@validate_bench_and_site
def move_site_to_bench(bench, site):
    data = request.json
    job = Server().move_site_to_bench(
        site,
        bench,
        data["target"],
        data.get("deactivate", True),
        data.get("activate", True),
        data.get("skip_failing_patches", False),
    )
    return {"job": job}


@application.route("/benches/<string:bench>/codeserver", methods=["POST"])
@validate_bench
def setup_code_server(bench):
    data = request.json
    job = Server().benches[bench].setup_code_server(**data)

    return {"job": job}


@application.route(
    "/benches/<string:bench>/codeserver/start", methods=["POST"]
)
@validate_bench
def start_code_server(bench):
    data = request.json
    job = Server().benches[bench].start_code_server(**data)
    return {"job": job}


@application.route("/benches/<string:bench>/codeserver/stop", methods=["POST"])
@validate_bench
def stop_code_server(bench):
    job = Server().benches[bench].stop_code_server()
    return {"job": job}


@application.route(
    "/benches/<string:bench>/codeserver/archive", methods=["POST"]
)
@validate_bench
def archive_code_server(bench):
    job = Server().benches[bench].archive_code_server()
    return {"job": job}


@application.route(
    "/benches/<string:bench>/patch/<string:app>", methods=["POST"]
)
@validate_bench
def patch_app(bench, app):
    data = request.json
    job = (
        Server()
        .benches[bench]
        .patch_app(
            app,
            data["patch"],
            data["filename"],
            data["build_assets"],
            data["revert"],
        )
    )
    return {"job": job}


@application.errorhandler(Exception)
def all_exception_handler(error):
    try:
        from sentry_sdk import capture_exception

        capture_exception(error)
    except ImportError:
        pass
    return {
        "error": "".join(
            traceback.format_exception(*sys.exc_info())
        ).splitlines()
    }, 500


@application.route("/benches/<string:bench>/docker_execute", methods=["POST"])
@validate_bench
def docker_execute(bench: str):
    data = request.json
    _bench = Server().benches[bench]
    result: "ExecuteReturn" = _bench.docker_execute(
        command=data.get("command"),
        subdir=data.get("subdir"),
        non_zero_throw=False,
    )

    result["start"] = result["start"].isoformat()
    result["end"] = result["end"].isoformat()
    result["duration"] = result["duration"].total_seconds()
    return result


@application.route("/benches/<string:bench>/supervisorctl", methods=["POST"])
@validate_bench
def call_bench_supervisorctl(bench: str):
    data = request.json
    _bench = Server().benches[bench]
    job = _bench.call_supervisorctl(
        data["command"],
        data["programs"],
    )
    return {"job": job}


@application.errorhandler(BenchNotExistsException)
def bench_not_found(e):
    return {
        "error": "".join(
            traceback.format_exception(*sys.exc_info())
        ).splitlines()
    }, 404


@application.errorhandler(SiteNotExistsException)
def site_not_found(e):
    return {
        "error": "".join(
            traceback.format_exception(*sys.exc_info())
        ).splitlines()
    }, 404


@application.route("/docker_cache_utils/<string:method>", methods=["POST"])
def docker_cache_utils(method: str):
    from agent.docker_cache_utils import (
        run_command_in_docker_cache,
        get_cached_apps,
    )

    if method == "run_command_in_docker_cache":
        return run_command_in_docker_cache(**request.json)

    if method == "get_cached_apps":
        return get_cached_apps()

    return None
