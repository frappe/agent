from __future__ import annotations

import time
import traceback

from flask import Blueprint, current_app, jsonify, render_template, request
from peewee import DoesNotExist

from agent.ai_control.foundry import FoundryClient
from agent.ai_control.execution import FoundryAgentRuntime
from agent.ai_control.a2a_runtime import (
    send_message as send_a2a_message,
    status as a2a_status_payload,
    sync_participants as sync_a2a_participants,
    task as get_a2a_task_payload,
    tasks as list_a2a_task_payloads,
)
from agent.ai_control.knowledge import inspect_and_process, mark_failed, preview, save_upload
from agent.ai_control.training import add_message, create_session, get_session, list_messages, list_sessions, send_training_message
from agent.ai_control.protocols import ProtocolTestError, test_integration
from agent.ai_control.store import (
    create_binding,
    create_integration,
    delete_binding,
    delete_integration,
    finish_execution,
    get_asset,
    get_integration,
    get_knowledge,
    list_assets,
    list_bindings,
    list_configurations,
    list_executions,
    list_integrations,
    list_knowledge,
    list_production_tools,
    record_execution,
    set_knowledge_approval,
    topology,
    update_configuration,
    update_integration,
    update_production_tool,
    upsert_asset,
    upsert_production_tool,
    create_configuration,
    get_a2a_participant,
    list_a2a_participants,
    update_a2a_participant,
    remove_agent_tool_binding,
    list_bound_production_tools,
    list_approval_requests,
    get_production_tool,
    get_execution,
    get_agent_tool_binding,
    decide_approval_request,
)

ai_control = Blueprint("ai_control", __name__, url_prefix="/ai")

AI_CONTROL_PAGES = {"dashboard", "agents", "training", "knowledge", "configurations", "production-tools", "integrations", "a2a", "topology", "runs"}


@ai_control.route("/")
def index():
    return render_template("ai_control/index.html", active_page="dashboard")


@ai_control.route("/agents/<string:agent_name>")
def agent_studio_page(agent_name: str):
    return render_template("ai_control/index.html", active_page="agent-studio", active_agent=agent_name)


@ai_control.route("/<string:page>")
def ui_page(page: str):
    if page not in AI_CONTROL_PAGES:
        return jsonify({"error": "AI Control page not found"}), 404
    return render_template("ai_control/index.html", active_page=page)


@ai_control.route("/api/overview")
def overview():
    integrations = list_integrations()
    assets = list_assets()
    return jsonify(
        {
            "integrations": {
                kind: sum(1 for x in integrations if x["type"] == kind)
                for kind in ("mcp", "a2a", "api", "webhook", "ai_tool")
            },
            "assets": {
                kind: sum(1 for x in assets if x["type"] == kind)
                for kind in ("agent", "model", "tool", "toolbox", "connection")
            },
        }
    )


@ai_control.route("/api/integrations", methods=["GET", "POST"])
def integrations():
    if request.method == "GET":
        return jsonify(list_integrations(request.args.get("type")))
    try:
        row = create_integration(request.get_json(force=True) or {})
        return jsonify(row.as_dict()), 201
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@ai_control.route("/api/integrations/<int:integration_id>", methods=["GET", "PATCH", "DELETE"])
def integration(integration_id: int):
    try:
        if request.method == "GET":
            return jsonify(get_integration(integration_id).as_dict())
        if request.method == "PATCH":
            return jsonify(update_integration(integration_id, request.get_json(force=True) or {}).as_dict())
        return jsonify(delete_integration(integration_id))
    except DoesNotExist:
        return jsonify({"error": "Integration not found"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@ai_control.route("/api/integrations/<int:integration_id>/test", methods=["POST"])
def integration_test(integration_id: int):
    try:
        row = get_integration(integration_id)
    except DoesNotExist:
        return jsonify({"error": "Integration not found"}), 404

    execution = record_execution("test", row.integration_type, row.id, {"endpoint": row.endpoint})
    started = time.monotonic()
    try:
        result = test_integration(row)
    except ProtocolTestError as exc:
        result = {"ok": False, "error": str(exc)}
        finish_execution(execution, "Unavailable", result, (time.monotonic() - started) * 1000)
        return jsonify(result), 501
    except Exception as exc:
        result = {"ok": False, "error": str(exc), "traceback": traceback.format_exc().splitlines()}
        finish_execution(execution, "Failure", result, (time.monotonic() - started) * 1000)
        return jsonify(result), 502
    finish_execution(execution, "Success" if result.get("ok") else "Failure", result, (time.monotonic() - started) * 1000)
    return jsonify(result)


@ai_control.route("/api/assets")
def assets():
    return jsonify(list_assets(request.args.get("type"), request.args.get("project")))


@ai_control.route("/api/foundry/capabilities")
def foundry_capabilities():
    try:
        return jsonify(FoundryClient().capability_summary())
    except Exception as exc:
        return jsonify({"configured": False, "error": str(exc)}), 503


@ai_control.route("/api/foundry/sync/deployments", methods=["POST"])
def sync_foundry_deployments():
    execution = record_execution("sync", "foundry_deployments")
    started = time.monotonic()
    try:
        client = FoundryClient()
        deployments = client.list_deployments()
        project = client.config.project_name or "unknown"
        saved = []
        for deployment in deployments:
            name = deployment.get("name") or deployment.get("deployment_name") or deployment.get("id")
            if not name:
                continue
            asset = upsert_asset(
                {
                    "project": project,
                    "type": "model",
                    "name": str(name),
                    "external_id": deployment.get("id"),
                    "status": deployment.get("state") or deployment.get("provisioning_state"),
                    "version": deployment.get("version") or (deployment.get("model") or {}).get("version") if isinstance(deployment.get("model"), dict) else None,
                    "metadata": deployment,
                }
            )
            saved.append(asset.as_dict())
        finish_execution(execution, "Success", {"count": len(saved)}, (time.monotonic() - started) * 1000)
        return jsonify({"count": len(saved), "assets": saved})
    except Exception as exc:
        result = {"error": str(exc)}
        finish_execution(execution, "Failure", result, (time.monotonic() - started) * 1000)
        return jsonify(result), 502


def _asset_name(data):
    return data.get("name") or data.get("display_name") or data.get("id")


def _asset_status(data):
    return data.get("status") or data.get("state") or data.get("provisioning_state")


def _asset_version(data):
    value = data.get("version") or data.get("latest_version")
    if value is not None:
        return str(value)
    return None


@ai_control.route("/api/foundry/sync", methods=["POST"])
def sync_foundry_inventory():
    execution = record_execution("sync", "foundry_inventory")
    started = time.monotonic()
    try:
        client = FoundryClient()
        project_name = client.config.project_name or "unknown"
        groups = (
            ("agent", client.list_agents()),
            ("model", client.list_deployments()),
            ("toolbox", client.list_toolboxes()),
            ("connection", client.list_connections()),
        )
        saved = []
        for asset_type, items in groups:
            for item in items:
                name = _asset_name(item)
                if not name:
                    continue
                row = upsert_asset(
                    {
                        "project": project_name,
                        "type": asset_type,
                        "name": str(name),
                        "external_id": item.get("id"),
                        "status": _asset_status(item),
                        "version": _asset_version(item),
                        "metadata": item,
                    }
                )
                saved.append(row.as_dict())
        summary = {}
        for row in saved:
            summary[row["type"]] = summary.get(row["type"], 0) + 1
        finish_execution(execution, "Success", summary, (time.monotonic() - started) * 1000)
        return jsonify({"project": project_name, "counts": summary, "assets": saved})
    except Exception as exc:
        result = {"error": str(exc)}
        finish_execution(execution, "Failure", result, (time.monotonic() - started) * 1000)
        return jsonify(result), 502


@ai_control.route("/api/foundry/agents/<string:agent_name>/chat", methods=["POST"])
def chat_with_foundry_agent(agent_name: str):
    payload = request.get_json(force=True) or {}
    message = str(payload.get("message") or "").strip()
    if not message:
        return jsonify({"error": "message is required"}), 400
    execution = record_execution(
        "chat",
        "foundry_agent",
        agent_name,
        {"message": message, "conversation_id": payload.get("conversation_id")},
        status="Running",
    )
    started = time.monotonic()
    try:
        result = FoundryClient().chat_with_agent(
            agent_name,
            message,
            conversation_id=payload.get("conversation_id"),
        )
        finish_execution(execution, "Success", result, (time.monotonic() - started) * 1000)
        return jsonify(result)
    except Exception as exc:
        result = {"error": str(exc)}
        finish_execution(execution, "Failure", result, (time.monotonic() - started) * 1000)
        return jsonify(result), 502


@ai_control.route("/api/foundry/agents/<string:agent_name>/invoke", methods=["POST"])
def invoke_foundry_agent(agent_name: str):
    payload = request.get_json(force=True) or {}
    message = str(payload.get("message") or "").strip()
    if not message:
        return jsonify({"error": "message is required"}), 400
    actor = request.headers.get("X-Operator") or payload.get("requested_by")
    try:
        result = FoundryAgentRuntime().start(
            agent_name,
            message,
            conversation_id=payload.get("conversation_id"),
            requested_by=actor,
        )
        code = 202 if result["run"]["status"] == "AwaitingApproval" else 200
        return jsonify(result), code
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@ai_control.route("/api/foundry/agents/<string:agent_name>/tools", methods=["GET"])
def foundry_agent_tools(agent_name: str):
    try:
        return jsonify(FoundryAgentRuntime().tool_inventory(agent_name))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@ai_control.route("/api/foundry/agents/<string:agent_name>/tools/<int:tool_id>", methods=["POST", "DELETE"])
def foundry_agent_tool_binding(agent_name: str, tool_id: int):
    try:
        get_production_tool(tool_id)
    except DoesNotExist:
        return jsonify({"error": "Production tool not found"}), 404

    if request.method == "DELETE":
        try:
            return jsonify(remove_agent_tool_binding(agent_name, tool_id))
        except DoesNotExist:
            return jsonify({"error": "Agent tool binding not found"}), 404

    payload = request.get_json(silent=True) or {}
    existing = get_agent_tool_binding(agent_name, tool_id)
    if existing:
        return jsonify(existing.as_dict())
    try:
        row = create_binding(
            {
                "source_type": "agent",
                "source_ref": agent_name,
                "target_type": "production_tool",
                "target_ref": str(tool_id),
                "status": "Active",
                "config": payload.get("config") or {},
            }
        )
        return jsonify(row.as_dict()), 201
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@ai_control.route("/api/foundry/agents/<string:agent_name>/tools/sync", methods=["POST"])
def sync_foundry_agent_tools(agent_name: str):
    execution = record_execution("sync_tools", "foundry_agent", agent_name, status="Running")
    started = time.monotonic()
    try:
        result = FoundryAgentRuntime().sync_tools(agent_name)
        finish_execution(execution, "Success", result, (time.monotonic() - started) * 1000)
        return jsonify(result)
    except Exception as exc:
        result = {"error": str(exc)}
        finish_execution(execution, "Failure", result, (time.monotonic() - started) * 1000)
        return jsonify(result), 502


@ai_control.route("/api/foundry/tools/<string:tool_ref>/execute", methods=["POST"])
def execute_foundry_bound_tool(tool_ref: str):
    payload = request.get_json(force=True) or {}
    agent_name = str(payload.get("agent_name") or "").strip()
    if not agent_name:
        return jsonify({"error": "agent_name is required"}), 400
    actor = request.headers.get("X-Operator") or payload.get("requested_by")
    try:
        result = FoundryAgentRuntime().execute_tool(
            agent_name,
            tool_ref,
            payload.get("arguments") or {},
            requested_by=actor,
        )
        code = 202 if result["run"]["status"] == "AwaitingApproval" else 200
        return jsonify(result), code
    except DoesNotExist:
        return jsonify({"error": "Production tool not found"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@ai_control.route("/api/approvals", methods=["GET"])
def approvals():
    limit = min(max(int(request.args.get("limit", 200)), 1), 2000)
    execution_id = request.args.get("execution_id")
    return jsonify(
        list_approval_requests(
            status=request.args.get("status"),
            execution_id=int(execution_id) if execution_id else None,
            limit=limit,
        )
    )


@ai_control.route("/api/approvals/<int:approval_id>/decision", methods=["POST"])
def approval_decision(approval_id: int):
    payload = request.get_json(force=True) or {}
    actor = request.headers.get("X-Operator") or payload.get("decided_by") or "operator"
    try:
        approval = decide_approval_request(
            approval_id,
            payload.get("decision"),
            decided_by=actor,
            note=payload.get("note"),
        )
        execution = get_execution(approval.execution_id)
        runtime = FoundryAgentRuntime()
        if execution.resource_type == "foundry_agent_run":
            run = runtime.resume(execution.id)
        elif execution.resource_type == "production_tool":
            run = runtime.resume_direct(execution.id)
        else:
            run = execution.as_dict()
        return jsonify({"approval": approval.as_dict(), "execution": run})
    except DoesNotExist:
        return jsonify({"error": "Approval or execution not found"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@ai_control.route("/api/runs/<int:execution_id>", methods=["GET"])
def run_detail(execution_id: int):
    try:
        return jsonify(FoundryAgentRuntime().run_detail(execution_id))
    except DoesNotExist:
        return jsonify({"error": "Run not found"}), 404


@ai_control.route("/api/foundry/agents/<string:agent_name>/enabled", methods=["POST"])
def set_foundry_agent_enabled(agent_name: str):
    payload = request.get_json(force=True) or {}
    enabled = bool(payload.get("enabled"))
    execution = record_execution("enable" if enabled else "disable", "foundry_agent", agent_name)
    started = time.monotonic()
    try:
        result = FoundryClient().set_agent_enabled(agent_name, enabled)
        finish_execution(execution, "Success", result, (time.monotonic() - started) * 1000)
        return jsonify(result)
    except Exception as exc:
        result = {"error": str(exc)}
        finish_execution(execution, "Failure", result, (time.monotonic() - started) * 1000)
        return jsonify(result), 502


@ai_control.route("/webhooks/<int:integration_id>", methods=["POST"])
def receive_webhook(integration_id: int):
    try:
        row = get_integration(integration_id)
    except DoesNotExist:
        return jsonify({"error": "Webhook integration not found"}), 404
    if row.integration_type != "webhook":
        return jsonify({"error": "Integration is not a webhook"}), 409
    payload = request.get_json(silent=True)
    if payload is None:
        payload = {"raw": request.get_data(as_text=True)}
    execution = record_execution(
        "receive",
        "webhook",
        row.id,
        {
            "payload": payload,
            "headers": {k: v for k, v in request.headers.items() if k.lower() not in {"authorization", "cookie"}},
        },
        status="Success",
    )
    finish_execution(execution, "Success", {"accepted": True})
    return jsonify({"accepted": True, "execution_id": execution.id}), 202


@ai_control.route("/api/bindings", methods=["GET", "POST"])
def bindings():
    if request.method == "GET":
        return jsonify(list_bindings())
    try:
        row = create_binding(request.get_json(force=True) or {})
        return jsonify(row.as_dict()), 201
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@ai_control.route("/api/bindings/<int:binding_id>", methods=["DELETE"])
def binding(binding_id: int):
    try:
        return jsonify(delete_binding(binding_id))
    except DoesNotExist:
        return jsonify({"error": "Binding not found"}), 404


@ai_control.route("/api/a2a/status")
def a2a_status():
    return jsonify(a2a_status_payload())


@ai_control.route("/api/a2a/sync", methods=["POST"])
def a2a_sync():
    execution = record_execution("sync", "a2a_participants")
    started = time.monotonic()
    try:
        result = sync_a2a_participants()
        status = "Success" if not result.get("errors") else "Partial"
        finish_execution(execution, status, result, (time.monotonic() - started) * 1000)
        return jsonify(result)
    except Exception as exc:
        result = {"error": str(exc)}
        finish_execution(execution, "Failure", result, (time.monotonic() - started) * 1000)
        return jsonify(result), 502


@ai_control.route("/api/a2a/participants")
def a2a_participants():
    return jsonify(list_a2a_participants(request.args.get("type"), request.args.get("status")))


@ai_control.route("/api/a2a/participants/<string:participant_name>", methods=["GET", "PATCH"])
def a2a_participant(participant_name: str):
    try:
        if request.method == "GET":
            return jsonify(get_a2a_participant(participant_name).as_dict())
        payload = request.get_json(force=True) or {}
        allowed = {key: payload[key] for key in ("status", "endpoint", "agent_card", "config") if key in payload}
        if not allowed:
            return jsonify({"error": "No supported participant fields supplied"}), 400
        return jsonify(update_a2a_participant(participant_name, allowed).as_dict())
    except DoesNotExist:
        return jsonify({"error": "A2A participant not found"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@ai_control.route("/api/a2a/participants/<string:participant_name>/card")
def a2a_participant_card(participant_name: str):
    try:
        row = get_a2a_participant(participant_name).as_dict()
        return jsonify(row.get("agent_card") or {})
    except DoesNotExist:
        return jsonify({"error": "A2A participant not found"}), 404


@ai_control.route("/api/a2a/message", methods=["POST"])
def a2a_message():
    payload = request.get_json(force=True) or {}
    message = str(payload.get("message") or "").strip()
    if not message:
        return jsonify({"error": "message is required"}), 400
    try:
        result = send_a2a_message(
            message,
            source=str(payload.get("source") or "agent-ui"),
            context_id=str(payload.get("context_id") or "").strip() or None,
        )
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@ai_control.route("/api/a2a/tasks")
def a2a_tasks():
    try:
        limit = int(request.args.get("limit") or 100)
    except ValueError:
        return jsonify({"error": "limit must be an integer"}), 400
    return jsonify(list_a2a_task_payloads(limit=limit, context_id=request.args.get("context_id")))


@ai_control.route("/api/a2a/tasks/<string:task_id>")
def a2a_task_detail(task_id: str):
    try:
        return jsonify(get_a2a_task_payload(task_id))
    except DoesNotExist:
        return jsonify({"error": "A2A task not found"}), 404


@ai_control.route("/api/topology")
def get_topology():
    return jsonify(topology())


@ai_control.route("/api/agent-studio/<string:agent_name>")
def agent_studio(agent_name: str):
    assets = [x for x in list_assets("agent") if x["name"] == agent_name]
    if not assets:
        return jsonify({"error": "Agent not found in synchronized inventory"}), 404
    bindings = [x for x in list_bindings() if x["source_type"] == "agent" and x["source_ref"] == agent_name]
    knowledge_by_id = {str(x["id"]): x for x in list_knowledge()}
    production_by_id = {str(x["id"]): x for x in list_production_tools()}
    integrations_by_id = {str(x["id"]): x for x in list_integrations()}
    attached = {"knowledge": [], "production_tool": [], "integration": [], "other": []}
    for binding in bindings:
        target_type = binding["target_type"]
        target_ref = str(binding["target_ref"])
        if target_type == "knowledge" and target_ref in knowledge_by_id:
            attached["knowledge"].append({"binding": binding, "resource": knowledge_by_id[target_ref]})
        elif target_type == "production_tool" and target_ref in production_by_id:
            attached["production_tool"].append({"binding": binding, "resource": production_by_id[target_ref]})
        elif target_type in {"mcp", "a2a", "api", "webhook", "ai_tool", "integration"} and target_ref in integrations_by_id:
            attached["integration"].append({"binding": binding, "resource": integrations_by_id[target_ref]})
        else:
            attached["other"].append(binding)
    return jsonify({
        "agent": assets[0],
        "bindings": bindings,
        "attached": attached,
        "configurations": list_configurations("agent", agent_name),
        "training_sessions": list_sessions(agent_name),
    })


@ai_control.route("/api/dashboard")
def dashboard():
    integrations = list_integrations()
    assets = list_assets()
    knowledge = list_knowledge()
    tools = list_production_tools()
    executions = list_executions(limit=10)
    return jsonify(
        {
            "agents": sum(1 for x in assets if x["type"] == "agent"),
            "models": sum(1 for x in assets if x["type"] == "model"),
            "integrations": len(integrations),
            "knowledge": {
                "total": len(knowledge),
                "approved": sum(1 for x in knowledge if x["approval_status"] == "Approved"),
                "pending": sum(1 for x in knowledge if x["approval_status"] == "Pending"),
            },
            "production_tools": {
                "total": len(tools),
                "active": sum(1 for x in tools if x["status"] == "Active"),
            },
            "recent_runs": executions,
        }
    )


@ai_control.route("/api/training/sessions", methods=["GET", "POST"])
def training_sessions():
    if request.method == "GET":
        return jsonify(list_sessions(request.args.get("agent")))
    try:
        row = create_session(request.get_json(force=True) or {})
        return jsonify(row.as_dict()), 201
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@ai_control.route("/api/training/sessions/<int:session_id>/messages", methods=["GET", "POST"])
def training_messages(session_id: int):
    if request.method == "GET":
        return jsonify(list_messages(session_id))
    payload = request.get_json(force=True) or {}
    execution = record_execution("training_chat", "foundry_agent", payload.get("agent_name"), {"session_id": session_id})
    started = time.monotonic()
    try:
        result = send_training_message(session_id, payload.get("content"))
        finish_execution(execution, "Success", {"response_id": result["runtime"].get("response_id")}, (time.monotonic() - started) * 1000)
        return jsonify(result)
    except ValueError as exc:
        finish_execution(execution, "Failure", {"error": str(exc)}, (time.monotonic() - started) * 1000)
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        finish_execution(execution, "Failure", {"error": str(exc)}, (time.monotonic() - started) * 1000)
        return jsonify({"error": str(exc)}), 502


@ai_control.route("/api/training/sessions/<int:session_id>/attachments", methods=["POST"])
def training_attachment(session_id: int):
    try:
        session = get_session(session_id)
    except DoesNotExist:
        return jsonify({"error": "Training session not found"}), 404
    if "file" not in request.files:
        return jsonify({"error": "file is required"}), 400
    execution = record_execution("training_attachment", "knowledge", request.files["file"].filename, {"session_id": session_id, "agent": session.agent_name})
    started = time.monotonic()
    try:
        row = save_upload(request.files["file"])
        row = inspect_and_process(row)
        binding = create_binding({
            "source_type": "agent",
            "source_ref": session.agent_name,
            "target_type": "knowledge",
            "target_ref": str(row.id),
            "status": "PendingReview",
            "config": {"training_session_id": session.id},
        })
        add_message(
            session.id,
            "system",
            f"Knowledge attachment prepared: {row.original_filename} · pending review",
            metadata={"knowledge_id": row.id, "binding_id": binding.id, "status": "PendingReview"},
        )
        finish_execution(execution, "Success", {"knowledge_id": row.id, "binding_id": binding.id}, (time.monotonic() - started) * 1000)
        return jsonify({"knowledge": row.as_dict(), "binding": binding.as_dict()}), 201
    except Exception as exc:
        finish_execution(execution, "Failure", {"error": str(exc)}, (time.monotonic() - started) * 1000)
        return jsonify({"error": str(exc)}), 422


@ai_control.route("/api/knowledge", methods=["GET", "POST"])
def knowledge_collection():
    if request.method == "GET":
        return jsonify(list_knowledge(request.args.get("status")))
    if "file" not in request.files:
        return jsonify({"error": "file is required"}), 400
    execution = record_execution("upload", "knowledge", request.files["file"].filename)
    started = time.monotonic()
    try:
        row = save_upload(request.files["file"])
        finish_execution(execution, "Success", {"knowledge_id": row.id, "sha256": row.sha256}, (time.monotonic() - started) * 1000)
        return jsonify(row.as_dict()), 201
    except Exception as exc:
        finish_execution(execution, "Failure", {"error": str(exc)}, (time.monotonic() - started) * 1000)
        return jsonify({"error": str(exc)}), 500


@ai_control.route("/api/knowledge/<int:knowledge_id>")
def knowledge_detail(knowledge_id: int):
    try:
        return jsonify(get_knowledge(knowledge_id).as_dict(include_paths=False))
    except DoesNotExist:
        return jsonify({"error": "Knowledge source not found"}), 404


@ai_control.route("/api/knowledge/<int:knowledge_id>/process", methods=["POST"])
def knowledge_process(knowledge_id: int):
    try:
        row = get_knowledge(knowledge_id)
    except DoesNotExist:
        return jsonify({"error": "Knowledge source not found"}), 404
    execution = record_execution("inspect_extract_clean_document", "knowledge", knowledge_id)
    started = time.monotonic()
    try:
        row = inspect_and_process(row)
        finish_execution(execution, "Success", row.as_dict(), (time.monotonic() - started) * 1000)
        return jsonify(row.as_dict())
    except Exception as exc:
        mark_failed(row, exc)
        finish_execution(execution, "Failure", {"error": str(exc)}, (time.monotonic() - started) * 1000)
        return jsonify({"error": str(exc), "knowledge": row.as_dict()}), 422


@ai_control.route("/api/knowledge/<int:knowledge_id>/preview")
def knowledge_preview(knowledge_id: int):
    try:
        row = get_knowledge(knowledge_id)
    except DoesNotExist:
        return jsonify({"error": "Knowledge source not found"}), 404
    variant = request.args.get("variant", "clean")
    return jsonify({"id": row.id, "variant": variant, "content": preview(row, variant)})


@ai_control.route("/api/knowledge/<int:knowledge_id>/approval", methods=["POST"])
def knowledge_approval(knowledge_id: int):
    payload = request.get_json(force=True) or {}
    try:
        approved = bool(payload.get("approved"))
        row = set_knowledge_approval(knowledge_id, approved)
        from agent.ai_control.models import AIBindingModel
        bindings = AIBindingModel.select().where(
            (AIBindingModel.target_type == "knowledge") &
            (AIBindingModel.target_ref == str(knowledge_id))
        )
        for binding in bindings:
            if binding.status == "PendingReview":
                binding.status = "Active" if approved else "Rejected"
                binding.save()
        return jsonify(row.as_dict())
    except DoesNotExist:
        return jsonify({"error": "Knowledge source not found"}), 404


@ai_control.route("/api/knowledge/<int:knowledge_id>/publish", methods=["POST"])
def knowledge_publish(knowledge_id: int):
    try:
        row = get_knowledge(knowledge_id)
    except DoesNotExist:
        return jsonify({"error": "Knowledge source not found"}), 404
    if row.approval_status != "Approved":
        return jsonify({"error": "Knowledge must be approved before publishing"}), 409
    payload = request.get_json(force=True) or {}
    vector_store_id = str(payload.get("vector_store_id") or "").strip()
    execution = record_execution("publish", "knowledge", row.id, {"vector_store_id": vector_store_id or None})
    started = time.monotonic()
    try:
        client = FoundryClient()
        if not vector_store_id:
            created = client.create_vector_store(payload.get("vector_store_name") or f"agent-knowledge-{row.id}")
            vector_store_id = created.get("id")
        if not vector_store_id:
            raise RuntimeError("Foundry did not return a vector store id")
        result = client.publish_file_to_vector_store(row.document_path or row.clean_path, vector_store_id)
        row.vector_store_id = vector_store_id
        row.foundry_file_id = result.get("file_id")
        row.status = "Published"
        row.save()
        finish_execution(execution, "Success", result, (time.monotonic() - started) * 1000)
        return jsonify({"knowledge": row.as_dict(), "foundry": result})
    except Exception as exc:
        finish_execution(execution, "Failure", {"error": str(exc)}, (time.monotonic() - started) * 1000)
        return jsonify({"error": str(exc)}), 502


@ai_control.route("/api/knowledge/<int:knowledge_id>/bind", methods=["POST"])
def knowledge_bind(knowledge_id: int):
    try:
        row = get_knowledge(knowledge_id)
    except DoesNotExist:
        return jsonify({"error": "Knowledge source not found"}), 404
    payload = request.get_json(force=True) or {}
    agent_name = str(payload.get("agent_name") or "").strip()
    if not agent_name:
        return jsonify({"error": "agent_name is required"}), 400
    try:
        binding = create_binding({
            "source_type": "agent",
            "source_ref": agent_name,
            "target_type": "knowledge",
            "target_ref": str(row.id),
            "config": {"vector_store_id": row.vector_store_id, "foundry_file_id": row.foundry_file_id},
        })
        return jsonify(binding.as_dict()), 201
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@ai_control.route("/api/configurations", methods=["GET", "POST"])
def configurations():
    if request.method == "GET":
        return jsonify(list_configurations(request.args.get("scope_type"), request.args.get("scope_ref")))
    try:
        row = create_configuration(request.get_json(force=True) or {})
        return jsonify(row.as_dict()), 201
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@ai_control.route("/api/configurations/<int:config_id>", methods=["PATCH"])
def configuration(config_id: int):
    try:
        return jsonify(update_configuration(config_id, request.get_json(force=True) or {}).as_dict())
    except DoesNotExist:
        return jsonify({"error": "Configuration not found"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


def _route_category(route: str) -> str:
    if route.startswith("/benches") or "/bench" in route:
        return "bench"
    if "/sites" in route or "/site" in route:
        return "site"
    if "database" in route or "/db" in route:
        return "database"
    if "server" in route:
        return "server"
    if "builder" in route:
        return "build"
    return "agent_api"


def _route_risk(methods: list[str], route: str = "") -> tuple[str, str]:
    normalized = {str(method).upper() for method in methods}
    route_lower = str(route or "").lower()
    if not (normalized & {"POST", "PUT", "PATCH", "DELETE"}):
        return "read", "auto"
    high_markers = (
        "delete", "drop", "restore", "reinstall", "restart", "migrate",
        "deploy", "database", "bench/update", "server",
    )
    if "DELETE" in normalized or any(marker in route_lower for marker in high_markers):
        return "high", "manual"
    return "review", "confirm"


@ai_control.route("/api/production-tools/discover", methods=["POST"])
def discover_production_tools():
    saved = []
    for rule in current_app.url_map.iter_rules():
        route = str(rule.rule)
        if route.startswith("/ai/") or route in {"/static/<path:filename>"}:
            continue
        methods = sorted(set(rule.methods or []) - {"HEAD", "OPTIONS"})
        if not methods:
            continue
        risk, approval = _route_risk(methods, route)
        name = f"{rule.endpoint}:{','.join(methods)}"
        row = upsert_production_tool({
            "name": name,
            "display_name": rule.endpoint.replace("_", " ").title(),
            "category": _route_category(route),
            "source": "Agent API",
            "route": route,
            "methods": methods,
            "status": "Discovered",
            "risk_level": risk,
            "approval_policy": approval,
            "config": {"endpoint": rule.endpoint},
        })
        saved.append(row.as_dict())
    return jsonify({"count": len(saved), "tools": saved})


@ai_control.route("/api/production-tools", methods=["GET"])
def production_tools():
    return jsonify(list_production_tools(request.args.get("status")))


@ai_control.route("/api/production-tools/<int:tool_id>", methods=["PATCH"])
def production_tool(tool_id: int):
    try:
        return jsonify(update_production_tool(tool_id, request.get_json(force=True) or {}).as_dict())
    except DoesNotExist:
        return jsonify({"error": "Production tool not found"}), 404


@ai_control.route("/api/runs")
def runs():
    limit = min(max(int(request.args.get("limit", 100)), 1), 1000)
    return jsonify(list_executions(limit=limit, resource_type=request.args.get("resource_type")))
