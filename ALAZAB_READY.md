# Alazab Agent — Ready Build 05

Role: Microsoft Foundry / AI control plane.

Primary capabilities:
- Foundry agents and model/deployment inventory
- Agent Studio
- training sessions
- knowledge ingest/review/publish/bind pipeline
- configurations
- MCP / A2A / API / Webhook / AI Tool registry
- production tool catalog and audit
- A2A participant/context/task persistence
- GPT-5.6 Sol A2A orchestration through Foundry Responses
- standard A2A v1.x sidecar

Install AI+A2A extras from this source directory:
`python -m pip install -e '.[ai-a2a]'`

Live values must come from the actual environment:
- `FOUNDRY_PROJECT_ENDPOINT`
- `A2A_ORCHESTRATOR_MODEL` (only when explicitly required; runtime can resolve an unambiguous synchronized gpt-5.6-sol deployment)
- `A2A_COPILOT_BASE_URL`
- `ALAZAB_AGENT_CHANNEL_SECRET`
- `A2A_COPILOT_CONFIGURATION`

A2A sidecar defaults to localhost. Do not expose it externally before live validation.

Validation in build environment:
- 9/9 Agent A2A static contract tests passed
- Python compile passed
- JavaScript syntax passed

This package contains only Agent; it does not include Copilot.
