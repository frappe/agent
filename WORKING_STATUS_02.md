# Agent AI Control Center — Working Snapshot 02

This is a continuing development snapshot, not a final release.

## Implemented in this snapshot

- Multi-page AI Control Center UI.
- Dashboard and Foundry inventory views.
- Agent Studio per Foundry agent.
- Training sessions and multi-turn Foundry Conversations + Responses chat adapter.
- Training file attachments routed into the knowledge review pipeline.
- Knowledge Library pipeline:
  - preserve original
  - inspect/extract
  - clean without semantic rewriting
  - generate provenance/reference document
  - review/approve
  - publish approved reference document to a Foundry vector store
  - bind knowledge to an agent
- File extractors for text/code/config, JSON, CSV, HTML, PDF, DOCX, XLSX/XLSM.
- Configuration registry with Global/Agent/Tool/Integration scopes.
- Production Tool registry with live discovery from the actual Flask Agent route map.
- Runtime risk/approval metadata for discovered tools.
- MCP/A2A/API/Webhook/AI Tool integration registry retained.
- Unified topology includes knowledge and production tools.
- Unified Runs & Audit screen.
- New database patch for added AI Control tables.
- Optional `agent[ai]` dependency group.

## Important operating rule

Original uploaded files are not modified. Processed/clean/reference artefacts are stored separately with SHA-256 provenance.
Training attachments enter as `PendingReview`; approval activates their knowledge binding.

## Static verification performed

- Python compile checks passed for all AI Control modules.
- JavaScript syntax check passed.
- Static API/UI contract checks passed for knowledge, training, configurations, production tools, agent studio, and multi-page UI.

## Runtime verification still required on the actual Agent/WSL environment

The container used to assemble this patch does not have Agent runtime dependencies (Flask/Peewee/Azure SDK) installed, so live Flask + Foundry calls are intentionally not claimed as tested here. They are wired to the documented current Foundry project-scoped Conversations/Responses/File Search APIs and must be exercised in the user's WSL development environment next.
