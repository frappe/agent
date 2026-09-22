# Agent AI Control Center — Working Snapshot 01

This is an in-progress development snapshot, not a final release.

Implemented in this snapshot:
- `/ai/` graphical control center blueprint
- Foundry asset inventory cache
- Integration registry: MCP, A2A, API/OpenAPI, Webhook, AI Tool
- Integration CRUD API
- API connectivity test
- Outbound webhook test
- Inbound webhook capture/audit endpoint
- Foundry adapter using current `azure-ai-projects` + `DefaultAzureCredential`
- Foundry sync hooks for agents, model deployments, toolboxes and connections
- Agent enable/disable API hook
- Binding registry for Agent ↔ Model/Tool/Integration relationships
- Topology API and first graphical topology view
- SQLite migration patch for AI control tables
- Separate `requirements-ai.txt` so the legacy Agent dependency set is not modified blindly

Intentionally not enabled yet:
- MCP runtime execution client
- A2A runtime execution client
- Foundry standalone Tools asset synchronization from the Assets > Tools surface
- OpenAPI spec editor/validator/publisher wizard
- Webhook action mapping/retry engine
- Credential secret resolver
- Full graphical create/edit workflows for Foundry-managed resources

Validation performed:
- Python compileall: passed for new AI Control modules
- JavaScript syntax check with Node: passed
- Full runtime import test was not executed in this container because the legacy Agent runtime dependencies (for example `redis`) are not installed in the analysis environment.
