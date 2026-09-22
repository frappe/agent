# Agent AI Control Center

Graphical control plane added to the existing Frappe Press Agent runtime.

## Resource domains

- Foundry Assets: Agents, Models/Deployments, Tools, Toolboxes, Connections
- Integration Registry: MCP, A2A, API/OpenAPI, Webhook, AI Tool
- Operations: test, sync, execution audit, health/observability

## Source of truth

- Microsoft Foundry remains source of truth for Foundry-managed assets.
- Agent stores local integration definitions, links, execution observations, and cached inventory.
- Secret values are not stored in integration rows; only `secret_ref` is stored.

## UI

`/ai/`

## API

- `GET /ai/api/overview`
- `GET|POST /ai/api/integrations`
- `GET|PATCH|DELETE /ai/api/integrations/<id>`
- `POST /ai/api/integrations/<id>/test`
- `GET /ai/api/assets`
- `GET /ai/api/foundry/capabilities`
- `POST /ai/api/foundry/sync/deployments`

## Runtime status in this patch

- GUI shell: implemented
- Integration registry CRUD: implemented
- API connectivity test: implemented
- Outbound webhook test: implemented
- Foundry deployment sync adapter: implemented (requires `requirements-ai.txt` and identity)
- MCP protocol execution: next patch, via official MCP client
- A2A protocol execution: next patch, via current A2A client/protocol support
- Foundry agents/tools/toolboxes synchronization: next patch
- Inbound webhook routing and trigger actions: next patch
