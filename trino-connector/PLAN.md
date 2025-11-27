# Trino Connector Enablement Plan (API Draft)

This is a proposed API surface to let the Metacat-backed Trino connector push down projections and
predicates, fetch typed schemas, resolve views, and read files via presigned URLs. Nothing here is
implemented or wired into the build; it is a design scaffold so we can iterate safely.

## New/extended gRPC (draft)

- **PlanningEx** (new service, avoids breaking current Planning):
  - `BeginPlanEx` request adds:
    - `repeated string required_columns`
    - `map<string,string> predicates` (e.g., simple column→expression; evolve to a structured
      expression tree)
    - `bool include_schema` to return typed schema used for pushdown.
  - Response adds:
    - `SchemaDescriptor schema` (columns with names, types, field IDs, partition info).
    - `repeated PlanFile data_files`, `repeated PlanFile delete_files` (same as Planning, but
      filtered by predicates).
- **ViewServiceEx** (optional, or reuse existing ViewService with extensions):
  - `GetViewExpanded(ResourceId view_id)` → canonical SQL + base table IDs (mirrors
    `ExpansionMap` semantics).
- **ObjectAccessService**:
  - `PresignFiles` takes `repeated string file_path` and returns `repeated string presigned_url`
    (or temp credentials) so Trino can read files directly.

## Connector wiring (to implement once APIs land)

- Pass Trino projections/predicates into `BeginPlanEx`; build splits from filtered PlanFiles.
- Use `SchemaDescriptor` to build real ColumnHandles/ColumnMetadata.
- For views, resolve via `GetViewExpanded` or `BeginPlanEx.expansion`.
- Before creating page sources, call `PresignFiles` for each file in the split and feed signed URIs
  to Trino readers.

## Open questions

- Predicate representation: start with simple column→expression strings or a minimal AST?
- Auth propagation: how to pass principal/session info from Trino to presign service?
- Delta delete vectors: needed only if serving Delta through Metacat plans; can defer if Trino uses
  native Delta connector.
