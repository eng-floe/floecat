# Client CLI

## Overview
`client-cli/` packages a Picocli-based interactive shell backed by the same gRPC stubs used by the
service. It is the quickest way for developers and operators to explore catalogs, namespaces, tables,
connectors, and planning workflows without writing bespoke clients.

The CLI runs as a Quarkus application, depends on generated RPC stubs, and includes helpers for
fully-qualified name parsing (`FQNameParserUtil`) and CSV-like argument parsing for column lists
(`CsvListParserUtil`).

## Architecture & Responsibilities
- **`Shell` (top-level Picocli command)** – Boots an interactive REPL using JLine, configures history
  persistence (`~/.metacat_shell_history`), auto-completion, and command dispatch. Commands share a
  current tenant context and use injected blocking stubs annotated with `@GrpcClient("metacat")`.
- **Utility parsers** – `FQNameParserUtil` splits catalog.namespace.table strings into `NameRef`s;
  `CsvListParserUtil` converts `k=v` style arguments into Java maps/lists.
- **Display helpers** – The shell formats responses into human-readable tables, summarising
  `MutationMeta`, pagination, and plan descriptors.

Responsibilities:
- Provide a REPL for catalog CRUD operations.
- Manage gRPC connections and propagate `PrincipalContext` headers (via Quarkus client interceptors).
- Perform minimal validation before sending requests (for example verifying namespace path syntax).

## Public API / Surface Area
The CLI exposes commands documented at runtime via `help`. Highlights:
- `tenant <UUID>` – Sets `currentTenantId` environment for subsequent commands.
- `catalogs` / `catalog <subcommand>` – List/create/update/delete catalogs.
- `namespaces` / `namespace <subcommand>` – Inspect namespace hierarchies, create nested paths.
- `tables` / `table <subcommand>` – Manage table definitions, specify schema JSON, upstream
  connectors, and properties.
- `snapshots` / `stats` – Query snapshot lineage and table/column/file statistics (`stats files`
  lists per-file row/byte counts and per-column metrics).
- `resolve` / `describe` – Exercise DirectoryService for name→ID lookups.
- `plan` – Execute the plan lifecycle: `plan begin`, `plan renew`, `plan get`, `plan end`, and view
  plan contents (snapshot pins, obligations, file lists).
- `connectors` / `connector <subcommand>` – Manage connector definitions and reconciliation jobs.

Inputs map directly to RPCs (for example `table create` builds `CreateTableRequest`). The CLI adds
syntactic sugar such as `catalog.ns.table` references and `--props k=v` repeated arguments that fill
`map<string,string>` fields.

## Important Internal Details
- **Tenant context** – Commands fail fast if `currentTenantId` is empty. The REPL prompts the user to
  run `tenant <id>` first or set `METACAT_TENANT` before launching.
- **Error handling** – Set `METACAT_SHELL_DEBUG=true` (or `-Dmetacat.shell.debug=true`) to print full
  stack traces when gRPC calls fail. Otherwise the shell surfaces localized messages from
  `GrpcErrors`.
- **Pagination defaults** – `DEFAULT_PAGE_SIZE` is 1000; commands like `catalogs` accept
  `--page-size` overrides and persist `next_page_token` transparently.
- **Plan display** – `plan get` prints `PlanDescriptor` plus `PlanFile` lists (data/delete files)
  returned by connectors via the planning SPI.

## Data Flow & Lifecycle
```
User command → Picocli parser → Shell subcommand → gRPC stub call
  → Interceptors attach tenant/principal headers → Service executes request
  ← Response printed (tables/JSON summaries)
```
Commands run synchronously inside the REPL thread; long-running operations (for example connector
reconciliation) show job IDs that can be polled via `connector job <id>`.

## Configuration & Extensibility
- Build via `make cli` or run directly with `make cli-run` (delegates to `quarkus:dev`).
- Configure the target endpoint using Quarkus client properties (default `localhost:9100`).
- Extend commands by adding nested `@Command` classes under `Shell`. Because the CLI already injects
  every gRPC stub, new commands only need to format inputs and call the stub.
- Add new parsing helpers in `client-cli/src/main/java/ai/floedb/metacat/client/cli/util` for custom
  arg shapes.

## Examples & Scenarios
- **Provisioning a namespace**
  ```
  tenant 31a47986-efaf-35f5-b810-09ba18ca81d2
  namespace create demo.sales --desc "Sales sandbox" --props owner=analyst
  ```
- **Creating a table referencing an upstream connector**
  ```
  table create demo.sales.events \
    --schema @schema/events.json \
    --up-connector glue-catalog --up-ns demo.sales --up-table events \
    --format ICEBERG
  ```
- **Planning a query read**
  ```
  plan begin table demo.sales.events --snapshot current --ttl 120
  plan get <plan-id>
  plan end <plan-id> --commit
  ```

## Cross-References
- RPC contract details: [`docs/proto.md`](proto.md)
- Service behavior enforced by catalog/table/plan implementations: [`docs/service.md`](service.md)
