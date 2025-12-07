# Client CLI

## Overview

`client-cli/` packages a Picocli-based interactive shell backed by the same gRPC stubs used by the
service. It is the quickest way for developers and operators to explore catalogs, namespaces, tables,
connectors, and query-lifecycle workflows without writing bespoke clients.

The CLI runs as a Quarkus application, depends on generated RPC stubs, and includes helpers for
fully-qualified name parsing (`FQNameParserUtil`) and CSV-like argument parsing for column lists
(`CsvListParserUtil`).

## Architecture & Responsibilities

- **`Shell` (top-level Picocli command)** – Boots an interactive REPL using JLine, configures history
  persistence (`~/.floecat_shell_history`), auto-completion, and command dispatch. Commands share a
  current tenant context and use injected blocking stubs annotated with `@GrpcClient("floecat")`.
- **Utility parsers** – `FQNameParserUtil` splits catalog.namespace.table strings into `NameRef`s;
  `CsvListParserUtil` converts `k=v` style arguments into Java maps/lists.
- **Display helpers** – The shell formats responses into human-readable tables, summarising
  `MutationMeta`, pagination, and query descriptors.

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
- `query` – Execute the query lease lifecycle: `query begin`, `query renew`, `query end`, `query get`,
  and `query fetch-scan`. `query get` surfaces lease metadata (snapshots, expansions, obligations)
  while `query fetch-scan <query_id> <table_id>` requests the connector-provided `ScanFile` lists for
  an individual table.
- `connectors` / `connector <subcommand>` – Manage connector definitions and reconciliation jobs.

Inputs map directly to RPCs (for example `table create` builds `CreateTableRequest`). The CLI adds
syntactic sugar such as `catalog.ns.table` references and `--props k=v` repeated arguments that fill
`map<string,string>` fields.

## Important Internal Details

- **Tenant context** – Commands fail fast if `currentTenantId` is empty. The REPL prompts the user to
  run `tenant <id>` first or set `FLOECAT_TENANT` before launching.
- **Error handling** – Set `FLOECAT_SHELL_DEBUG=true` (or `-Dfloecat.shell.debug=true`) to print full
  stack traces when gRPC calls fail. Otherwise the shell surfaces localized messages from
  `GrpcErrors`.
- **Pagination defaults** – `DEFAULT_PAGE_SIZE` is 1000; commands like `catalogs` accept
  `--page-size` overrides and persist `next_page_token` transparently.
- **Query display** – `query get` prints the `QueryDescriptor` metadata (snapshots, expansions,
  obligations). Use `query fetch-scan <query_id> <table_id>` to print the data/delete `ScanFile`
  entries returned by connectors via the query lifecycle SPI.

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
- Add new parsing helpers in `client-cli/src/main/java/ai/floedb/floecat/client/cli/util` for custom
  arg shapes.

## Examples & Scenarios

- **Provisioning a catalog and namespace**

  ```
  tenant 31a47986-efaf-35f5-b810-09ba18ca81d2
  catalog create demo --desc "Demo catalog"
  namespace create demo.sales --desc "Sales sandbox" --props owner=analyst
  ```

- **Creating a connector to an upstream Glue Iceberg table**

  ```
  connector create "glue-iceberg" ICEBERG
    "https://glue.us-east-1.amazonaws.com/iceberg/"
    tpcds_iceberg demo --auth-scheme aws-sigv4
    --source-table call_center
    --dest-ns sales --dest-table my_call_center
  ```

- **Triggering a connector to read an upstream table**

  ```
  connector trigger glue-iceberg
  ```

## Cross-References

- RPC contract details: [`docs/proto.md`](proto.md)
- Service behavior enforced by catalog/table/query implementations: [`docs/service.md`](service.md)
