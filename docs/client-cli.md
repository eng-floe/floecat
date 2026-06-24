# Client CLI

## Overview

`client-cli/` packages a Picocli-based interactive shell backed by the same gRPC stubs used by the
service. It is the quickest way for developers and operators to explore catalogs, namespaces, tables,
connectors, storage authorities, and query-lifecycle workflows without writing bespoke clients.

The CLI runs as a Quarkus application, depends on generated RPC stubs, and includes helpers for
fully-qualified name parsing (`FQNameParserUtil`) and CSV-like argument parsing for column lists
(`CsvListParserUtil`).

## Architecture & Responsibilities

- **`Shell` (top-level Picocli command)** – Boots an interactive REPL using JLine, configures history
  persistence (`~/.floecat_shell_history`), auto-completion, and command dispatch. Commands share a
  current account context and use injected blocking stubs annotated with `@GrpcClient("floecat")`.
- **`CliCommandExecutor`** – Standalone, embeddable command dispatcher. No Quarkus, no JLine; takes
  gRPC stubs and a `PrintStream`, tokenizes input, and routes to the appropriate `*CliSupport`
  handler. Thread-safe and reusable across calls. See the Embedding section below.
- **Utility parsers** – `FQNameParserUtil` splits catalog.namespace.table strings into `NameRef`s;
  `CsvListParserUtil` converts `k=v` style arguments into Java maps/lists.
- **Display helpers** – The shell formats responses into human-readable tables, summarising
  `MutationMeta`, pagination, and query descriptors.

Responsibilities:

- Provide a REPL for catalog CRUD operations.
- Manage gRPC connections and propagate auth headers (via Quarkus client interceptors).
- Perform minimal validation before sending requests (for example verifying namespace path syntax).

## Public API / Surface Area

The CLI exposes commands documented at runtime via `help`. Highlights:

- `account <UUID>` – Sets `currentAccountId` environment for subsequent commands.
- `account list|get|create|delete` – Manage accounts and resolve display names to IDs.
- `catalogs` / `catalog <subcommand>` – List/create/update/delete catalogs.
- `catalog use <name>` – Sets the default catalog used by `query begin` when inputs omit a catalog.
- `namespaces` / `namespace <subcommand>` – Inspect namespace hierarchies, create nested paths.
- `tables` / `table <subcommand>` – Manage table definitions, specify schema JSON, upstream
  connectors, and properties.
- `views` / `view <subcommand>` – List and manage view definitions.
- `snapshots` / `stats` – Query snapshot lineage, table/column/file statistics, and index artifacts
  (`stats files` lists per-file row/byte counts and per-column metrics; `stats index` lists stored
  sidecar index artifacts for a snapshot).
- `analyze` – Run synchronous table-scoped capture for stats/index generation.
- `constraints` – Manage snapshot-scoped table constraints (`get`, `list`, `put`/`update`/`add`,
  `delete`) using either table FQNs or table IDs.
- `resolve` / `describe` – Exercise DirectoryService for name→ID lookups.
- `query` – Execute the query lease lifecycle: `query begin`, `query renew`, `query end`, `query get`,
  and `query fetch-scan`. `query get` surfaces lease metadata (snapshots, expansions, obligations)
  while `query fetch-scan <query_id> <table_id>` requests the connector-provided `ScanFile` lists for
  an individual table.
- `connectors` / `connector <subcommand>` – Manage connector definitions and reconciliation jobs.
- `storage-authorities` / `storage-authority <subcommand>` – Manage storage credential authorities
  used by the Iceberg REST gateway to vend temporary object-store credentials.

Inputs map directly to RPCs (for example `table create` builds `CreateTableRequest`). The CLI adds
syntactic sugar such as `catalog.ns.table` references and `--props k=v` repeated arguments that fill
`map<string,string>` fields.

## Important Internal Details

- **Account context** – Commands fail fast if `currentAccountId` is empty. The REPL prompts the user to
  run `account <id>` first or set `FLOECAT_ACCOUNT` before launching.
- **Error handling** – Set `FLOECAT_SHELL_DEBUG=true` (or `-Dfloecat.shell.debug=true`) to print full
  stack traces when gRPC calls fail. Otherwise the shell surfaces localized messages from
  `GrpcErrors`.
- **Secret handling** – Secret-bearing connector and storage-authority values must be supplied via
  `--cred-type` / `--cred`, not `--auth k=v`. The service stores those secrets out-of-band and
  only returns redacted or client-safe values.
- **Pagination defaults** – Commands paginate internally in batches of 100 rows. User-facing limits
  are exposed only on ordered result sets such as `snapshots`, `connector jobs`, and ordered stats
  commands.
- **Query display** – `query get` prints the `QueryDescriptor` metadata (snapshots, expansions,
  obligations). Use `query fetch-scan <query_id> <table_id>` to print the data/delete `ScanFile`
  entries returned by connectors via the query lifecycle SPI.

## Data Flow & Lifecycle

```
User command → Picocli parser → Shell subcommand → gRPC stub call
  → Interceptors attach auth headers → Service executes request
  ← Response printed (tables/JSON summaries)
```

Commands run synchronously inside the REPL thread; long-running operations (for example connector
reconciliation) show job IDs that can be polled via `connector job <id>`.

`connector jobs` shows a parent-job summary table by default. Use
`connector jobs --child <parent-job-id>` to render the descendant job tree rooted at that job.
Both commands support `--json` for machine-readable output, while `connector job <id>` remains a
detailed human-readable view and also supports `--json`.

## Configuration & Extensibility

- Build via `make cli`. Run with `make cli-run` (build + run) or `make cli-start` (run only, no
  Maven rebuild — avoids triggering Quarkus live-reload on the service).
- Configure the target endpoint using Quarkus client properties (default `localhost:9100`), or pass
  `--host` / `--port` flags at startup. `FLOECAT_GRPC_HOST` and `FLOECAT_GRPC_PORT` are also
  supported environment overrides.
- Extend commands by adding a new `*CliSupport` handler and wiring it into `CliCommandExecutor`.
- Add new parsing helpers in `client-cli/src/main/java/ai/floedb/floecat/client/cli/util` for custom
  arg shapes.

## Embedding

`CliCommandExecutor` can be used directly in any JVM application without the Quarkus shell. It has
no dependency on JLine, Picocli, or Quarkus runtime — only the gRPC stubs from `floecat-proto`.

```text
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("localhost", 9100)
    .usePlaintext()
    .build();

CliCommandExecutor executor = CliCommandExecutor.fromChannel(channel, System.out);
executor.execute("catalogs");
executor.execute("tables my-catalog.my-ns");
```

`fromChannel` creates all service stubs from a single channel and uses no-op session-state
callbacks. For full session management (e.g. `account set` / `catalog use` persisting across
calls), use the builder directly and supply real `setAccountId` / `setCatalog` consumers.

`execute` never throws — errors are written to the provided `PrintStream` and the method returns
`false`. A single executor instance is thread-safe and may be reused across any number of calls.

## Examples & Scenarios

- **Provisioning a catalog and namespace**

  ```
  account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2
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
    --props iceberg.source=glue
  ```

- **Creating a connector to an upstream Glue Iceberg table via assume role**

  ```
  connector create "call_center" ICEBERG
    "https://glue.us-east-1.amazonaws.com/iceberg/"
    tpcds_iceberg as --dest-ns iceberg --dest-table call_center
    --auth-scheme aws-sigv4
    --cred-type aws-assume-role
    --cred role_arn=arn:aws:iam::123456789012:role/floecat-prod-s3-readonly
    --cred aws.region=us-east-1
    --props iceberg.source=glue
    --props s3.region=us-east-1
  ```

- **Configuring connector reconcile policy**

  ```
  connector update glue-iceberg --policy-enabled true --policy-mode incremental --policy-current
  connector update glue-iceberg --policy-latest-n 5
  connector update glue-iceberg --policy-all
  connector update glue-iceberg --policy-capture stats,index
  connector update glue-iceberg --policy-capture index --policy-default-cols explicit-only
  ```

- **Triggering a connector to read an upstream table**

  ```
  connector trigger glue-iceberg --incremental --current --mode metadata-only
  connector trigger glue-iceberg --incremental --current --mode metadata-and-capture
  connector trigger glue-iceberg --full --current --mode metadata-and-capture --capture stats
  connector trigger glue-iceberg --full --current --mode metadata-and-capture --capture index
  ```

  For capture modes, trigger-time `--capture` flags are optional. If omitted, the run inherits the
  connector's persisted auto-capture policy when present, otherwise it falls back to the default
  stats capture policy. Trigger-time capture flags override the connector default for that run.

- **Creating a storage authority for Iceberg REST credential vending**

  ```
  storage-authority create localstack-demo \
    --location-prefix s3://warehouse/ \
    --region us-east-1 \
    --endpoint http://localhost:4566 \
    --path-style-access true \
    --assume-role-arn arn:aws:iam::123456789012:role/floecat-vended-storage \
    --cred-type aws \
    --cred access_key_id=test \
    --cred secret_access_key=test
  ```

  See the storage authorities guide below in Cross-References for dedicated
  storage-authority examples, including cross-account assume-role vending.

- **Managing snapshot constraints**

  ```
  constraints put demo.sales.users --snapshot 42 --file /tmp/users_constraints.json
  constraints put demo.sales.users --snapshot 42 --file /tmp/users_constraints.json --idempotency my-op-123
  constraints update demo.sales.users --snapshot 42 --file /tmp/users_constraints_patch.json
  constraints update demo.sales.users --snapshot 42 --file /tmp/users_constraints_patch.json --version 3
  constraints add demo.sales.users --snapshot 42 --file /tmp/users_constraints.json
  constraints add-one demo.sales.users --snapshot 42 --file /tmp/users_constraint_pk.json
  constraints add-pk demo.sales.users pk_users id --snapshot 42
  constraints add-unique demo.sales.users uq_users_email email --snapshot 42
  constraints add-not-null demo.sales.users nn_users_email email --snapshot 42
  constraints add-check demo.sales.users chk_users_age "age > 0" --snapshot 42
  constraints add-fk demo.sales.users fk_users_org org_id demo.sales.orgs id --snapshot 42
  constraints get demo.sales.users --snapshot 42 --json
  constraints list demo.sales.users --limit 50
  constraints delete demo.sales.users --snapshot 42
  constraints delete-one demo.sales.users pk_users --snapshot 42
  ```

  If `--snapshot` is omitted in constraints commands, the CLI resolves and uses the current
  snapshot.
  For `constraints put`/`add`/`update`, the command target table + snapshot is authoritative; any
  payload identity fields are normalized to match that target before write.
  Semantics:
  `put` replaces the full bundle, `update` does a server-side merge by constraint name and
  shallow-merges bundle `properties` (incoming keys override existing keys), and `add` does a
  server-side append-only mutation (errors on duplicates). `update`/`add` support optional
  `--etag` / `--version` preconditions.
  Without a precondition, `update` and `add` create the snapshot bundle if it does not exist.
  For atomic single-constraint mutations under concurrency, prefer `add-one` / `delete-one`.
  `put` uses `--idempotency <key>` for safe-to-retry full replaces (same key guarantees the same
  outcome); `update` and `add` use `--etag`/`--version` for conditional mutations that prevent
  lost updates when multiple writers race on the same snapshot.

## Cross-References

- [`docs/proto.md`](proto.md)
- [`docs/service.md`](service.md)
