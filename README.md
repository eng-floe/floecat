# Floecat

Floecat is a catalog-of-catalogs for modern data lakehouses. It federates metadata harvested from
Delta Lake and Iceberg sources, stores the canonical view in an append-only blob store, and exposes
the resulting hierarchy over gRPC for discovery, authorization, and query planning.

The repository is purposely modular. Each top-level directory corresponds to an operational
component (service runtime, connector packages, storage backends, CLI, etc.). Detailed,
component-specific documentation lives under [`docs/`](docs); see the [Documentation Map](#documentation-map).

## System Overview

Floecat tracks a resource graph anchored at Accounts and spanning Catalogs, Namespaces, Tables,
Views, Snapshots, statistics, and query-lifecycle artifacts.

```
Account
 └── Catalog (logical catalog-of-catalogs)
      └── Namespace (hierarchical path)
           ├── Table (Iceberg/Delta metadata, upstream reference)
           │    └── Snapshot (immutable upstream checkpoints)
           └── View (stored definition)
```

Two storage primitives underpin every service:

- **BlobStore** – immutable protobuf payloads such as `catalog.pb`, `table.pb`,
  `snapshots/{snapshot_id}/stats/table.pb`. Blobs are content-addressed via SHA256 ETAGs.
- **PointerStore** – versioned key→blob indirection to support atomic compare-and-set (CAS),
  hierarchical listing, and name→ID lookups. Keys use deterministic prefixes:
  `/accounts/{account_id}/catalogs/by-name/{name}`, `/tables/{table_id}/snapshots/{id}`, etc.

The gRPC service (Quarkus) enforces tenancy, authorization, and idempotency while orchestrating
connectors that ingest upstream metadata, reconciling it into the canonical blob/pointer stores, and
serving execution-ready scan bundles.

## Component Architecture

The following modules compose the system (see linked docs for deep dives):

| Component | Responsibility |
|-----------|----------------|
| [`proto/`](docs/proto.md) | All protobuf/gRPC contracts (catalog, query lifecycle, execution scans, connectors, statistics, types). |
| [`service/`](docs/service.md) | Quarkus runtime, resource repositories, query lifecycle service, GC, security, metrics. |
| [`client-cli/`](docs/client-cli.md) | Interactive shell for humans; exercises every public RPC. |
| [`core/connectors/spi/`](docs/connectors-spi.md) | Connector interfaces, stats engines, NDV helpers, auth shims. |
| [`connectors/catalogs/iceberg/`](docs/connectors-iceberg.md) | Iceberg REST + AWS Glue connector implementation. |
| [`connectors/catalogs/delta/`](docs/connectors-delta.md) | Unity Catalog/Delta Lake connector using Delta Kernel + Databricks APIs. |
| [`core/connectors/common/`](docs/connectors-common.md) | Shared connector utilities (Parquet stats, NDV sketches, planners). |
| [`reconciler/`](docs/reconciler.md) | Connector scheduler/worker, reconciliation orchestration, job store. |
| [`core/storage-spi/`](docs/storage-spi.md) | Blob/pointer persistence contracts shared by service and GC. |
| [`storage/memory/`](docs/storage-memory.md) | In-memory dev/test stores (CAS semantics maintained). |
| [`storage/aws/`](docs/storage-aws.md) | Production DynamoDB pointer store + S3 blob store. |
| [`types/`](docs/types.md) | Logical type system utilities, coercions, min/max encoding. |
| [`extensions/builtin/`](docs/builtin-catalog.md) | Plugin architecture for engine-specific builtin catalogs (functions, operators, types, etc.). |
| [`log/`](docs/log.md) | Runtime log directory layout (service log + audit channel). |

### Data & Control Flow

1. **Connectors** (Delta/Iceberg) enumerate upstream namespaces, tables, snapshots, and file-level
   stats via the shared SPI.
2. The **Reconciler** schedules connector runs, materializes local Tables/Snapshots/Stats through
   repository APIs, and records incremental NDV, histograms, and scan manifests.
3. The **Service** exposes CRUD RPCs for catalogs/namespaces/tables/views, plus query-lifecycle and
   statistics APIs. Requests traverse interceptors that inject `PrincipalContext`, correlation IDs,
   and optional query leases before hitting service implementations.
4. **Repositories** translate RPCs into pointer/blob mutations, enforce optimistic concurrency, and
   update idempotency records.
5. **Query lifecycle RPCs** hand planners lease descriptors (snapshot pins, obligations) plus any
   connector-provided scan metadata needed before execution.

## Resource Model & Storage Layout

Floecat stores every entity twice: the immutable protobuf payload lives in a BlobStore while a
PointerStore entry (with versions) exposes hierarchical lookup and CAS updates. This design keeps
history in blobs while enabling fast name-based resolution via pointers.

Blobs follow deterministic prefixes:

```
/accounts/{account_id}
/accounts/{account_id}/catalogs/{catalog_id}
/accounts/{account_id}/namespaces/{namespace_id}
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id}
/accounts/{account_id}/tables/{table_id}/snapshots/{snapshot_id}/stats/(table|column/{column_id})
```

Pointers capture hierarchy and name lookups:

```
/accounts/{account_id}/by-id/{account_id}
/accounts/{account_id}/by-name/{account_name}
/accounts/{account_id}/catalogs/by-id/{catalog_id}
/accounts/{account_id}/catalogs/by-name/{catalog_name}
/accounts/{account_id}/catalogs/{catalog_id}/namespaces/by-id/{namespace_id}
/accounts/{account_id}/catalogs/{catalog_id}/namespaces/by-name/{namespace_name}
```

Each pointer carries a monotonically increasing version; repositories use compare-and-set to enforce
idempotency and optimistic concurrency. Two storage implementations ship with the repo:

- **Memory** – `InMemoryPointerStore` + `InMemoryBlobStore` (default for `make run`).
- **AWS** – DynamoDB pointer table + S3 blob bucket (see `service/src/main/resources/application.properties`).

## Build, Run, and Test

Requirements: Java 25+, Maven, Make.

```bash
# Generate protobuf stubs and build every module
make build

# Run the Quarkus service in foreground (hot reload)
make run

# Background the service / shut it down
make start
make stop

# Tail structured service + audit logs
make logs

# Run service wired to LocalStack (DynamoDB + S3 backends)
make run-localstack-localstack

# Run service wired to LocalStack upstream + real AWS catalog
make run-localstack-aws

# Run service wired to real AWS upstream + LocalStack catalog
make run-aws-localstack

# Run service wired to real AWS upstream + real AWS catalog
make run-aws-aws

# Run REST gateway (foreground, in-memory) or alongside the service
make run-rest
make run-all

# Start/stop the LocalStack container (if you launched it via the Make targets)
make localstack-up
make localstack-down
```

Seed data is enabled by default (`floecat.seed.enabled=true`); the service starts with a demo account,
catalogs, namespaces, tables, and snapshots.

### Docker Compose Modes

Docker build, compose modes, and AWS credential options live in [`docs/docker.md`](docs/docker.md).

Testing:

```bash
# Unit + integration tests across modules (in-memory)
make test

# Run full test suite against LocalStack (upstream + catalog)
make test-localstack

# Only run unit or integration suites
make unit-test
make integration-test
```

### Builtin Catalog Validator

Validate bundled or bespoke builtin catalog protobufs without running the service:

```bash
mvn -pl tools/builtin-validator package
java -jar tools/builtin-validator/target/builtin-validator.jar \
  service/src/main/resources/builtins/floe-demo.pbtxt
```

Flags:

- `--json` – emit machine-readable output (for CI or scripting).
- `--strict` – fail the run when warnings are present (warnings are currently reserved for future checks).

## Accessing the APIs

Use any gRPC client (for example `grpcurl`) once the service listens on `localhost:9100`.

```bash
grpcurl -plaintext -d '{}' \
  localhost:9100 ai.floedb.floecat.catalog.CatalogService/ListCatalogs

grpcurl -plaintext -d '{
  "catalog_id": {"account_id":"5eaa9cd5-7d08-3750-9457-cfe800b0b9d2",
                 "id":"109c1761-323a-3f72-83da-ff4f89c3b581","kind":"RK_CATALOG"}
}' localhost:9100 ai.floedb.floecat.catalog.NamespaceService/ListNamespaces

grpcurl -plaintext -d '{
  "namespace_id": {"account_id":"5eaa9cd5-7d08-3750-9457-cfe800b0b9d2",
                   "id":"86853a0f-a999-3c72-9a81-6dc66d1923a2","kind":"RK_NAMESPACE"}
}' localhost:9100 ai.floedb.floecat.catalog.TableService/ListTables
```

The [`client-cli`](docs/client-cli.md) module provides an interactive shell with auto-completion and
context-aware rendering. After building it (`make cli` or `make cli-run`), launch with:

```bash
java --enable-native-access=ALL-UNNAMED \
  -jar client-cli/target/quarkus-app/quarkus-run.jar
```

Set the account context first:

```
account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2
```

Then explore `catalog`, `namespace`, `table`, `connector`, and `query` commands. The CLI exercises
the same gRPC surface described in [`docs/service.md`](docs/service.md).

### CLI Command Reference

The interactive shell surfaces most service capabilities directly. Full command list:

```
Commands:
account <id>
catalogs
catalog create <display_name> [--desc <text>] [--connector <id>] [--policy <id>] [--props k=v ...]
catalog get <display_name|id>
catalog update <display_name|id> [--display <name>] [--desc <text>] [--connector <id>] [--policy <id>] [--props k=v ...] [--etag <etag>]
catalog delete <display_name|id> [--require-empty] [--etag <etag>]
namespaces (<catalog | catalog.ns[.ns...]> | <UUID>) [--id <UUID>] [--prefix P] [--recursive]
namespace create <catalog.ns[.ns...]> [--desc <text>] [--props k=v ...]
namespace get <id | catalog.ns[.ns...]>
namespace update <id|catalog.ns[.ns...]>
    [--display <name>] [--desc <text>]
    [--policy <ref>] [--props k=v ...]
    [--path a.b[.c]] [--catalog <id|name>]
    [--etag <etag>]
namespace delete <id|fq> [--require-empty] [--etag <etag>]
tables <catalog.ns[.ns...][.prefix]>
table create <catalog.ns[.ns...].name> [--desc <text>] [--root <uri>] [--schema <json>] [--parts k1,k2,...] [--format ICEBERG|DELTA] [--props k=v ...]
    [--up-connector <id|name>] [--up-ns <a.b[.c]>] [--up-table <name>]
table get <id|catalog.ns[.ns...].table>
table update <id|fq> [--catalog <catalogName|id>] [--namespace <namespaceFQ|id>] [--name <name>] [--desc <text>]
    [--root <uri>] [--schema <json>] [--parts k=v ...] [--format ICEBERG|DELTA] [--props k=v ...] [--etag <etag>]

snapshots <catalog.ns[.ns...].table>
snapshots get <table> <snapshot_id|current>
snapshots delete <table> <snapshot_id>

stats table <catalog.ns[.ns...].table> [--snapshot <id|current>]
stats column <catalog.ns[.ns...].table> [--snapshot <id|current>] [--column <name>]

resolve <catalog.ns[.ns...].table>
describe <catalog.ns[.ns...].table>

query begin [--ttl <seconds>] [--as-of-default <timestamp>]
    (table <catalog.ns....table> [--snapshot <id|current>] [--as-of <timestamp>]
     | table-id <uuid> [--snapshot <id|current>] [--as-of <timestamp>]
     | view-id <uuid>
     | namespace <catalog.ns[.ns...]>)+
query renew <query_id> [--ttl <seconds>]
query end <query_id> [--commit|--abort]
query get <query_id>
query fetch-scan <query_id> <table_id>

connectors
connector list [--kind <KIND>] [--page-size <N>]
connector get <display_name|id>
connector create <display_name> <source_type (ICEBERG|DELTA|GLUE|UNITY)> <uri> <source_namespace (a[.b[.c]...])> <destination_catalog (name)>
    [--source-table <name>] [--source-cols c1,#id2,...]
    [--dest-ns <a.b[.c]>] [--dest-table <name>]
    [--desc <text>] [--auth-scheme <scheme>] [--auth k=v ...]
    [--head k=v ...] [--secret <ref>]
    [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...]
connector update <display_name|id> [--display <name>] [--kind <kind>] [--uri <uri>]
    [--dest-account <account>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
    [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
    [--policy-enabled true|false] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...] [--etag <etag>]
connector delete <display_name|id>  [--etag <etag>]
connector validate <kind> <uri>
    [--dest-account <account>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
    [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
    [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...]
connector trigger <display_name|id> [--full]
connector job <jobId>

help
quit
```

## Documentation Map

All component references, extension points, APIs, and data-flow diagrams are captured under `docs/`.

```
docs/
├── builtin-catalog.md
├── client-cli.md
├── connectors-common.md
├── connectors-delta.md
├── connectors-iceberg.md
├── connectors-spi.md
├── docker.md
├── iceberg-rest-gateway.md
├── log.md
├── metadata-graph.md
├── reconciler.md
├── proto.md
├── service.md
├── storage-spi.md
├── storage-memory.md
├── storage-aws.md
├── system-objects.md
├── system-scans.md
└── types.md
```

Cross-links between files describe how modules interact (for example, the scan bundle assembly
section in `service.md` references the type system in `types.md` and the connector SPI in
`connectors-spi.md`).

## Contributing

Floecat enforces branch protections and CI. Preferred workflow:

1. **Fork or branch** – Create a feature branch from `main` (internal) or fork the repo (external).
2. **Develop** – Keep commits focused, add/extend tests, and run `make fmt` for Google Java Format.
   Execute `make verify` (build + unit/integration tests) before pushing.
3. **Open a PR** – Target `main`. CI enforces formatting and `make verify`. PRs require green checks
   and at least one approval. Merge via squash.

Follow conventional commits (`feat:`, `fix:`, etc.) and avoid embedding secrets. CI enforces Google
Java Format via the Spotify `fmt` plugin. Use `.editorconfig` for whitespace settings.

This project is licensed under the Apache License, Version 2.0.

By submitting a pull request, you represent that you have the right to license your
contribution to Yellowbrick Data, Inc. and the Apache Software Foundation, and you
agree that your contribution will be licensed under the Apache License, Version 2.0.

## Observability & Operations

- **Logging** – JSON console logs plus rotating files under `log/`. Audit logs route gRPC request
  summaries to `log/audit.json`; see [`docs/log.md`](docs/log.md).
- **Metrics** – Micrometer/Prometheus exporters summarise pointer/blob IO and reconciliation stats.
- **Tracing** – OpenTelemetry (TraceContext propagator) is enabled; export pipelines can be wired by
  overriding `quarkus.otel.traces.exporter`.

Configuration flags are documented per module (for example storage backend selection in
[`docs/storage-spi.md`](docs/storage-spi.md), GC cadence in [`docs/service.md`](docs/service.md)).
