# Metacat

Metacat is a lightweight catalog-of-catalogs for the modern data lakehouse.
It federates metadata from Delta and Iceberg catalogs into a unified, gRPC-based service for discovery, access control, and query planning.

## Build and Run

Requires Java 17+, Maven, and Make.

### Quick Start

```bash
# build protobuf stubs + service
make build

# run Quarkus dev server (foreground)
make run

# start Quarkus dev in background
make start

# stop background process
make stop

# tail logs
make logs
```

## Testing

```bash
# run unit + integration tests
make test
```

You can also run per-module tests:

```bash
make unit-test
make integration-test
```

## Examples

The Metacat APIs can be accessed with a gRPC client, for example, `grpcurl`. To do so, start the server with `make run`,
and then issue the following requests:

### ListCatalogs

```bash
grpcurl -plaintext -d '{}' localhost:9100 ai.floedb.metacat.catalog.CatalogService/ListCatalogs
```

### ListNamespaces

```bash
grpcurl -plaintext \
  -d '{
    "catalog_id": {
      "tenant_id": "31a47986-efaf-35f5-b810-09ba18ca81d2",
      "id": "109c1761-323a-3f72-83da-ff4f89c3b581",
      "kind": "RK_CATALOG"
    }
  }' \
  localhost:9100 ai.floedb.metacat.catalog.NamespaceService/ListNamespaces
```

### ListTables

```bash
grpcurl -plaintext \
  -d '{
    "namespace_id": {
      "tenant_id": "31a47986-efaf-35f5-b810-09ba18ca81d2",
      "id": "86853a0f-a999-3c72-9a81-6dc66d1923a2",
      "kind": "RK_NAMESPACE"
    }
  }' \
  localhost:9100 ai.floedb.metacat.catalog.TableService/ListTables
```

This early version of Metacat is seeded with test catalogs, namespaces and tables for testing.

## Command-Line Interface

There is also a simple interactive shell to run commands against the service. To build the CLI:

```
make cli
```

To build and run the CLI:

```
make cli-run
```

Once built, run it without rebuilding with:

```
java --enable-native-access=ALL-UNNAMED -jar client-cli/target/quarkus-app/quarkus-run.jar
```

The supported shell commands and options are:

```
Commands:
tenant <id>
catalogs
catalog create <display_name> [--desc <text>] [--connector <id>] [--policy <id>] [--props k=v ...]
catalog get <display_name|id>
catalog update <display_name|id> [--display <name>] [--desc <text>] [--connector <id>] [--policy <id>] [--props k=v ...] [--etag <etag>]
catalog delete <display_name|id> [--require-empty] [--etag <etag>]
namespaces (<catalog | catalog.ns[.ns...]> | <UUID>) [--id <UUID>] [--prefix P] [--recursive]
namespace create <catalog.ns[.ns...]> [--desc <text>] [--props k=v ...] [--policy <id>]
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
table update <id|fq> [--catalog <catalogName|id>] [--namespace <namespaceFQ|id>] [--name <name>] [--desc <text>] [--root <uri>] [--schema <json>] [--parts k1,k2,...] [--format ICEBERG|DELTA] [--props k=v ...] [--etag <etag>]
    [--up-connector <id|name>] [--up-ns <a.b[.c]>] [--up-table <name>]
table delete <id|fq> [--purge-stats] [--purge-snaps] [--etag <etag>]
resolve table <fq> | resolve view <fq> | resolve catalog <name> | resolve namespace <fq>
describe table <fq>
snapshots <tableFQ>
stats table <tableFQ> [--snapshot <id>|--current] (defaults to --current)
stats columns <tableFQ> [--snapshot <id>|--current] [--limit N] defaults to --current
plan begin [--ttl <seconds>] [--as-of-default <timestamp>] (table <catalog.ns....table> [--snapshot <id|current>] [--as-of <timestamp>] | table-id <uuid> [--snapshot <id|current>] [--as-of <timestamp>] | view-id <uuid> | namespace <catalog.ns[.ns...]>)+
plan renew <plan_id> [--ttl <seconds>]
plan end <plan_id> [--commit|--abort]
plan get <plan_id>
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
    [--dest-tenant <tenant>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
    [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
    [--policy-enabled true|false] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...] [--etag <etag>]
connector delete <display_name|id>  [--etag <etag>]
connector validate <kind> <uri>
    [--dest-tenant <tenant>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
    [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
    [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...]
connector trigger <display_name|id> [--full]
connector job <jobId>
help
quit
```

A tenant ID must be configured in the shell before the commands can be used. To use the default seeded tenant, enter into the metacat CLI:

```
tenant 31a47986-efaf-35f5-b810-09ba18ca81d2
```

## Project Structure

| Module | Purpose |
|--------|---------|
| proto/ | Protobuf and gRPC interface definitions (ai.floedb.metacat.*) |
| service/ | Quarkus service layer, repositories, and tests |
| connectors-spi | Generic interfaces that connectors that pull statistics from upstream repositories must implement |
| connectors-iceberg | A connector to an upstream AWS Glue repository of Iceberg tables |
| client-cli | The interactive shell |
| reconciler | The service that schedules and runs connectors |
| types | Generic type handling for Iceberg and Delta parquet tables |
| storage-spi | Generic interfaces that pointer and blob stores that persist metacat data must implement |
| storage-memory | In-memory pointer and blob stores |
| storage-aws | DynamoDB pointer store and S3 blob store implementations |

## Key Concepts

Metacat maintains a canonical resource hierarchy in an immutable object store and a pointer index for fast lookup.

Blobs are named with the following prefixes:

```bash
/tenants/{tenant_id}
/tenants/{tenant_id}/catalogs/{catalog_id}
/tenants/{tenant_id}/namespaces/{namespace_id}
/tenants/{tenant_id}/tables/{table_id}/snapshots/{snapshot_id}
/tenants/{tenant_id}/tables/{table_id}/snapshots/{snapshot_id}/stats/(table|column/{column_id})
...
```

Pointers to the blobs capture the hierarchical relationships between tenants, catalogs, namespaces and tables:

```bash
/tenants/{tenant_id}/by-id/{tenant_id}
/tenants/{tenant_id}/by-name/{tenant_name}
/tenants/{tenant_id}/catalogs/by-id/{catalog_id}
/tenants/{tenant_id}/catalogs/by-name/{catalog_name}
/tenants/{tenant_id}/catalogs/{catalog_id}/namespaces/by-id/{namespace-id}
/tenants/{tenant_id}/catalogs/{catalog_name}/namespaces/by-name/{namespace_name}
...
```

This arrangement allows fast name to id resolution and listing.

Each level (Catalog, Namespace, Table, Snapshot) is represented by:

- BlobStore → immutable protobuf payloads (catalog.pb, namespace.pb, table.pb, snapshot.pb,...)
- PointerStore → lightweight key→blob mappings with versions for fast enumeration and CAS updates.

There are two pointer/blob store implementations right now. The first is a simple in memory pointer and blob store for testing. The second uses AWS DynamoDB as the pointer store and AWS S3 as the blob store. The choice of pointer and blob store can be configured in the `application.properties` file.

## Contributing

This repo enforces branch protections and CI, so please follow these steps:

1. **Fork or branch**
   - Internal contributors: create a feature branch from `main`.
   - External contributors: fork the repo.

2. **Develop**
   - Make changes and keep commits focused.
   - Add unit/integration tests.
   - Format Java code locally:

     ```bash
     make fmt
     ```

   - Run the full test suite:

     ```bash
     make verify
     ```

3. **Open a Pull Request**
   - Target branch: `main`
   - CI will run:
     - Build & tests (`make verify`)
     - Formatting check (`make fmt`)
   - PRs require at least one approval and all checks to pass.
   - Merge method: **Squash merge**.

4. **Commit messages**
   - Prefer conventional commits (e.g., `feat: ...`, `fix: ...`, `chore: ...`, `docs: ...`, `test: ...`).

5. **Code style**
   - Java: Google Java Format (enforced by CI).
   - Use `.editorconfig` defaults for whitespace/newlines.

6. **Security**
   - Do not include secrets in code or tests.
