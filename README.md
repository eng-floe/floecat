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

## Project Structure

| Module | Purpose |
|--------|---------|
| proto/ | Protobuf and gRPC interface definitions (ai.floedb.metacat.*) |
| service/ | Quarkus service layer, repositories, and tests | 

## Key Concepts

Metacat maintains a canonical resource hierarchy in an immutable object store and a pointer index for fast lookup.

```bash
tenants/{tenant_id}/catalogs/{catalog_id}/namespaces/{namespace_id}/tables/{table_id}
tenants/{tenant_id}/tables/{table_id}/snapshots/{snapshot_id}
```

Each level (Catalog, Namespace, Table, Snapshot) is represented by:
 - BlobStore → immutable protobuf payloads (catalog.pb, namespace.pb, table.pb, snapshot.pb)
 - PointerStore → lightweight key→blob mappings with versions for fast enumeration and CAS updates.
