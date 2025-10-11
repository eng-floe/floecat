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
grpcurl -plaintext -d '{}' localhost:9100 ai.floedb.metacat.catalog.ResourceAccess/ListCatalogs
```

### ListNamespaces

```bash
grpcurl -plaintext \
  -d '{
    "catalog_id": {
      "tenant_id": "t-0001",
      "id": "263b28bc-00e6-35bd-9f1a-8f9c13601cb3",
      "kind": "RK_CATALOG"
    }
  }' \
  localhost:9100 ai.floedb.metacat.catalog.ResourceAccess/ListNamespaces
```
### ListTables

```bash
grpcurl -plaintext \
  -d '{
    "namespace_id": {
      "tenant_id": "t-0001",
      "id": "4f8ed10e-897d-3113-bae0-076ebcc70871",
      "kind": "RK_NAMESPACE"
    }
  }' \
  localhost:9100 ai.floedb.metacat.catalog.ResourceAccess/ListTables
```

This early version of Metacat is seeded with test catalogs, namespaces and tables for testing.

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
