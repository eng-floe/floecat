# Metacat

Metacat is a lightweight catalog-of-catalogs for the modern data lakehouse.
It federates metadata from Delta and Iceberg catalogs into a unified, gRPC-based service for discovery, access control, and query planning.

## Build and Run

Requires Java 25, Maven, and Make.

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
