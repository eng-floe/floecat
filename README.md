<p align="center">
  <img src="images/floecat.png" alt="Floecat logo" width="200">
</p>

Floecat is a catalog-of-catalogs for modern data lakehouses. It federates metadata harvested from
Delta Lake and Iceberg sources, stores a canonical view in append-only blob storage, and exposes the
resulting hierarchy over gRPC for discovery, authorization, and query planning.

The repository is purposely modular. Each top-level directory corresponds to an operational
component (service runtime, connector packages, storage backends, CLI, etc.). Detailed,
component-specific documentation lives under [`docs/`](docs).

## Build, Run, and Test

Requirements: Java 25+, Maven, Make.

```bash
make build
make run
make test
```

Seed data is enabled by default (`floecat.seed.enabled=true`); the service starts with a demo account,
catalogs, namespaces, tables, and snapshots.

## Documentation

- Architecture, system flow, and modules: [`docs/architecture.md`](docs/architecture.md)
- Storage model and key layout: [`docs/storage-layout.md`](docs/storage-layout.md)
- API access patterns: [`docs/api-access.md`](docs/api-access.md)
- External authentication and session headers: [`docs/external-authentication.md`](docs/external-authentication.md)
- System scan overview (gRPC + Flight): [`docs/system-scans.md`](docs/system-scans.md)
- gRPC system scan protocol: [`docs/system-scans-grpc.md`](docs/system-scans-grpc.md)
- Arrow Flight protocol and integration: [`docs/arrow-flight.md`](docs/arrow-flight.md)
- CLI commands: [`docs/cli-reference.md`](docs/cli-reference.md)
- Operations, testing, and observability: [`docs/operations.md`](docs/operations.md)

All additional component docs live under [`docs/`](docs).

## Contributing

Contribution workflow, review policy, and contributor expectations are documented in:

- [`CONTRIBUTING.md`](CONTRIBUTING.md)

All participants are expected to follow:

- [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)
- [`SECURITY.md`](SECURITY.md) for vulnerability reporting
