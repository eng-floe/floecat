# Trino Connector

A Metacat-backed Trino connector.

## Goal

- Expose Metacat-managed tables (Iceberg, Delta, etc.) to Trino by pulling schemas/snapshots/stats via Metacat APIs and returning upstream file locations for Trino to scan directly. Upstream connectors (Glue/Unity/etc.) remain Metacat’s concern; the Trino connector only speaks to Metacat services (Directory/Table/Planning/Stats).

## Status

- Java package scaffolded under `ai.floedb.metacat.client.trino`.
- Connector calls the Metacat query service to fetch pruned plan files.
- A standalone `pom.xml` declares Trino SPI + gRPC + Metacat proto dependencies

## Next Steps

1. Implement `MetacatPageSourceProvider` to read Parquet/ORC using Trino readers against file paths from plan files (or delegate to native connectors) and wire presigning if needed.
2. Add this module to the root `pom.xml` once it compiles.
