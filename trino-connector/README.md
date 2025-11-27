# Trino Connector (Skeleton)

Early skeleton for a Metacat-backed Trino connector. This is not wired into the root Maven build
yet; it is meant as a starting point to iterate without breaking the existing modules.

## Goal
- Expose Metacat-managed tables (Iceberg, Delta, etc.) to Trino by pulling schemas/snapshots/stats
  via Metacat APIs and returning upstream file locations for Trino to scan directly. Upstream
  connectors (Glue/Unity/etc.) remain Metacat’s concern; the Trino connector only speaks to Metacat
  services (Directory/Table/Planning/Stats).

## Status
- Java package scaffolded under `ai.floedb.metacat.trino`.
- Connector calls planning to fetch pruned plan files; schema is still parsed from `GetTable`
  JSON (no SchemaService dependency). Splits carry file paths and content type; metadata maps real
  columns. Page source still TODO.
- A standalone `pom.xml` declares Trino SPI + gRPC + Metacat proto dependencies; it is not added to
  the root aggregator.

## Next Steps
1. Implement `MetacatPageSourceProvider` to read Parquet/ORC using Trino readers against file paths
   from plan files (or delegate to native connectors) and wire presigning if needed.
2. Add this module to the root `pom.xml` once it compiles.
