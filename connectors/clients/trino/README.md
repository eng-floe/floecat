# Trino Connector

A Metacat-backed Trino connector.

## Goal

- Expose Metacat-managed tables (Iceberg, Delta, etc.) to Trino by pulling schemas/snapshots/stats via Metacat APIs and returning upstream file locations for Trino to scan directly. Upstream connectors (Glue/Unity/etc.) remain Metacat’s concern; the Trino connector only speaks to Metacat services (Directory/Table/Planning/Stats).

## Status

- Java package scaffolded under `ai.floedb.metacat.client.trino`.
- Connector calls the Metacat query service to fetch pruned plan files.
- A standalone `pom.xml` declares Trino SPI + gRPC + Metacat proto dependencies

## Configuration

The connector can pass explicit S3 settings through to Trino’s Iceberg file IO via `metacat.s3.*`:

- `metacat.s3.access-key`, `metacat.s3.secret-key`, optional `metacat.s3.session-token`
- `metacat.s3.region`, optional `metacat.s3.endpoint`
- Optional STS role: `metacat.s3.sts.role-arn`, `metacat.s3.sts.region`, `metacat.s3.sts.endpoint`, `metacat.s3.role-session-name`

If unset, Trino falls back to its own S3 configuration/credential chain.
