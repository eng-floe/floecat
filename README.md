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
- CLI commands: [`docs/cli-reference.md`](docs/cli-reference.md)
- Operations, testing, and observability: [`docs/operations.md`](docs/operations.md)

All additional component docs live under [`docs/`](docs).

## External Session Header

The `floecat` service can accept an external session header carrying a JWT. This is validated by Quarkus OIDC and used to authenticate gRPC calls.

Configuration (service `application.properties`):
- `floecat.interceptor.session.header=x-floe-session` to enable the header.
- `floecat.interceptor.validate.account=false` to skip account lookup when the caller supplies a trusted account id.
- `quarkus.oidc.auth-server-url=...` and `quarkus.oidc.client-id=...` to validate with an issuer URL.
- `quarkus.oidc.public-key=...` to validate locally with a public key (tests/dev).
- `quarkus.oidc.client_id=...` to set the `aud` claim value expected.

Environment equivalents:
- FLOECAT_INTERCEPTOR_SESSION_HEADER
- FLOECAT_INTERCEPTOR_VALIDATE_ACCOUNT
- QUARKUS_OIDC_AUTH_SERVER_URL
- QUARKUS_OIDC_PUBLIC_KEY
- QUARKUS_OIDC_CLIENT_ID

Further documentation on integration to external OpenID Connect IDPs can be found here:
https://quarkus.io/guides/security-oidc-configuration-properties-reference

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
