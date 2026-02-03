# API Access

## Authentication & permissions

Floecat supports two client authentication modes:

- **Authorization header (Bearer token)**: pass `--token` (or `FLOECAT_TOKEN`). The token is sent
  as `authorization: Bearer <token>` by default. The server derives identity and account from token
  claims.
- **Session header (custom)**: pass `--session-token` (or `FLOECAT_SESSION_TOKEN`) and optionally
  set `--session-header` (default `x-floe-session`). This is functionally equivalent to
  authorization header auth but uses a custom header name.

Optional **account header**:

- If you provide `--account-id` (or `FLOECAT_ACCOUNT`), the CLI sends `x-account-id` by default
  (overridable via `--account-header`). The server will reject the request if the header does not
  match the `account_id` claim in the JWT.

How to tell if you have permission:

- If a command succeeds, you have the required permission.
- If it fails with `PERMISSION_DENIED` and a message like `missing permission: <perm>`, you do not
  have that permission for the current identity.

## gRPC

Use any gRPC client (for example `grpcurl`) once the service listens on `localhost:9100`.

```bash
grpcurl -plaintext -d '{}' \
  localhost:9100 ai.floedb.floecat.catalog.CatalogService/ListCatalogs
```

If the server is configured for authorization-header auth (default header `authorization`):

```bash
grpcurl -plaintext \
  -H 'authorization: Bearer <TOKEN>' \
  -d '{}' localhost:9100 \
  ai.floedb.floecat.catalog.CatalogService/ListCatalogs
```

If the server is configured for session-header auth (default header `x-floe-session`):

```bash
grpcurl -plaintext \
  -H 'x-floe-session: <TOKEN>' \
  -d '{}' localhost:9100 \
  ai.floedb.floecat.catalog.CatalogService/ListCatalogs
```

grpcurl -plaintext -d '{
  "catalog_id": {"account_id":"5eaa9cd5-7d08-3750-9457-cfe800b0b9d2",
                 "id":"109c1761-323a-3f72-83da-ff4f89c3b581","kind":"RK_CATALOG"}
}' localhost:9100 ai.floedb.floecat.catalog.NamespaceService/ListNamespaces

grpcurl -plaintext -d '{
  "namespace_id": {"account_id":"5eaa9cd5-7d08-3750-9457-cfe800b0b9d2",
                   "id":"86853a0f-a999-3c72-9a81-6dc66d1923a2","kind":"RK_NAMESPACE"}
}' localhost:9100 ai.floedb.floecat.catalog.TableService/ListTables
```

## CLI

The [`client-cli`](client-cli.md) module provides an interactive shell with auto-completion and
context-aware rendering. After building it (`make cli` or `make cli-run`), launch with:

```bash
java --enable-native-access=ALL-UNNAMED \
  -jar client-cli/target/quarkus-app/quarkus-run.jar
```

Set the account context first (only if you intend to send the account header):

```
account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2
```

If you use bearer-token auth, provide it via `--token` or `FLOECAT_TOKEN`:

```bash
FLOECAT_TOKEN=<TOKEN> \
  java --enable-native-access=ALL-UNNAMED \
  -jar client-cli/target/quarkus-app/quarkus-run.jar
```

Then explore `catalog`, `namespace`, `table`, `connector`, and `query` commands. The CLI exercises
the same gRPC surface described in [`service.md`](service.md).
