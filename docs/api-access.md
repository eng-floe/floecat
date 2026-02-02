# API Access

## Authentication & permissions

Floecat supports two client authentication modes:

- **Token auth** (recommended): pass a session token via `--token` / `FLOECAT_TOKEN`. In this mode the **server derives the account and identity from the token claims**; the CLI `account` command only affects how the client *formats* resource IDs, and does **not** change who you are server-side.
- **Principal auth**: no token; the client sends a principal (via `x-principal-bin`). In this mode you must set an **account UUID** (`account <uuid>` or `FLOECAT_ACCOUNT`) so resource IDs are constructed correctly.

How to tell if you have permission:

- If a command succeeds, you have the required permission.
- If it fails with `PERMISSION_DENIED` and a message like `missing permission: <perm>`, you do **not** have that permission for the current identity/account.

Common failure modes:

- `No account set`: you are in principal auth mode and did not set `account <uuid>` (or `FLOECAT_ACCOUNT`).
- `invalid or unknown account`: you set a non-UUID account (e.g. `t-001`). Accounts are UUIDs; use token auth if you need environment-style identifiers.
- `PERMISSION_DENIED`: your current identity is valid, but your role bindings do not include the required permission (e.g. `catalog.read`).

Tip: run the shell with `FLOECAT_SHELL_DEBUG=1` (or `-Dfloecat.shell.debug=true`) to print deeper gRPC error details.

## gRPC

Use any gRPC client (for example `grpcurl`) once the service listens on `localhost:9100`.

```bash
grpcurl -plaintext -d '{}' \
  localhost:9100 ai.floedb.floecat.catalog.CatalogService/ListCatalogs
```

If the server is configured for token auth, pass the token as metadata (header name defaults to `Authorization`):

```bash
grpcurl -plaintext \
  -H 'Authorization: Bearer <TOKEN>' \
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


## CLI

The [`client-cli`](client-cli.md) module provides an interactive shell with auto-completion and
context-aware rendering. After building it (`make cli` or `make cli-run`), launch with:

```bash
java --enable-native-access=ALL-UNNAMED \
  -jar client-cli/target/quarkus-app/quarkus-run.jar
```

Set the account context first (principal auth mode only):

```
account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2
```

If you are using token auth (`--token` / `FLOECAT_TOKEN`), the server account comes from the token; the `account` command is optional and does not grant permissions.

Then explore `catalog`, `namespace`, `table`, `connector`, and `query` commands. The CLI exercises
the same gRPC surface described in [`service.md`](service.md).
