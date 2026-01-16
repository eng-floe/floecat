# API Access

## gRPC

Use any gRPC client (for example `grpcurl`) once the service listens on `localhost:9100`.

```bash
grpcurl -plaintext -d '{}' \
  localhost:9100 ai.floedb.floecat.catalog.CatalogService/ListCatalogs

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

Set the account context first:

```
account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2
```

Then explore `catalog`, `namespace`, `table`, `connector`, and `query` commands. The CLI exercises
the same gRPC surface described in [`service.md`](service.md).
