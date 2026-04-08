# Embedding floecat commands

`CliCommandExecutor` lets you run floecat commands programmatically against an active gRPC
connection — no JLine terminal or interactive shell required. This is useful when another
application wants to expose floecat's catalog/table/connector operations as first-class
commands within its own interface.

---

## Instantiation

Construct a `CliCommandExecutor` with the gRPC stubs from your active channel and suppliers
for the current account/catalog context:

```java
ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();

CliCommandExecutor executor = CliCommandExecutor.builder()
    .out(out)                                             // PrintStream for all output
    .accounts(AccountServiceGrpc.newBlockingStub(channel))
    .catalogs(CatalogServiceGrpc.newBlockingStub(channel))
    .directory(DirectoryServiceGrpc.newBlockingStub(channel))
    .namespaces(NamespaceServiceGrpc.newBlockingStub(channel))
    .tables(TableServiceGrpc.newBlockingStub(channel))
    .viewService(ViewServiceGrpc.newBlockingStub(channel))
    .connectors(ConnectorsGrpc.newBlockingStub(channel))
    .reconcileControl(ReconcileControlGrpc.newBlockingStub(channel))
    .snapshots(SnapshotServiceGrpc.newBlockingStub(channel))
    .statistics(TableStatisticsServiceGrpc.newBlockingStub(channel))
    .constraintsService(TableConstraintsServiceGrpc.newBlockingStub(channel))
    .queries(QueryServiceGrpc.newBlockingStub(channel))
    .queryScan(QueryScanServiceGrpc.newBlockingStub(channel))
    .querySchema(QuerySchemaServiceGrpc.newBlockingStub(channel))
    .getAccountId(() -> session.getAccountId())   // read current account
    // setAccountId omitted — defaults to no-op; embedding context owns account identity
    .getCatalog(() -> session.getCatalog())        // read current catalog
    // setCatalog omitted — defaults to no-op; embedding context owns active catalog
    .build();
```

The executor is **stateless** — it holds no mutable state of its own beyond the injected
suppliers. It is safe to reuse across multiple `execute()` calls.

---

## Executing commands

```java
boolean ok = executor.execute("catalog list");
boolean ok = executor.execute("table create my-cat.my-ns.orders --schema ...");
boolean ok = executor.execute("connector trigger my-connector");
```

`execute()` tokenizes the input, dispatches to the appropriate handler, and writes all output
(including errors) to the `PrintStream` passed at construction. It **never throws**.

Returns `true` if the command completed without error, `false` otherwise. Use the return value
to detect failures and decide whether to continue or abort.

---

## Session state policy

The executor reads session state via `getAccountId` and `getCatalog`, but delegates state
mutation to the injected `setAccountId` and `setCatalog` callbacks. You control the policy:

| Callback | When to use |
|---|---|
| Real consumer (`id -> this.accountId = id`) | REPL / interactive shell — full session management |
| No-op (`id -> {}`) | Embedding — your context owns the session; mutations are silently ignored |
| Error-throwing (`id -> { throw new UnsupportedOperationException(...); }`) | Embedding — make mutations explicit failures |

When passing no-ops, commands like `account set` or `catalog use` will run without error but
will not affect your application's session state. If you want users to see an explicit error
for those commands, pass an error-throwing consumer instead.

---

## Building an escape interface

If your application has its own input parser (e.g., a SQL parser), and you want users to
invoke floecat commands inline, intercept the raw input **before** it reaches your parser.
Do not parse the line first — many parsers treat `--` as a line comment, which would silently
strip flag arguments like `--snapshot 42`.

Pseudocode:

```
for each input line:
    if line starts with your chosen escape prefix:
        command = line.removePrefix(escapePrefix).trim()
        ok = executor.execute(command)
        if not ok: handle error
    else:
        send to your parser as normal
```

The escape prefix is your choice (`\fc`, `!`, `/`, etc.). The executor handles tokenization
of the remainder — no further preprocessing needed. Quoted arguments with spaces work as
expected: `catalog create "my catalog"` becomes three tokens.

---

## Available commands

| Command group | Examples |
|---|---|
| `account` | `account list`, `account get <id>`, `account create <name>` |
| `catalog` / `catalogs` | `catalogs`, `catalog get <name>`, `catalog create <name>`, `catalog use <name>` |
| `namespace` / `namespaces` | `namespaces <catalog>`, `namespace create <fq>` |
| `table` / `tables` | `tables <ns>`, `table get <fq>`, `resolve <fq>`, `describe <fq>` |
| `view` / `views` | `views <ns>`, `view get <fq>` |
| `connector` / `connectors` | `connectors`, `connector create ...`, `connector trigger <id>` |
| `snapshot` / `snapshots` | `snapshots <table>`, `snapshot get <table> --snapshot <id>` |
| `stats` / `analyze` | `stats table <fq>`, `stats columns <fq>`, `analyze <fq>` |
| `constraints` | `constraints list <table>` |
| `query` | `query begin ...`, `query renew <id>`, `query end <id>`, `query fetch-scan <id> <table>` |

All commands support the same flags as the interactive shell. See `help` output in the shell
for full syntax.
