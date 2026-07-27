# Floecat connector transfer

`floecat-connector-transfer` exports and imports complete connector definitions. The export archive
contains the unmasked connector protobuf and, when present, its separate `AuthCredentials`
protobuf. Treat the archive as a secret.

The tool uses the project's Java 25 target. The source Floecat service must include the
`ExportConnector` RPC added with this module. Avoid updating connector credentials while an export
is running; connector records and secrets live in separate stores and cannot provide a distributed
transactional snapshot.

Build the executable jar:

```shell
mvn -pl tools/connector-transfer -am package
```

Export every connector from an account:

```shell
tools/connector-transfer/floecat-connector-transfer \
  --host localhost --port 9100 --account-id ACCOUNT_ID \
  export connectors.zip --plaintext-secrets
```

Use repeated `--connector NAME_OR_ID` options to select individual connectors. Export requires the
`connector.export` permission. Administrator and developer roles receive this permission.

Inspect an archive without printing credential values:

```shell
tools/connector-transfer/floecat-connector-transfer inspect connectors.zip
```

Validate an import without changing the target:

```shell
tools/connector-transfer/floecat-connector-transfer \
  --host target.example --port 9100 --account-id TARGET_ACCOUNT \
  import connectors.zip --dry-run
```

The dry run reports whether each connector would be created, skipped, replaced, or rejected. It
returns a nonzero exit status when `--conflict FAIL` encounters an existing display name.

Import and fail if a display name already exists:

```shell
tools/connector-transfer/floecat-connector-transfer \
  --host target.example --port 9100 --account-id TARGET_ACCOUNT \
  import connectors.zip
```

`--conflict SKIP` leaves existing connectors unchanged. `--conflict REPLACE` deletes an existing
same-name connector before creating the imported definition, so use it deliberately. The tool emits
the source connector ID and new target connector ID for each successful import.

Authentication can be provided through `--token`, `--session-token`, `FLOECAT_TOKEN`, or
`FLOECAT_SESSION_TOKEN`. Connections are plaintext by default, matching the Floecat development
CLI; pass `--tls` for a TLS endpoint.
