# Trino Metacat Connector

A read-only Trino connector that federates Metacat-managed tables into Trino. Metacat handles
namespace/catalog resolution, table schemas, and scan planning; Trino only scans the returned data files.

## What this connector does

- Lists schemas and tables by calling Metacat Directory/Namespace services.
- Resolves table schemas (with optional snapshot/as-of semantics) and hands the scan off to Trino’s Iceberg readers using file locations returned by the Metacat query service.
- Pushes projections and predicates into Metacat planning so only the needed columns/files are returned.
- Supports Iceberg delete files returned by Metacat planning.
- Exposes time-travel session properties (`snapshot_id`, `as_of_epoch_millis`).
- Read-only: no CREATE/INSERT/ALTER/DELETE; DDL and write paths stay in Metacat.

## Build and install

1. Build the connector from the repo root (Java 21):

   ```
   mvn -pl connectors/clients/trino -am package
   ```

2. Copy the connector jar and its runtime deps into a Trino plugin directory (e.g.
   `$TRINO_HOME/plugin/metacat`):

   ```
   PLUGIN_DIR=/opt/trino/plugin/metacat
   mkdir -p "$PLUGIN_DIR"
   cp connectors/clients/trino/target/trino-metacat-connector-0.1.0-SNAPSHOT.jar "$PLUGIN_DIR"/
   cp connectors/clients/trino/target/plugin-deps/*.jar "$PLUGIN_DIR"/
   ```

3. Restart Trino.

## Configure a catalog

Create `etc/catalog/metacat.properties`:

```
connector.name=metacat
metacat.uri=localhost:19000              # gRPC endpoint for Metacat services
metacat.coordinator-file-caching=true    # optional: enable FS caching on the coordinator
fs.native-s3.enabled=true                # recommended when reading S3-backed tables

# Optional S3 overrides passed through to the Iceberg file IO
# metacat.s3.access-key=YOUR_KEY
# metacat.s3.secret-key=YOUR_SECRET
# metacat.s3.session-token=YOUR_SESSION_TOKEN
# metacat.s3.region=us-east-1
# metacat.s3.endpoint=https://s3.us-east-1.amazonaws.com

# Optional STS assume-role configuration
# metacat.s3.sts.role-arn=arn:aws:iam::<account>:role/<role-name>
# metacat.s3.sts.region=us-east-1
# metacat.s3.sts.endpoint=https://sts.us-east-1.amazonaws.com
# metacat.s3.role-session-name=metacat-trino
```

## Authentication to S3

- **Static keys:** If `metacat.s3.access-key`/`metacat.s3.secret-key` (and optional
  `metacat.s3.session-token`) are set, they are passed to Iceberg as `s3.aws-*` overrides and used
  directly.
- **Assume role:** If `metacat.s3.sts.role-arn` is set, Trino’s S3 client will assume that role
  using the credentials available in the process (static keys above or the default AWS provider
  chain). Optional `metacat.s3.sts.region`/`endpoint`/`role-session-name` tune the STS call.
- **Default credential chain:** When no `metacat.s3.*` fields are provided, Trino falls back to its
  standard AWS credential resolution (env vars, `~/.aws` profiles, web identity, EC2/ECS instance
  metadata, etc.). On an EC2 node with an IAM role attached, that instance role is used. In that
  case, leaving `metacat.s3.sts.role-arn` commented out (as in the example) means no extra assume
  role occurs.

## Usage notes

- Time travel: `SET SESSION metacat.snapshot_id = 123;` or
  `SET SESSION metacat.as_of_epoch_millis = 1700000000000;` (only one at a time). Set to `-1` to
  return to the current snapshot.
- The connector expects Metacat to return Iceberg-compatible file plans; table lifecycle and
  snapshot management stay in Metacat.
