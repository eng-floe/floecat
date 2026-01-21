# Trino Floecat Connector

A read-only Trino connector that federates Floecat-managed tables into Trino. Floecat handles
namespace/catalog resolution, table schemas, and scan planning; Trino only scans the returned data files.

## What this connector does

- Lists schemas and tables by calling Floecat Directory/Namespace services.
- Resolves table schemas (with optional snapshot/as-of semantics) and hands the scan off to Trino’s Iceberg readers using file locations returned by the Floecat query service.
- Pushes projections and predicates into Floecat planning so only the needed columns/files are returned.
- Supports Iceberg delete files returned by Floecat planning.
- Exposes time-travel session properties (`snapshot_id`, `as_of_epoch_millis`).
- Read-only: no CREATE/INSERT/ALTER/DELETE; DDL and write paths stay in Floecat.

## Build and install

1. Build the connector from the repo root (Java 21):

   ```
   mvn -pl connectors/clients/trino -am package
   ```

2. Copy the connector jar and its runtime deps into a Trino plugin directory (e.g.
   `$TRINO_HOME/plugin/floecat`):

   ```
   PLUGIN_DIR=/opt/trino/plugin/floecat
   mkdir -p "$PLUGIN_DIR"
   cp connectors/clients/trino/target/trino-floecat-connector-0.1.0-SNAPSHOT.jar "$PLUGIN_DIR"/
   cp connectors/clients/trino/target/plugin-deps/*.jar "$PLUGIN_DIR"/
   ```

3. Restart Trino.

## Configure a catalog

Create `etc/catalog/floecat.properties`:

```
connector.name=floecat
floecat.uri=localhost:19000              # gRPC endpoint for Floecat services
floecat.coordinator-file-caching=true    # optional: enable FS caching on the coordinator
fs.native-s3.enabled=true                # recommended when reading S3-backed tables

# Optional S3 overrides passed through to the Iceberg file IO
# floecat.s3.access-key=YOUR_KEY
# floecat.s3.secret-key=YOUR_SECRET
# floecat.s3.session-token=YOUR_SESSION_TOKEN
# floecat.s3.region=us-east-1
# floecat.s3.endpoint=https://s3.us-east-1.amazonaws.com

# Optional STS assume-role configuration
# floecat.s3.sts.role-arn=arn:aws:iam::<account>:role/<role-name>
# floecat.s3.sts.region=us-east-1
# floecat.s3.sts.endpoint=https://sts.us-east-1.amazonaws.com
# floecat.s3.role-session-name=floecat-trino
```

## Authentication to S3

- **Static keys:** If `floecat.s3.access-key`/`floecat.s3.secret-key` (and optional
  `floecat.s3.session-token`) are set, they are passed to Iceberg as `s3.aws-*` overrides and used
  directly.
- **Assume role:** If `floecat.s3.sts.role-arn` is set, Trino’s S3 client will assume that role
  using the credentials available in the process (static keys above or the default AWS provider
  chain). Optional `floecat.s3.sts.region`/`endpoint`/`role-session-name` tune the STS call.
- **Default credential chain:** When no `floecat.s3.*` fields are provided, Trino falls back to its
  standard AWS credential resolution (env vars, `~/.aws` profiles, web identity, EC2/ECS instance
  metadata, etc.). On an EC2 node with an IAM role attached, that instance role is used. In that
  case, leaving `floecat.s3.sts.role-arn` commented out (as in the example) means no extra assume
  role occurs.

## Usage notes

- Time travel: `SET SESSION floecat.snapshot_id = 123;` or
  `SET SESSION floecat.as_of_epoch_millis = 1700000000000;` (only one at a time). Set to `-1` to
  return to the current snapshot.
- The connector expects Floecat to return Iceberg-compatible file plans; table lifecycle and
  snapshot management stay in Floecat.
