# Catalog Integrations and Overlays

Catalog integrations and overlays establish the resource, authentication, and SQL naming
foundation for external-catalog connectivity:

- A **catalog integration** records an upstream catalog type, URI, display name, non-secret
  connection properties, and typed authentication configuration. Credential material is stored
  separately and is never returned by the API.
- A **catalog overlay** maps selected upstream namespaces from an integration into an existing
  Floecat destination catalog.

```text
CatalogIntegration (external catalog identity)
 ├── CatalogOverlay "sales"
 └── CatalogOverlay "finance"
```

Catalog Integration RPCs validate connectivity and browse upstream metadata using the current
write-only credential generation. Discovery is read-only: it does not reconcile or capture tables
and does not affect query paths.

## Validation and discovery workflow

After creating an Integration, clients call `ValidateCatalogIntegration` with its resource ID. The
response reports catalog connection, catalog authentication, namespace/table discovery, credential
vending, and storage access as separate checks. Credential issues distinguish vending failure,
expiry, and invalid scope. With the default `vended-credentials` mode, `valid` is true only when all
five checks pass; an empty catalog cannot prove credential vending and therefore does not report
full validation success. For an Iceberg REST Integration configured with
`access-delegation-mode=none`, credential vending and its storage probe are intentionally not run,
and validation succeeds after connection, authentication, and discovery pass.

The response capability set covers operations relevant to public Integration validation and
discovery. Internal table and view loading capabilities belong to reconciliation and are not
reported by this RPC.

`ListUpstreamNamespaces` lists direct children of an optional parent path. Omitting the parent lists
the upstream root. `ListUpstreamObjects` lists lightweight table and view names within one upstream
namespace; callers may filter by object kind. Both operations are paginated, case-preserving, and
return the Integration mutation metadata used for the call. Returned namespace paths can be copied
directly into an Overlay's `include_namespaces` or `exclude_namespaces` selection.

These operations require `catalog-integration.read` and `catalog-integration.use`. They use the
catalog-access SPI directly and never call or fall back to the legacy Connector path.

Tables materialized by an Iceberg REST overlay retain their source Catalog Integration identity.
With the default `vended-credentials` mode, Floecat reopens that Integration through the
catalog-access SPI and asks the upstream catalog for table-scoped storage credentials when no
storage authority covers a table read. If vending was requested but the provider cannot supply
usable credentials, the read fails with the vending cause rather than silently changing credential
sources.

`access-delegation-mode=none` explicitly selects the alternative path. Floecat does not request
credentials from the upstream catalog and normal storage-authority resolution remains responsible
for the table read. The query path does not reconstruct or depend on a legacy Connector in either
mode.

One case is not a refusal. When the catalog vends a scope that does not reach the location Floecat
asked about, the credential is returned stamped with the location the caller was authorized for and
the mismatch is logged, because the read may still succeed and the object store enforces the real
grant either way. A scope merely narrower than the request is stamped as itself rather than widened.

A legacy Connector still behaves as it did: it opts in to vending, so one that does not is left to
the storage authority the operator configured for it.

## Shell workflow

Create the integration record, then map its selected namespaces into an existing destination
catalog:

```text
integration create lakehouse iceberg-rest https://catalog.example/v1 \
  --auth-type oauth-client-credentials \
  --auth client_id=floecat token_uri=https://identity.example/token \
  --cred client_secret=secret \
  --props warehouse=analytics
overlay create sales-overlay lakehouse local-catalog --include prod.sales,prod.reference
integration validate lakehouse
integration namespaces lakehouse
integration objects lakehouse prod.sales --kinds table,view
overlay reconcile sales-overlay
```

For Unity Catalog with a bearer token, the equivalent Delta integration is:

```text
integration create databricks unity https://workspace.example \
  --auth-type bearer --cred token=secret \
  --props catalog=main s3.region=us-east-1
overlay create delta-sales databricks local-catalog --include sales
integration validate databricks
integration namespaces databricks
integration objects databricks sales --kinds table,view
overlay reconcile delta-sales
```

For a Delta Sharing recipient, the share and schema are the namespace:

```text
integration create partner delta-sharing https://sharing.example/delta-sharing \
  --auth-type bearer --cred token=recipient-token \
  --props s3.region=us-east-1
overlay create partner-tables partner local-catalog --include acme_share.gold
integration validate partner
integration namespaces partner
integration namespaces partner --parent acme_share
integration objects partner acme_share.gold
overlay reconcile partner-tables
```

Delta Sharing accepts bearer authentication only, because the protocol defines no other scheme. A
table is usable where its provider offers directory access; one offering url access alone returns
presigned per-file URLs rather than credentials, and is refused when the overlay reconciles rather
than materialized as a table nothing can open. A table stating no
access modes is asked rather than refused -- see
[`docs/operations.md`](operations.md#delta-sharing-access-modes) for why, and for
`delta.sharing.strict-access-modes`.

Unity OAuth client credentials use the same `oauth-client-credentials` CLI form as Iceberg REST.
The configured token URI is optional; when omitted, the Unity provider uses `/oidc/v1/token` on the
catalog host. Unity Integration discovery currently exposes Delta tables and Unity views. Table
storage credentials are obtained only from Unity's temporary-table-credentials API and are
validated without falling back to configured or ambient AWS credentials.

The overlay command accepts either a resource ID or display name for the integration.
Namespace filters are comma-separated paths supplied with `--include` and `--exclude`. Omitting both
selects the whole upstream namespace tree.

The available commands are:

```text
integrations
integration list
integration get <name|id>
integration create <name> <type> <uri> --auth-type <type> [--auth k=v ...] [--cred k=v ...] [--props k=v ...]
integration update <name|id> [--display <name>] [--uri <uri>] [--props k=v ...] [--etag <etag>]
integration update-auth <name|id> --auth-type <type> [--auth k=v ...] [--cred k=v ...]
integration validate <name|id>
integration namespaces <name|id> [--parent <namespace>]
integration objects <name|id> <namespace> [--kinds table,view]
integration delete <name|id>

overlays [--integration <name|id>]
overlay list [--integration <name|id>]
overlay get <name|id>
overlay create <name> <integration-name|id> <catalog-name|id> [options]
overlay update <name|id> [options]
overlay reconcile <name|id> [--etag <etag>]
overlay delete <name|id>
```

Run only the real-Polaris Integration validation and Overlay reconciliation smoke scenario with:

```text
COMPOSE_SMOKE_MODES=polaris-integration make compose-smoke
```

This mode does not create or trigger a legacy Connector resource.

After the Overlay materializes, the Polaris scenario disables **every** storage authority whose
prefix covers the table location, then loads the Overlay table through Floecat's own Iceberg REST
gateway with `X-Iceberg-Access-Delegation: vended-credentials` and requires a complete session
tuple in the response.

Every covering authority matters, not just the one the smoke created. `matchesLocationPrefix`
strips a trailing slash from the configured prefix and then requires a path boundary, so the
seeded `fixture-floecat` authority at `s3://floecat` covers `s3://floecat/sales/...` exactly as the
smoke's own `s3://floecat/` does — and leaving it enabled means the read resolves through it and
never reaches the vend. The scenario computes coverage with that same rule, disables what it finds,
and re-lists to confirm nothing still covers before reading. They are disabled rather than deleted
so re-enabling restores the exact record, which recreating a seeded fixture from guessed arguments
would not.

With nothing covering the location and the table already asserted to carry no Connector,
`vendFromCatalogIntegration` is the only code that can put a credential in that response, so the
response carries the whole assertion. The authorities are re-enabled afterwards for the sections
that still read through them.

The gateway is used rather than a capture because `overlay reconcile` is metadata-only and capture
needs `connector trigger`, which an Overlay-materialized table has no Connector for.

The full LocalStack smoke also exercises the Unity Integration and Overlay path against the same
TLS-backed Unity/Delta fixture used by the Connector migration scenario. It validates discovery,
credential vending, a storage read with those credentials, and Overlay materialization.

The Unity Integration finishes with the same gateway `loadTable` check as the Polaris one. No
authority has to be removed there: that fixture is copied to a bucket deliberately absent from
`COMPOSE_SMOKE_LOCALSTACK_BUCKETS`, which the scenario asserts rather than assumes. Its upstream
namespace has two levels, so the URL uses the `%1F` separator the gateway's own `/v1/config`
advertises.

The Delta Sharing scenario runs beside them, against a recipient endpoint served by
`docker/delta-sharing/stub_server.py`. It is a stub rather than the reference server because no
published `deltaio/delta-sharing-server` image implements directory access: that landed upstream in
March 2026 and the last image tag is from April 2024. It serves the same Delta fixture from a third
bucket, also absent from `COMPOSE_SMOKE_LOCALSTACK_BUCKETS`, and records every request it answered so
the scenario can assert the share was actually asked for credentials rather than inferring it from a
check that passed. It finishes with the same gateway `loadTable` check, and separately asserts that a
recipient token the share does not accept fails validation.

Authentication types and their properties are:

| `--auth-type` | `--auth` properties | `--cred` properties |
| --- | --- | --- |
| `oauth-client-credentials` | `client_id`; optional `token_uri`, `scopes` CSV | `client_secret` |
| `bearer` | none | `token` |
| `aws-access-key` | `access_key_id` | `secret_access_key`; optional `session_token` |
| `aws-sigv4` | `region`, `credential_source`; optional `signing_name`, plus source fields | source-dependent |

For SigV4, `credential_source` is `default`, `assume-role`, or `access-key`. Assume-role requires
`role_arn`; access-key requires `access_key_id` plus its secret credential properties. The CLI
rejects unknown properties instead of silently dropping them.

Ambient credentials are deployment-gated because they expose the Floecat service's AWS identity to
tenant-authored Catalog Integrations. They are disabled by default; enable them only in a trusted
deployment with `floecat.catalog-integrations.aws.default-credentials-enabled=true`.

AssumeRole is always available as a Catalog Integration AWS SigV4 credential source; it has no
deployment enablement switch. Floecat takes the service principal ARN from
`FLOECAT_CATALOG_INTEGRATIONS_AWS_SERVICE_PRINCIPAL_ARN` when explicitly configured, otherwise it
uses the `AWS_ROLE_ARN` injected into EKS pods using IRSA. Standalone deployments that use AWS
AssumeRole must configure the explicit value. Standalone deployments without an AWS identity may
leave both unset; Floecat still starts, but the trust-configuration RPC fails closed because it has
no principal to advertise. The corresponding application property is
`floecat.catalog-integrations.aws.service-principal-arn`.
Select the account in the CLI and run `account aws-trust-configuration` to obtain that principal
ARN, the stable Floecat-issued external ID for the account, and a sample AWS IAM trust policy. The
customer configures that policy on the target role, then creates the Catalog Integration with
`--auth-type aws-sigv4`, `credential_source=assume-role`, and `role_arn`; tenants cannot choose the
external ID or complete role session name sent to STS. Existing accounts receive an external ID
atomically on first trust-configuration request or first AssumeRole use. That first write advances
the account resource version, so an account update holding the prior version must reload before
retrying. Account records are the source of truth. A fresh credential-cache hit performs no account
lookup; other reads use the normal repository cache, and only first-time initialization uses the raw
mutation read. There is no separate external-ID cache. Consequently, deleting an account is
observed when the cached credentials next require a refresh, rather than by re-reading the account
on every Catalog Integration open.

Before Floecat obtains credentials with the account-owned external ID, it attempts the target role
with a random incorrect external ID. AWS STS must reject that probe with `AccessDenied`; if it
succeeds, or if enforcement cannot be verified, Floecat fails closed and does not use or cache the
target credentials. The check is repeated whenever assumed-role credentials are renewed, so a role
whose trust policy is later weakened stops refreshing. Each check intentionally produces one denied
`AssumeRole` event in the target account's CloudTrail history. Deployments should expect these probe
events and account for them in alerts on denied STS calls.

The trust-configuration RPC fails closed if `service-principal-arn` is absent or is not an IAM
principal ARN. Top-level AWS AssumeRole authentication is not supported; configure AssumeRole as
the credential source inside AWS SigV4.

This behavior is specific to Catalog Integrations. Storage Authorities and Connectors retain their
existing AssumeRole configuration and external-ID behavior pending separate migrations.

`--props` supplies non-secret provider connection properties. For Iceberg REST catalogs such as
Polaris, `warehouse=<catalog-name>` selects the upstream catalog without putting a query parameter in
the base URI. Iceberg REST integrations request `vended-credentials` by default. Set
`access-delegation-mode=none` to omit `X-Iceberg-Access-Delegation` when Floecat already has the
storage configuration needed by the downstream reader. In that mode, source-catalog credential
vending is skipped and normal storage-authority resolution is used instead. Updating properties
replaces the complete map; passing `--props` with no values clears it.

Each Floecat process caches source-catalog vends, keyed by the Connector or Integration
configuration and the upstream table. An answer is reused only while at least
`floecat.storage.source-catalog.vend-cache.min-remaining-fraction` of its lifetime (default `0.5`)
and at least five minutes remain. With a fraction of `0`, an answer is reused until five minutes
before it expires, which is when an Iceberg client refreshes a vended credential; the default keeps
half the lifetime for readers that do not refresh. Answers without an expiry are not cached, nor are
answers missing a field every use requires, nor credentials a Connector obtains by exchanging the
caller's own token. `floecat.storage.source-catalog.vend-cache.max-entries` (default 10000) bounds
the cache for each source kind; a value of zero or less disables it. Vends of a table that arrive
together before an answer is held each go upstream. The vend permission check runs on every call. A
Connector's key includes the credential it authenticates with, so rotating its secret in the
credential store misses. An Integration's key is its record, which carries its credential
generation, so rotating its secret through the API misses as well. Only an edit of the stored secret
made outside Floecat keeps serving answers vended with the old secret until their reuse window ends.
The cache reports `floecat_core_cache_*` series tagged `cache="vended-credential"`.

For Delta Sharing, supported properties are `http.connect.ms`, `http.read.ms`,
`delta.sharing.strict-access-modes`, `delta.sharing.reader-features`, `s3.region`, `s3.endpoint`,
`client.region`, and `s3.path-style-access`. The S3 properties route reads of credentials the share
vends; they do not supply storage credentials. `s3.endpoint` is held to the same rule as Unity's,
below, for the same reason: a Delta Sharing vend also carries an AWS session token.

For Unity Catalog, the optional `catalog` property scopes the Integration to exactly one Unity
catalog and exposes that catalog's schemas as root Floecat namespaces. Without `catalog`, all Unity
catalogs are exposed as root namespaces and their schemas as child namespaces, preserving the
behavior of existing Integrations. Other supported properties are `http.connect.ms`, `http.read.ms`,
`unity.temporary-table-vend-path`, `s3.region`, `s3.endpoint`, and `s3.path-style-access`. The S3
properties route validation of credentials vended by Unity; they do not supply storage credentials.
There is no `s3.access-point` property: validation probes the bucket named in the object URI, which
is what a reader addresses, so an access point set here would describe an endpoint no scan uses.

`s3.endpoint` must be HTTPS unless the deployment sets
`FLOECAT_SECURITY_ALLOW_CLEARTEXT_S3_ENDPOINTS=true`. A Unity vend is published only when it carries
an AWS session token, which travels in a request header and is replayable against the table's
storage prefix until it expires, and the endpoint is republished to reconcile and query workers. An
`s3.endpoint` written as a private address literal additionally needs
`FLOECAT_SECURITY_ALLOW_PRIVATE_CATALOG_ENDPOINTS`; a hostname is never resolved and needs neither.
See [Operations](operations.md#cleartext-s3-endpoints).

`s3.region` may be spelled `region`, `client.region`, or `aws.region`; whichever is present decides
the region for both validation and reads. Only when none is set does the deployment's
`floecat.storage.aws.region` apply.

## Lifecycle

- Overlay creation requires an existing integration.
- Overlay display names are unique within an account and identify the mapping into a destination
  catalog.
- An integration cannot be deleted while overlays refer to it.
- Integration deletion supports `--cascade` to delete dependent overlays.
- Integration and overlay mutations support optimistic `--etag` preconditions.
- Authentication replacement uses the dedicated `integration update-auth` command so credential
  values remain write-only.

The protobuf contracts are in `core/proto/src/main/proto/floecat/integration/`.
