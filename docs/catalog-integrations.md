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
expiry, and invalid scope. `valid` is true only when all five checks pass; an empty catalog cannot
prove credential vending and therefore does not report full validation success.

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
When no storage authority covers a table read, Floecat reopens that Integration through the
catalog-access SPI and asks the upstream catalog for table-scoped storage credentials. The query
path therefore does not reconstruct or depend on a legacy Connector.

A storage authority is not an alternative for an Integration-backed table, and Floecat does not fall
back to one. Pairing an authority with an Integration is the split-brain this feature removes --
authenticate to the catalog here, obtain storage credentials somewhere else -- and the Integration
record has no way to express it: nothing on it names an authority, and `ValidateCatalogIntegration`
reports an Integration whose provider cannot vend as invalid rather than as configured differently.
Anything that means "this Integration cannot vend" therefore fails the read naming the cause: a
provider that does not advertise storage-credential vending, an authentication mode the
catalog-access SPI does not implement, a provider reporting that what it can vend does not cover the
upstream table, or a catalog that returns no credentials for it.

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

Authentication types and their properties are:

| `--auth-type` | `--auth` properties | `--cred` properties |
| --- | --- | --- |
| `oauth-client-credentials` | `client_id`; optional `token_uri`, `scopes` CSV | `client_secret` |
| `bearer` | none | `token` |
| `aws-assume-role` | `role_arn`; optional `external_id`, `role_session_name` | none |
| `aws-access-key` | `access_key_id` | `secret_access_key`; optional `session_token` |
| `aws-sigv4` | `region`, `credential_source`; optional `signing_name`, plus source fields | source-dependent |

For SigV4, `credential_source` is `default`, `assume-role`, or `access-key`. Assume-role uses the
role properties above. Access-key uses the access-key properties above. The CLI rejects unknown
properties instead of silently dropping them.

`--props` supplies non-secret provider connection properties. For Iceberg REST catalogs such as
Polaris, `warehouse=<catalog-name>` selects the upstream catalog without putting a query parameter in
the base URI. Updating properties replaces the complete map; passing `--props` with no values clears
it.

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
