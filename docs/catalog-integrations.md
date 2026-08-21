# Catalog Integrations and Overlays

Catalog integrations and overlays establish the resource, authentication, and SQL naming
foundation for external-catalog connectivity:

- A **catalog integration** records an upstream catalog type, URI, display name, non-secret
  connection properties, and typed authentication configuration. Credential material is stored
  separately and is never returned by the API.
- A **catalog overlay** defines a top-level Floecat catalog backed by an integration and filtered
  to selected upstream namespaces.

```text
CatalogIntegration (external catalog identity)
 ├── CatalogOverlay "sales"
 └── CatalogOverlay "finance"
```

These resources do not yet validate connectivity, refresh metadata, reconcile or capture tables,
or affect query paths. The legacy `Connector` API and Shell commands remain the operational path
for external catalog connectivity.

## Shell workflow

Create the integration record, then define a top-level overlay catalog:

```text
integration create lakehouse iceberg-rest https://catalog.example/v1 \
  --auth-type oauth-client-credentials \
  --auth client_id=floecat token_uri=https://identity.example/token \
  --cred client_secret=secret \
  --props warehouse=analytics
overlay create sales-overlay lakehouse --include prod.sales,prod.reference
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
integration update <name|id> [--display <name>] [--props k=v ...] [--etag <etag>]
integration update-auth <name|id> --auth-type <type> [--auth k=v ...] [--cred k=v ...]
integration delete <name|id>

overlays [--integration <name|id>]
overlay list [--integration <name|id>]
overlay get <name|id>
overlay create <name> <integration-name|id> [options]
overlay update <name|id> [options]
overlay delete <name|id>
```

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
- Overlay display names are unique within an account and identify the top-level catalog.
- An integration cannot be deleted while overlays refer to it.
- Integration deletion supports `--cascade` to delete dependent overlays.
- Integration and overlay mutations support optimistic `--etag` preconditions.
- Authentication replacement uses the dedicated `integration update-auth` command so credential
  values remain write-only.

The protobuf contracts are in `core/proto/src/main/proto/floecat/integration/`.
