# Catalog Access SPI

The catalog-access SPI is the Connector-independent boundary for communicating with upstream
catalogs. Catalog Integrations will use this boundary for connection validation, discovery, and
provider metadata access without creating or reading legacy Connector resources.

The SPI and its providers are separate Maven modules:

| Module | Responsibility |
| --- | --- |
| `core/catalog-access-spi` | Provider-neutral connection, authentication, credentials, capabilities, namespace, table, view, and client contracts. |
| `catalog-access/iceberg-rest` | Iceberg REST client provider and ServiceLoader registration. |
| `catalog-access/unity` | Unity Catalog client provider, OAuth token refresh, Delta metadata mapping, credential vending, storage validation, and ServiceLoader registration. |

Neither module depends on Connector protobufs or `FloecatConnector`.

## Connection boundary

`CatalogConnectionConfig` contains only persistable, non-secret configuration: protocol, endpoint,
properties, and `CatalogAuthentication`. Secret material is resolved immediately before opening a
client and supplied separately as `ResolvedCatalogCredentials`. Its `toString()` implementation
redacts property and header values.

```java
CatalogConnectionConfig config =
    new CatalogConnectionConfig(
        CatalogProtocol.ICEBERG_REST,
        URI.create("https://catalog.example/v1"),
        Map.of("warehouse", "sales"),
        new CatalogAuthentication(CatalogAuthenticationScheme.OAUTH2, Map.of()));

ResolvedCatalogCredentials credentials =
    new ResolvedCatalogCredentials(Map.of("token", token), Map.of(), expiresAt);

try (CatalogClient client = CatalogClientFactory.load().open(config, credentials)) {
  client.validate();
  List<NamespacePath> namespaces = client.listNamespaces(NamespacePath.root());
}
```

Provider lookup is by `CatalogProtocol`, which describes the catalog protocol/provider rather than
the table format. Missing and duplicate providers fail explicitly.

## Unity Catalog slice

The Unity Catalog provider adapts the transport-neutral client in
`connectors/clients/unity-catalog` onto the catalog-access SPI. It supports:

- bearer tokens and OAuth client credentials, with catalog-token refresh kept separate from
  storage credential vending;
- catalog/schema discovery mapped onto hierarchical namespace paths;
- Delta table and view discovery, metadata loading, and stable identity through Unity table IDs;
- table-scoped AWS credentials from Unity Catalog's temporary-table-credentials endpoint; and
- non-mutating validation of the vended credentials against the table's Delta log, without ambient
  AWS credential fallback.

The provider consumes Integration configuration directly and has no dependency on Connector
resources or `FloecatConnector`. Unity-hosted non-Delta tables are not exposed by this provider;
Iceberg catalogs use the Iceberg REST integration path.

OAuth client credentials use `oauth2-server-uri` and optional `scope` authentication properties.
When no token URI is configured, the provider resolves `/oidc/v1/token` against the catalog URI.
Provider connection properties include `http.connect.ms`, `http.read.ms`,
`unity.temporary-table-vend-path`, `s3.region`, `s3.endpoint`, and `s3.path-style-access`.

Table schemas reported through this slice are the ones Unity holds, not the ones the Delta log
holds. The Unity Delta *Connector* prefers the log for a table with a storage location and falls
back to Unity's column list only when the log will not read, because for an externally written
table Unity's columns can lag the log. This provider has no Delta reader, so an Overlay-materialized
table can report an older schema than the Connector-imported copy of the same upstream table, and
the Overlay is the stale side. Closing the gap means reading the log from `catalog-access/unity`;
until then, treat overlay schemas as catalog-reported.

## Iceberg REST slice

The Iceberg REST provider currently supports:

- anonymous, OAuth2, and AWS SigV4 connections;
- connection validation through a real namespace-list operation;
- structured, case-preserving namespace discovery;
- table enumeration and provider-neutral table metadata;
- view enumeration and provider-neutral output schema, SQL dialect, default namespace, properties,
  and identity metadata;
- metadata and storage locations when exposed by Iceberg;
- table-scoped storage credentials obtained only from Iceberg's dedicated protocol vending channel;
- non-mutating validation of storage access through an upstream table metadata-file read using the
  exact vended credentials returned to the caller, without ambient credential fallback; and
- idempotent ownership of the underlying REST session.

`vendStorageCredentials` performs a fresh `loadTable` request on every call and copies only the S3
storage credential allowlist. Among the credentials covering the table location it selects the
longest prefix, but only complete renewable sessions are candidates: a longer-prefix credential
missing a session token or a parseable expiry loses to a shorter complete one. A Catalog Integration
vends only when what it holds is itself a temporary session, and Floecat does not narrow a
credential with STS, so a bare key pair is refused rather than returned. Callers reacquire
credentials at the returned expiry, which this provider always supplies; the SPI itself still
permits an absent expiry and `CatalogClient.vendStorageCredentials` requires callers to handle one.

A response the provider cannot use is reported rather than returned empty, because an Integration
has no storage authority to fall back to. `INVALID_CONFIGURATION` covers an incomplete tuple, a
covering credential that is not a renewable session, and an expiry that does not parse;
`CREDENTIAL_SCOPE_INVALID` covers a response whose credentials cover no part of the table location.
An empty result still means "this catalog does not vend", and catalog tokens and ordinary FileIO
properties are never treated as vended storage credentials -- there is deliberately no fall-back to
the table's FileIO properties, which cannot distinguish a credential vended for the table from one
merged in from the client's own configuration.

Two pieces of SPI surface support this. `StorageLocations` holds the one prefix-comparison rule
shared by providers and callers; it is textual, matching Iceberg's own
`S3FileIO.clientForStoragePath`, and deliberately differs from
`StorageAuthorityResolver.matchesLocationPrefix`, which is path-boundary strict because it decides
what Floecat authorizes rather than what an upstream credential applies to. Never be stricter than
the component that will use the credential. `CatalogAccessException.Code.CREDENTIAL_UNAVAILABLE`
separates "the credentials are configured but momentarily unresolvable" -- the window while a stored
secret generation is superseded, which a retry clears -- from `INVALID_CONFIGURATION`, which will
stay wrong until someone changes it.

### Renewable AWS credentials

SigV4 catalog signing and storage/FileIO access are separate credential scopes. Either scope can
use static resolved keys or a renewable registration. Renewable registrations are process-local,
opaque, and owned by the caller:

```java
try (var catalogRegistration =
        RefreshingAwsCredentialsRegistry.register(initialCatalogCredentials, catalogRefresher);
    var storageRegistration =
        RefreshingAwsCredentialsRegistry.register(initialStorageCredentials, storageRefresher)) {
  Map<String, String> resolved = new HashMap<>();
  resolved.putAll(
      RefreshingAwsCredentialsRegistry.propertiesFor(
          catalogRegistration, AwsCredentialScope.CATALOG));
  resolved.putAll(
      RefreshingAwsCredentialsRegistry.propertiesFor(
          storageRegistration, AwsCredentialScope.STORAGE));

  try (CatalogClient client =
      factory.open(config, new ResolvedCatalogCredentials(resolved, Map.of(), null))) {
    client.validate();
  }
}
```

The registry refreshes expiring credentials with adaptive skew and refreshes credentials without a
reported expiry on the configured refresh cadence. It serializes concurrent refresh for a
registration, retains still-valid credentials after a transient refresh failure, caches terminal
refresh failures, and stops resolving a registration after it is closed. Logs and object string
representations use hashed provider references and never include keys or tokens.

The Iceberg REST auth manager replaces storage provider configuration with the catalog-scoped
provider only while signing REST requests. FileIO retains the separately configured storage scope,
including credentials vended through later Iceberg table configuration.

Iceberg REST tables use the Iceberg table UUID as their stable external identity when table metadata
exposes it. Tables without an available UUID use a path-derived, explicitly unstable fallback; a
rename of such a table is interpreted as deletion plus creation.

OAuth secrets, AWS keys, renewable-provider IDs, and Iceberg credential-provider hooks are rejected
in persistable connection configuration and accepted only through `ResolvedCatalogCredentials`.
Persisted secret-bearing properties, HTTP headers, endpoint user-info, endpoint fragments, and
secret-bearing endpoint query parameters are rejected. Configuration string representations print
property names but redact their values and query values. Unsupported authentication schemes,
incomplete or unknown credential properties, and endpoint protocols fail instead of falling back
to Connector behavior.

## Current boundary

The service resolves persisted Catalog Integration OAuth, bearer, and explicit static SigV4
credentials onto this SPI. Iceberg REST and Unity Catalog have registered providers. The service
exposes read-only RPCs for full connection validation, direct-child
namespace listing, and lightweight table/view listing. Validation succeeds only after catalog
connection, authentication, discovery, credential vending, and a non-mutating storage read all
pass. Authentication, expiry, scope, and storage failures remain distinct in the public validation
result.

Discovery results are not persisted and do not materialize Floecat resources. Pagination is applied
to deterministic provider inventories and continuation tokens are bound to the Integration pointer
and credential generation, namespace, and object filter.

The synchronous Catalog Overlay reconciliation RPC uses the same Integration adapter to materialize
selected namespaces, table definitions, and views as ordinary Floecat resources. Durable scheduling
and snapshot/file capture remain separate changes.

The legacy Connector remains operational and unchanged until its explicit migration/removal change.
The Integration adapter and discovery implementation do not import, call, or fall back to it.

See [Catalog Integration Architecture Decisions](catalog-integration-design.md) for ownership,
mapping, reconciliation, and migration decisions.
