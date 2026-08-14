# Catalog Access SPI

The catalog-access SPI is the Connector-independent boundary for communicating with upstream
catalogs. Catalog Integrations will use this boundary for connection validation, discovery, and
provider metadata access without creating or reading legacy Connector resources.

The SPI and its first provider are separate Maven modules:

| Module | Responsibility |
| --- | --- |
| `core/catalog-access-spi` | Provider-neutral connection, authentication, credentials, capabilities, namespace, table, view, and client contracts. |
| `catalog-access/iceberg-rest` | Iceberg REST client provider and ServiceLoader registration. |

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
  provider-configured, vended credential path; and
- idempotent ownership of the underlying REST session.

`vendStorageCredentials` performs a fresh `loadTable` request on every call, selects the
longest-prefix credential matching the table location, and copies only the S3 storage credential
allowlist. Callers reacquire credentials at the returned expiry. A missing or invalid expiry means
the result must not be cached. Catalog tokens and ordinary FileIO properties are never treated as
vended storage credentials, so a server that does not vend returns an empty result instead of
falling back to catalog authentication, configured storage keys, or Connector behavior.

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
credentials onto this SPI. It exposes read-only RPCs for full connection validation, direct-child
namespace listing, and lightweight table/view listing. Validation succeeds only after catalog
authentication, discovery, credential vending, and a non-mutating storage read all pass.

Discovery results are not persisted and do not materialize Floecat resources. Pagination is applied
to deterministic provider inventories and continuation tokens are bound to the Integration pointer
and credential generation, namespace, and object filter. Scheduling, reconciliation, and captured
resource writes remain separate changes.

The legacy Connector remains operational and unchanged until its explicit migration/removal change.
The Integration adapter and discovery implementation do not import, call, or fall back to it.

See [Catalog Integration Architecture Decisions](catalog-integration-design.md) for ownership,
mapping, reconciliation, and migration decisions.
