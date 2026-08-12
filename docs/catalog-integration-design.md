# Catalog Integration Architecture and Delivery Plan

Status: proposed architecture and delivery plan for Catalog Integration.

This document defines the target architecture and planned boundary of each delivery phase. It is
aligned with the Catalog Integration and Catalog Overlay SQL specification. It separates the
initial delivery scope from work that remains deferred.

## Planned delivery boundaries

| Change | Planned boundary | Explicitly deferred |
| --- | --- | --- |
| This document | Architecture decisions and delivery boundaries | Production behavior |
| Catalog Integration and Overlay APIs | CRUD resources, typed authentication, write-only credential storage and rotation, shared SQL catalog-name reservation, idempotency, optimistic concurrency, dependencies, cascade deletion, and atomic persistence primitives | Upstream connectivity, provider discovery, validation RPCs, capture, reconciliation, and query visibility |
| Shell CLI | Integration and Overlay CRUD commands, typed authentication input, write-only credential input, authentication rotation, pagination, name resolution, namespace filters, cascade, and etag preconditions | Connectivity validation, discovery, capture, reconciliation, and query visibility |
| Catalog-access SPI | Connector-independent catalog client SPI plus an Iceberg REST provider with validation, discovery, table and view loading, OAuth2, SigV4, and renewable AWS credential support | Wiring Catalog Integration resources to the SPI, Integration validation/listing RPCs, persistence, scheduling, and capture |

## Goals

The catalog integration model will provide a SQL-compatible way to:

- authenticate Floecat to an upstream catalog;
- select upstream namespaces beneath a read-only SQL catalog name;
- discover and validate upstream namespaces, tables, and views without depending on Connector
  resources; and
- eventually capture external metadata once while exposing it through one or more overlays.

The Integration path will not use, wrap, synthesize, or fall back to Connector resources.
Connectors will remain a separate operational path until they are retired through an explicit
later migration, not through runtime fallback.

## Resource and ownership model

```text
CatalogIntegration                 connection and authentication
 ├── captured namespace/table/view future shared metadata and statistics
 ├── CatalogOverlay "sales"        top-level SQL catalog and namespace selection
 └── CatalogOverlay "finance"      another projection of the same integration
```

### Catalog integration

`CatalogIntegration` will own the upstream protocol, HTTP(S) endpoint, non-secret authentication
configuration, credential lifecycle, and eventually the canonical identity of captured external
objects. Its immutable resource ID is operational identity; its display name is mutable SQL-facing
metadata.

The API will initially support Iceberg REST and Unity integration types. Authentication will be
required and represented by typed protobuf messages for OAuth client credentials, bearer tokens,
AWS assume role, AWS access keys, and AWS SigV4. Unity will initially accept only OAuth client
credentials or bearer authentication. The base service will validate structural compatibility;
endpoint/provider support will remain the responsibility of the catalog-access adapter and
provider.

Type and catalog URI will be immutable through update. `CREATE OR REPLACE` will create a new
resource identity. It will be rejected while overlays depend on the existing integration because
replacement must not silently retarget those overlays.

Integration type identifies the catalog access protocol; it does not define the format of every
table in that catalog. As in the existing Connector path, the catalog-access provider will determine
each table's `TableFormat`. Capture will persist that value on the table protobuf at
`Table.upstream.format`. Table format therefore remains table-owned metadata and will not be stored
or inferred as an Integration-wide property.

### Authentication and credentials

Persisted resources will contain only non-secret authentication configuration,
`credentials_configured`, and a server-managed credential generation. Secret values will be
supplied on create or through the dedicated authentication-update RPC and will never be returned.

The service will derive its internal secret key from integration ID and credential generation. No
secret-manager reference will be exposed in the public resource.
`CatalogIntegrationCredentialStore` will provide the typed service-side resolution primitive
required by a later catalog-access adapter.

Credential publication must provide these guarantees:

- reserve a new immutable generation with atomic `putIfAbsent` before the resource CAS;
- retire the old generation only after the new resource generation is published;
- remove the prepared secret after a definite publication failure;
- retain the secret after an acknowledgement-uncertain publication because the resource CAS may have
  succeeded; and
- permit credentials on idempotent create, exclude secret bytes from its fingerprint, and return
  the first successfully published value for that key.

Reclamation of retained, unreachable credential generations is deferred to a later
orphan-reclamation phase.

### Vended storage credentials

Integration authentication and vended storage credentials serve different boundaries. Integration
authentication authorizes Floecat to call the catalog API. The catalog then vends short-lived,
storage-scoped credentials that authorize reads of the table metadata and data files referenced by
that catalog.

Credential vending is a required Catalog Integration contract, not an optional optimization. The
catalog-access provider will request vended credentials through the provider protocol and use them
for object-storage access. It must not fall back to Connector credentials, ambient service
credentials, or the credentials used to authenticate to the catalog API. Vended credentials may be
scoped to a table, path, or operation and must be reacquired or renewed according to their expiry.
They are process-local runtime material: they will not be written to the Integration resource,
`CatalogIntegrationCredentialStore`, table protobufs, logs, or persisted catalog-client
configuration.

Integration validation will test the two credential boundaries independently. It will verify that
the catalog endpoint is reachable and accepts the configured Integration authentication, then use a
provider-specific, non-mutating operation to obtain vended credentials and prove they can access
the referenced object storage. A validation result must not report `OK` when vending was skipped or
could not be verified. Authentication, vending, expiry, scope, and storage-access failures will be
reported as separate `ERROR` rows through the SQL validation contract.

### Catalog overlay

`CatalogOverlay` will be the read-only top-level SQL catalog backed by one integration. It will not
point to, create, mutate, empty, or delete a separate Floecat `Catalog` resource. Its display name
will be the SQL catalog name.

Catalog and CatalogOverlay create, rename, replacement, and delete operations will use one shared
account-level name reservation, so they cannot expose the same top-level SQL name. An overlay will
keep an immutable integration binding through update. Replacement may choose a different binding
because it creates a new overlay identity.

The SQL mutation surface is deliberately narrower than the administrative API. SQL
`ALTER CATALOG OVERLAY` will only rename an existing overlay; it will not change the Integration
binding or the included and excluded namespace paths. SQL will change either the binding or
namespace selection through `CREATE OR REPLACE CATALOG OVERLAY`, which atomically publishes a new
overlay identity under the SQL catalog name. Replacement will retire the old overlay's bindings and
capture demand and establish them from the replacement definition.

## Namespace selection

The SQL contract's include/exclude namespace paths are the target model; they are not placeholders
for a separate prefix-remapping contract.

- Paths are ordered from external catalog root to namespace leaf and matched case-sensitively.
- An empty include list selects the whole external namespace tree.
- An included path selects that namespace and all descendants.
- An excluded path removes that namespace and all descendants; exclusion wins.
- Paths are normalized and deduplicated without flattening segment boundaries.
- Newly discovered namespaces matching the stored selection become eligible automatically once
  discovery is wired to overlays.
- Several overlays may select the same upstream object without requiring duplicate capture.

The API and CLI layers will store and may update these selections directly. That administrative
update capability will not be mapped to SQL `ALTER CATALOG OVERLAY`; SQL clients must use
`CREATE OR REPLACE` for filter changes. The initial delivery phases will not make the selected
namespaces query-visible.

## Catalog access boundary

The catalog-access SPI phase will introduce the Connector-independent boundary defined by this
design:

```text
CatalogConnectionConfig
CatalogAuthentication
ResolvedCatalogCredentials
CatalogClient
CatalogClientProvider
CatalogClientFactory
CatalogCapabilities
```

No SPI type will import Connector protobufs or carry a Connector resource ID. Provider lookup will
be by catalog protocol and will fail explicitly for missing or duplicate providers.

The Iceberg REST provider will provide:

- connection validation using a namespace-list request;
- structured namespace, table, and view enumeration;
- provider-neutral table metadata, including the provider-determined `TableFormat`, and stable
  upstream table identity when available;
- provider-neutral view metadata, including output schema, SQL representations and dialects,
  default namespace/search path, properties, and stable view identity when available;
- anonymous, OAuth2, and AWS SigV4 client authentication;
- separate catalog-signing and storage credential scopes;
- acquisition and renewal of short-lived, provider-vended storage credentials; and
- renewable, process-local AWS credential registrations with serialized refresh and terminal
  failure handling.

Views are first-class catalog objects in discovery and capture. The Integration path will reuse
Floecat's existing view semantics and query-resolution behavior rather than introduce a parallel
external-view abstraction. A provider that advertises view support must support both enumeration
and loading of a view's metadata. Providers without that capability will return namespaces and
tables normally and will not advertise view support.

The SPI will validate persistable configuration so secrets, credential-provider handles,
user-info, and secret-bearing headers cannot cross the configuration boundary. Secret values will
be supplied separately through `ResolvedCatalogCredentials`.

The later adapter must translate the Integration protobuf authentication variants and resolved
`CatalogIntegrationCredentials` onto the SPI's authentication schemes. Integration authentication
will be required, so the adapter need not select the SPI's `NONE` scheme. It must also perform
provider-specific compatibility checks. The catalog-access SPI phase will not call
`CatalogIntegrationCredentialStore`, expose Integration validation/discovery RPCs, or schedule
work.

The catalog client will own external I/O only. Capture planning, Floecat persistence, overlay
binding, scheduling, and query resolution will remain outside it.

## Canonical captured metadata

External metadata will be captured once per integration rather than copied per overlay. A
canonical key will be:

```text
(account_id, integration_id, object_kind, upstream_object_id)
```

Provider-stable object IDs are preferred. When a provider has no stable ID, the normalized full
upstream path is an explicitly unstable identity and rename is observed as delete plus create.
Display names and overlay selection are never part of canonical identity.

Canonical tables will own their provider-determined format, schemas, snapshots, manifests, file
groups, and statistics. The capture adapter will carry the format reported by the catalog-access
provider into `Table.upstream.format`; downstream planning and query paths will continue to read it
from the table protobuf. Canonical views will own their output schema, SQL definitions and dialects,
creation search path, properties, and base-relation references. Views will not own table snapshots,
manifests, file groups, or table statistics.

A lightweight overlay binding will associate a canonical table or view with its SQL-visible path.
An overlay-bound view will resolve its base relations within that overlay's projection of the same
integration, using the captured upstream namespace/search-path context. The overlay display name
will not become part of the canonical view definition or identity, allowing one captured view to be
exposed through several overlays. This requires a new integration-owned external-object reference
rather than adding an integration ID to the existing Connector-rooted `UpstreamRef`.

## Table validation and visibility

Creating or replacing an overlay will establish capture demand for the tables selected by its
namespace rules. The integration-owned scheduler will trigger or join discovery, file scanning,
table validation, and statistics collection for the effective union of active overlay selections.
An existing capture may satisfy that demand when it is current for the Integration configuration
and capture-plan generations; creating another overlay will not duplicate the scan.

A table will not become query-visible through an overlay until its current capture has completed
validation. Validation will compare each discovered Parquet file with the captured table format and
metadata. A file that cannot be interpreted consistently with that metadata will produce one
integration-owned validation-error record containing the Integration identity, canonical table
identity and display path, `TableFormat`, file path, and error message. The SQL system view
`sys.catalog_integration_table_error` will expose one row per current file error.

Validation errors and table visibility will follow these rules:

- no current file errors makes the table eligible for every matching active overlay binding;
- one or more current file errors makes the table ineligible by default through every overlay that
  references the canonical table;
- error publication is atomic with the validation generation, so a partially published scan cannot
  expose a table or mix errors from different generations;
- a successful later validation atomically retires the previous error set and restores default
  visibility without requiring overlay recreation; and
- `DESCRIBE CATALOG INTEGRATION <integration> WITH VALIDATE` will request revalidation in addition
  to connection, authentication, and credential-vending checks.

The SQL `ALTER TABLE` override defined by the SQL specification will make a table query-visible
despite current file errors. That override will be stored on the addressed overlay binding, not on
the shared canonical table, so it will not weaken the default for other overlays. The error rows
will remain visible while the override is active. Views have no file-validation state; resolving a
view must still respect the visibility of its base tables and cannot use a view to bypass an
invalid table's visibility gate.

## Refresh and reconciliation ownership

This behavior is deferred beyond the initial delivery. Overlays will state user-facing freshness
and retention demand; integrations will own upstream work. For active overlays on one integration,
the scheduler will compute one effective plan:

- discovery and capture scope is the union of selected namespaces, including matching tables and
  views, while file validation and statistics collection apply to the matching tables;
- refresh interval is the shortest requested interval;
- retained history satisfies the longest requested retention; and
- pausing an overlay removes its visibility and demand, while pausing an integration stops all new
  upstream work.

A future integration configuration generation and capture-plan generation will fence every planned
and leased job. Stale work may finish external I/O but must not publish results after configuration,
selection, pause, replacement, or deletion changes.

## Lifecycle and garbage collection

| Operation | Initial delivery behavior | Later behavior |
| --- | --- | --- |
| Rename integration or overlay | Atomic rename with optimistic preconditions and shared SQL-name checks for overlays | No additional behavior required |
| Replace integration | New identity; rejected while dependent overlays exist | Validation may run before publication |
| Rotate credentials | Atomic new credential generation; old generation retired after publication | Reclaim acknowledgement-uncertain orphan generations |
| Create or replace overlay | Stores namespace selection and integration dependency | Trigger or join validation and statistics capture; expose only validated tables |
| Delete overlay | Removes resource pointers and integration dependency atomically | Fence jobs, remove bindings, retain shared captures still in use |
| Delete integration | Rejected while overlays exist | Fence integration-owned jobs and captured publication |
| Cascade integration delete | Durable deletion fence, dependent overlay deletion, resource deletion, and credential cleanup | Remove bindings and make unreferenced captures GC-eligible |
| Delete account | Integration credentials are included in account cleanup | Include future bindings and captures |

Captured metadata GC is deferred. Future capture data will become eligible only when no retained
binding or query pin references it, no current-generation job can publish to it, and its retention
grace period has elapsed. Validation-error records will be retained with their canonical table and
atomically replaced or removed when a later validation generation is published. They will also be
removed when the canonical table becomes eligible for collection.

## Follow-on API evolution

After the initial delivery, follow-on changes will:

1. Wire Integration resources and the typed credential resolver to the catalog-access SPI.
2. Add validation, capability, namespace-listing, and object-listing service RPCs required by the
   SQL functions. Validation will exercise both catalog authentication and usable storage-credential
   vending; object listing will distinguish namespaces, tables, and views.
3. Add integration-scoped canonical object identity, validation-error persistence, and lightweight
   overlay bindings.
4. Add overlay policy/state, table visibility and override handling, and integration-owned
   scheduling with generation fencing.
5. Add query visibility, retained capture GC, credential orphan reclamation, migration, and
   Connector retirement in separately reviewed changes.

Because backwards compatibility is not a project requirement at this stage, contracts should be
replaced when semantics differ rather than accumulating aliases or hidden fallback behavior.

## Legacy Connector disposition

Connectors will remain operational as a separate API while the Integration path is completed. This
will be temporary coexistence, not a compatibility layer:

- Integration and Overlay services will not create or read Connector resources.
- The catalog-access SPI will not import Connector protos or delegate to Connector services.
- Future Integration validation and reconciliation must not call Connector RPCs.
- Migration and Connector removal require their own reviewed change.

There is deliberately no runtime fallback from an Integration to a Connector. Unsupported provider
or authentication combinations must fail explicitly.

## Delivery phases and boundaries

1. **Architecture and delivery plan:** define the architecture, design decisions, and delivery
   boundaries.
2. **Resource and API foundation:** add CRUD, SQL identity, authentication/credential lifecycle,
   atomic storage, idempotency, dependency, cascade, and cleanup primitives.
3. **CLI surface:** add CLI coverage for those APIs, including typed authentication and write-only
   credential input for create and rotation commands.
4. **Catalog-access SPI:** add the neutral catalog-access SPI and Iceberg REST vertical slice,
   including table and view discovery and loading.
5. **Follow-on:** add the Integration-to-SPI adapter and SQL-required validation/discovery RPCs.
6. **Later:** add canonical capture/bindings, table validation and error visibility, scheduling,
   query visibility, garbage collection, migration, and Connector removal.

The CLI will initially resolve Integration and Overlay display names by listing the corresponding
resource type. The duplication will remain isolated and will not require expanding the API until
another consumer needs a generic resolver.
