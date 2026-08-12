# Catalog Integration Architecture and Delivery Plan

Status: proposed architecture and delivery plan for Catalog Integration.

This document defines the target architecture and planned boundary of each delivery phase. It
separates the initial delivery scope from work that remains deferred.

## What this work is

Catalog Integration and Catalog Overlay split today's `Connector` into its two independent halves:

| Connector concern | Replacement |
| --- | --- |
| `uri`, `auth`, `kind` — how Floecat reaches and authenticates to an upstream catalog | `CatalogIntegration` |
| `source` — which upstream namespaces are selected | `CatalogOverlay` |
| `destination` — where the selection lands in Floecat | `CatalogOverlay`, through the catalog it owns |
| `policy`, `state` — refresh cadence, retention, pause | `CatalogOverlay` |

Connectors will be deprecated and replaced by these two resources. One integration can serve many
overlays, which is the reuse a single `Connector` cannot express: today every destination needs its
own connector, and therefore its own copy of the endpoint and credentials.

This is a decomposition of the connector resource, not a new metadata architecture. Captured
metadata continues to materialize into the existing `Catalog` / `Namespace` / `Table` / `View` /
`Snapshot` resources, and every downstream consumer — name resolution, the metagraph, `TableRoot`
and pins, statistics visibility, garbage collection, transactions, and the Iceberg REST gateway —
continues to read those resources unchanged. Nothing in this work introduces a parallel object
hierarchy, a second table identity, or a second name-resolution path.

## Planned delivery boundaries

| Change | Planned boundary | Explicitly deferred |
| --- | --- | --- |
| This document | Architecture decisions and delivery boundaries | Production behavior |
| Catalog Integration and Overlay APIs | CRUD resources, typed authentication, write-only credential storage and rotation, overlay-owned catalog lifecycle, idempotency, optimistic concurrency, dependencies, cascade deletion, and atomic persistence primitives | Upstream connectivity, provider discovery, validation RPCs, capture, reconciliation, and query visibility |
| Shell CLI | Integration and Overlay CRUD commands, typed authentication input, write-only credential input, authentication rotation, pagination, name resolution, namespace filters, cascade, and etag preconditions | Connectivity validation, discovery, capture, reconciliation, and query visibility |
| Catalog-access SPI | Connector-independent catalog client SPI plus an Iceberg REST provider with validation, discovery, table and view loading, OAuth2, SigV4, and renewable AWS credential support | Wiring Catalog Integration resources to the SPI, Integration validation/listing RPCs, persistence, scheduling, and capture |

## Goals

The catalog integration model provides a way to:

- authenticate Floecat to an upstream catalog once, and reuse that connection across many overlays;
- select upstream namespaces and expose them beneath a single top-level catalog name;
- discover and validate upstream namespaces, tables, and views without depending on Connector
  resources; and
- eventually capture external metadata into ordinary Floecat resources on a schedule the overlay
  states.

The Integration path will not use, wrap, synthesize, or fall back to Connector resources.
Connectors will remain a separate operational path until they are retired through an explicit
later migration, not through runtime fallback.

## Resource and ownership model

```text
CatalogIntegration "prod-glue"     connection and authentication
 ├── CatalogOverlay "crm_data"     upstream selection -> owns Catalog "crm_data"
 └── CatalogOverlay "finance"      a different selection -> owns Catalog "finance"

Catalog "crm_data"                 owned by the overlay, read-only to clients
 └── Namespace ["sales"]           materialized from the overlay's selection
      └── Table / View             ordinary resources with snapshots and statistics
```

An overlay is a mapping resource that owns exactly one Floecat `Catalog`. The overlay's display name
is that catalog's display name, so an overlay contributes one top-level catalog name and its
selected upstream namespaces appear beneath it as ordinary namespaces. Name resolution, listing,
pinning, planning, and garbage collection all operate on those resources with no overlay-specific
path.

### Catalog integration

`CatalogIntegration` owns the upstream protocol, HTTP(S) endpoint, non-secret authentication
configuration, and credential lifecycle. Its immutable resource ID is operational identity; its
display name is mutable metadata and must be unique among integrations in the account.

The API initially supports Iceberg REST and Unity integration types. Authentication is required and
represented by typed protobuf messages for OAuth client credentials, bearer tokens, AWS assume role,
AWS access keys, and AWS SigV4. Unity initially accepts only OAuth client credentials or bearer
authentication. The base service validates structural compatibility; endpoint and provider support
remain the responsibility of the catalog-access adapter and provider.

Type and catalog URI are immutable through update. A replacing create produces a new resource
identity, and is rejected while overlays depend on the existing integration because replacement must
not silently retarget those overlays.

Integration type identifies the catalog access protocol; it does not define the format of every
table in that catalog. As in the existing Connector path, the catalog-access provider determines
each table's `TableFormat`, and capture persists that value on the existing table protobuf at
`Table.upstream.format`. Table format therefore remains table-owned metadata and is not stored or
inferred as an Integration-wide property.

### Authentication and credentials

Persisted resources contain only non-secret authentication configuration,
`credentials_configured`, and a server-managed credential generation. Secret values are supplied on
create or through the dedicated authentication-update RPC and are never returned.

The service derives its internal secret key from integration ID and credential generation. No
secret-manager reference is exposed in the public resource.
`CatalogIntegrationCredentialStore` provides the typed service-side resolution primitive required by
a later catalog-access adapter.

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
catalog-access provider requests vended credentials through the provider protocol and uses them for
object-storage access. It must not fall back to Connector credentials, ambient service credentials,
or the credentials used to authenticate to the catalog API. Vended credentials may be scoped to a
table, path, or operation and must be reacquired or renewed according to their expiry. They are
process-local runtime material: they are not written to the Integration resource,
`CatalogIntegrationCredentialStore`, table protobufs, logs, or persisted catalog-client
configuration.

Integration validation tests the two credential boundaries independently. It verifies that the
catalog endpoint is reachable and accepts the configured Integration authentication, then uses a
provider-specific, non-mutating operation to obtain vended credentials and prove they can access the
referenced object storage. A validation result must not report success when vending was skipped or
could not be verified. Authentication, vending, expiry, scope, and storage-access failures are
reported as separate error entries.

### Catalog overlay

`CatalogOverlay` binds one integration to one selection of upstream namespaces, and owns the Floecat
`Catalog` those namespaces are exposed beneath. It is the direct replacement for the connector's
`source` plus `destination` pair; the overlay does not carry a separate destination because the
catalog it owns is the destination.

```text
CatalogOverlay
  display_name         also the display name of the catalog it owns
  integration_id       which upstream catalog and credentials to use (immutable through update)
  include_namespaces   upstream selection
  exclude_namespaces   upstream selection
```

The overlay's resource state is the mapping; the catalog it owns is where captured metadata lands.
Creating an overlay creates that catalog in the same atomic pointer transaction, so an overlay can
never exist without its catalog and a partially created overlay cannot leave an orphan catalog
behind. Renaming an overlay renames its catalog in the same transaction.

Because the owned catalog is an ordinary `Catalog`, top-level name uniqueness is the existing
catalog by-name uniqueness. No separate top-level name reservation is required, and an overlay and a
directly created catalog cannot share a name for the same reason two catalogs cannot.

An overlay-owned catalog is read-only to clients: it is created, renamed, and deleted only through
its overlay, and the catalog API rejects direct mutation of it and of the namespaces, tables, and
views captured beneath it. This reuses the existing structural write guard rather than introducing a
second writability mechanism.

The integration binding is immutable through update, because changing it retargets everything the
overlay has materialized. A replacing create produces a new overlay identity and may choose a
different binding.

Deleting an overlay deletes the catalog it owns and everything captured beneath it. That cascade is
the overlay's own contract, not a side effect: an overlay's contents exist only because the overlay
selected them.

## Namespace selection

Selection is expressed as include and exclude namespace paths in the upstream catalog's namespace
space.

- Paths are ordered from external catalog root to namespace leaf and matched case-sensitively.
- An empty include list selects the whole external namespace tree.
- An included path selects that namespace and all descendants.
- An excluded path removes that namespace and all descendants; exclusion wins when both match.
- Paths are normalized and deduplicated without flattening segment boundaries.
- A selected upstream namespace materializes as a Floecat `Namespace` at the same path beneath the
  overlay's catalog, preserving segment boundaries.
- Newly discovered namespaces matching the stored selection become eligible automatically once
  discovery is wired to overlays.

Selection is updatable through the administrative API. A client that can only express selection at
creation time changes it by replacing the overlay, which produces a new identity under the same
name. The initial delivery phases do not make the selected namespaces query-visible.

## Catalog access boundary

The implemented contracts and provider behavior are documented in
[Catalog Access SPI](catalog-access-spi.md).

The catalog-access SPI phase introduces the Connector-independent boundary defined by this design:

```text
CatalogConnectionConfig
CatalogAuthentication
ResolvedCatalogCredentials
CatalogClient
CatalogClientProvider
CatalogClientFactory
CatalogCapabilities
```

No SPI type imports Connector protobufs or carries a Connector resource ID. Provider lookup is by
catalog protocol and fails explicitly for missing or duplicate providers.

The Iceberg REST provider provides:

- connection validation using a namespace-list request;
- structured namespace, table, and view enumeration;
- provider-neutral table metadata, including the provider-determined `TableFormat`, and stable
  upstream table identity when available;
- provider-neutral view metadata, including output schema, view definitions and dialects, default
  namespace/search path, properties, and stable view identity when available;
- anonymous, OAuth2, and AWS SigV4 client authentication;
- separate catalog-signing and storage credential scopes;
- acquisition and renewal of short-lived, provider-vended storage credentials; and
- renewable, process-local AWS credential registrations with serialized refresh and terminal
  failure handling.

Views are first-class catalog objects in discovery and capture. The Integration path reuses
Floecat's existing view semantics and resolution behavior rather than introducing a parallel
external-view abstraction. A provider that advertises view support must support both enumeration
and loading of a view's metadata. Providers without that capability return namespaces and tables
normally and do not advertise view support.

The SPI validates persistable configuration so secrets, credential-provider handles, user-info, and
secret-bearing headers cannot cross the configuration boundary. Secret values are supplied
separately through `ResolvedCatalogCredentials`.

The later adapter must translate the Integration protobuf authentication variants and resolved
`CatalogIntegrationCredentials` onto the SPI's authentication schemes. Integration authentication is
required, so the adapter need not select the SPI's `NONE` scheme. It must also perform
provider-specific compatibility checks. The catalog-access SPI phase does not call
`CatalogIntegrationCredentialStore`, expose Integration validation or discovery RPCs, or schedule
work.

The catalog client owns external I/O only. Capture planning, Floecat persistence, scheduling, and
name resolution remain outside it.

## Captured metadata

Capture materializes ordinary Floecat resources beneath the overlay's catalog. A selected upstream
namespace becomes a `Namespace`; a selected upstream table becomes a `Table` with its `Snapshot`
chain, file groups, and statistics; a selected upstream view becomes a `View`. These are the same
resources the Connector path produces today, written through the same repositories, with the same
`TableRoot` commit and pin semantics.

Upstream identity is recorded on the existing `UpstreamRef`. The Connector path stores
`connector_id` there; the Integration path needs the equivalent integration-rooted reference plus
the provider's stable upstream object ID when it has one. Provider-stable IDs are preferred; when a
provider has no stable ID, the normalized full upstream path is an explicitly unstable identity and
rename is observed as delete plus create.

Two overlays that select the same upstream table produce two Floecat tables, one beneath each
overlay's catalog, exactly as two connectors would today. Deduplicating captures across overlays is
not in scope: it would require a table identity separate from the `Catalog`/`Namespace` hierarchy,
which is precisely the parallel architecture this design avoids. If that reuse is wanted later, it
belongs in a separately reviewed change against the table model itself, not in the Integration path.

## Table validation and visibility

Creating or replacing an overlay establishes capture demand for the tables its selection matches.
The integration-owned scheduler triggers or joins discovery, file scanning, table validation, and
statistics collection for the effective union of active overlay selections on that integration.

A table does not become query-visible until its current capture has completed validation. Validation
compares each discovered Parquet file with the captured table format and metadata. A file that
cannot be interpreted consistently with that metadata produces one validation-error record against
the table, containing the table identity and display path, `TableFormat`, file path, and error
message. The API must expose those records for listing, one entry per current file error.

Validation errors and table visibility follow these rules:

- no current file errors makes the table query-visible;
- one or more current file errors makes the table invisible by default;
- error publication is atomic with the validation generation, so a partially published scan cannot
  expose a table or mix errors from different generations;
- a successful later validation atomically retires the previous error set and restores default
  visibility;
- an explicit per-table override makes a table query-visible despite current file errors, and the
  error records remain listable while the override is active; and
- describing an integration may request revalidation in addition to connection, authentication, and
  credential-vending checks.

Views have no file-validation state; resolving a view must still respect the visibility of its base
tables and cannot be used to bypass an invalid table's visibility gate.

## Refresh and reconciliation ownership

This behavior is deferred beyond the initial delivery. Overlays state freshness and retention
demand; integrations own upstream connection work. For active overlays on one integration, the
scheduler computes one effective plan:

- discovery scope is the union of selected namespaces, including matching tables and views, while
  file validation and statistics collection apply to the matching tables;
- refresh interval is the shortest requested interval;
- retained history satisfies the longest requested retention; and
- pausing an overlay removes its demand, while pausing an integration stops all new upstream work.

Sharing applies to upstream I/O: two overlays selecting the same upstream namespace from one
integration issue one discovery pass, then materialize into their own catalogs.

A future integration configuration generation and capture-plan generation will fence every planned
and leased job. Stale work may finish external I/O but must not publish results after configuration,
selection, pause, replacement, or deletion changes.

## Lifecycle and garbage collection

| Operation | Initial delivery behavior | Later behavior |
| --- | --- | --- |
| Rename integration | Atomic rename with optimistic preconditions | No additional behavior required |
| Rename overlay | Atomic rename of the overlay and the catalog it owns in one transaction | No additional behavior required |
| Replace integration | New identity; rejected while dependent overlays exist | Validation may run before publication |
| Rotate credentials | Atomic new credential generation; old generation retired after publication | Reclaim acknowledgement-uncertain orphan generations |
| Create or replace overlay | Creates the overlay and its catalog atomically; stores selection and integration dependency | Trigger or join capture into that catalog |
| Delete overlay | Removes the overlay, its catalog, and the integration dependency atomically | Fence jobs; retire capture demand and captured resources |
| Delete integration | Rejected while overlays exist | Fence integration-owned jobs |
| Cascade integration delete | Durable deletion fence, dependent overlay deletion, resource deletion, and credential cleanup | Also retire captured resources |
| Delete account | Durable deletion fence, then cleanup of overlays, integrations, and integration credentials | No additional behavior required |

Resources materialized by capture are ordinary Floecat resources and are collected by the existing
catalog garbage collection paths once nothing references them. Validation-error records are retained
with their table and atomically replaced or removed when a later validation generation is published.

## Durability contract

Integration-owned resources are held to a stricter durability contract than the Connector path they
replace. This is a deliberate decision, not a side effect of the decomposition, and it is intended
to become the standard the rest of the catalog moves toward rather than a local exception.

The Connector path is the baseline being improved on. It stores a connector's secret under a key
that is just the connector id, with no generation, and recovers from a failed rotation by putting
the previous value back on a best-effort basis — a restore that can itself fail or lose a race.
It does not publish an idempotency receipt atomically with the resource it created, so a retry
after an acknowledgement-uncertain create reconstructs its answer from mutable resource state.

Integration-owned resources instead guarantee:

- **Generation-reserved credentials.** Each credential version is an immutable generation reserved
  with atomic `putIfAbsent` before the resource CAS, retired only after the new generation is
  published, and deliberately retained when publication is acknowledgement-uncertain. Rotation
  never depends on a compensating write succeeding.
- **Receipt-in-batch idempotency.** A create reserves its resource identity in a durable pending
  record, then publishes the immutable success receipt in the same atomic pointer transaction as
  the resource. A replay returns the recorded answer; it never re-derives one from state that a
  later mutation may have changed.
- **Fenced dependencies and cascades.** Dependency counts, cascade deletion, and lifecycle markers
  are asserted inside the same pointer transaction as the mutation they guard, so a concurrent
  create cannot slip beneath a delete that has already checked for dependents.
- **Strongly consistent reads where absence is load-bearing.** A cascade or dependency check that
  misses a row produces an orphan or a wrongly permitted delete, so those paths do not read
  secondary indexes through an eventually consistent path.
- **Mutation reads on raw stores.** Prerequisite and post-commit reads inside a mutation protocol
  go to the stores being mutated, never through caches or read adapters.

The cost is real and concentrated in shared code rather than in the new resources: most of it lands
in the generic resource repository, the idempotency guard, account deletion, and the storage read
interfaces. Two consequences are large enough to describe separately.

### Delivery sequencing

This durability work is separable from the Catalog Integration decomposition and would have been
better delivered as its own reviewed change, landing before the resources that depend on it. It
touches shared infrastructure that every resource type uses, so reviewing it inside a feature
branch means the feature's diff carries changes whose rationale is not the feature.

Future work that raises a cross-cutting contract should land as its own change first, with the
feature that motivated it stacked on top. Where this contract is extended — to capture, scheduling,
or the remaining resource types — that extension is its own reviewed change and does not ride along
with the phase that needs it.

### Account deletion fence

Adding integrations, overlays, and their credentials to account cleanup makes account deletion a
multi-resource operation, so deleting the account record alone is no longer a sufficient contract.
Account deletion therefore installs a durable deletion fence before it removes the account record,
and clears that fence only if the account delete itself does not commit.

The fence has two consequences that reach beyond the Integration path:

- Every resource create asserts the account's fence is absent in the same atomic pointer
  transaction. A create that races an account deletion cannot leave a resource behind that cleanup
  has already walked past.
- A delete whose account record is already gone replays cleanup instead of reporting success. A
  prior delete that committed the account record and then failed part-way through descendant
  cleanup is resumable rather than permanently half-applied.

### Consistent reads for cascade operations

Cascade deletion and dependency checks must not read secondary indexes through an eventually
consistent path: a missed overlay would leave an orphan behind a deleted integration, and a missed
dependent would let a delete that should be rejected succeed. The storage SPI therefore exposes
strongly consistent variants of prefix listing and counting alongside the ordinary reads, and
cascade and dependency paths use those variants. Ordinary listing RPCs keep the default reads.

The variants are defaults on the SPI interfaces that delegate to the existing reads, so a backend
without a separate consistent-read mode inherits correct behavior. `storage/aws` overrides them
with DynamoDB consistent reads.

## Create-conflict behavior

Create requests carry an explicit conflict mode so a caller can choose between failing, replacing,
and returning the existing resource:

- **error if exists** — the default; a name collision is an error.
- **replace** — publishes a new resource identity under the same name, atomically retiring the old
  one. Replacement is itself state-idempotent and cannot be combined with an idempotency key.
- **return existing** — returns the current resource unchanged when the name is already taken.

Replacement rather than update is how a caller changes an immutable binding: the integration binding
on an overlay, and the type and URI on an integration.

## Follow-on API evolution

After the initial delivery, follow-on changes will:

1. Wire Integration resources and the typed credential resolver to the catalog-access SPI.
2. Add validation, capability, namespace-listing, and object-listing RPCs. Validation exercises both
   catalog authentication and usable storage-credential vending; object listing distinguishes
   namespaces, tables, and views.
3. Add the integration-rooted `UpstreamRef` variant and the capture adapter that materializes
   `Namespace`, `Table`, `View`, and `Snapshot` resources beneath an overlay's catalog.
4. Add overlay policy and state, table validation-error persistence, visibility and override
   handling, and integration-owned scheduling with generation fencing.
5. Add migration from Connectors and Connector removal in separately reviewed changes.

Because backwards compatibility is not a project requirement at this stage, contracts should be
replaced when semantics differ rather than accumulating aliases or hidden fallback behavior.

## Legacy Connector disposition

Connectors remain operational as a separate API while the Integration path is completed. This is
temporary coexistence, not a compatibility layer:

- Integration and Overlay services will not create or read Connector resources.
- The catalog-access SPI will not import Connector protos or delegate to Connector services.
- Future Integration validation and reconciliation must not call Connector RPCs.
- Migration and Connector removal require their own reviewed change.

There is deliberately no runtime fallback from an Integration to a Connector. Unsupported provider
or authentication combinations must fail explicitly.

Migration is mechanical because the resources are a decomposition of the connector: each connector
yields one integration from its `uri`, `auth`, and `kind`, and one overlay from its `source` and
`destination`, with connectors sharing an endpoint and credentials collapsing onto one integration.
A connector whose destination is an existing catalog migrates by making that catalog overlay-owned,
so its existing namespaces, tables, and snapshots remain valid and are not re-materialized.

## Delivery phases and boundaries

1. **Architecture and delivery plan:** define the architecture, design decisions, and delivery
   boundaries.
2. **Resource and API foundation:** add CRUD, overlay-owned catalog lifecycle,
   authentication/credential lifecycle, atomic storage, idempotency, dependency, cascade, and
   cleanup primitives.
3. **CLI surface:** add CLI coverage for those APIs, including typed authentication and write-only
   credential input for create and rotation commands.
4. **Catalog-access SPI:** add the neutral catalog-access SPI and Iceberg REST vertical slice,
   including table and view discovery and loading.
5. **Follow-on:** add the Integration-to-SPI adapter and the validation and discovery RPCs.
6. **Later:** add capture beneath overlay catalogs, table validation and error visibility,
   scheduling, query visibility, migration, and Connector removal.

The CLI will initially resolve Integration and Overlay display names by listing the corresponding
resource type. The duplication will remain isolated and will not require expanding the API until
another consumer needs a generic resolver.
