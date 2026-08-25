# Catalog Integration Architecture and Delivery Plan

Status: accepted architecture and delivery contract for the Catalog Integration stack and its
metadata-reconciliation follow-on.

This document defines the target architecture and the boundary of each change in the initial stack.
It separates that accepted delivery scope from architecture and behavior that remain deferred.

## What this work is

Catalog Integration and Catalog Overlay split today's `Connector` into its two independent halves:

| Connector concern | Replacement |
| --- | --- |
| `uri`, connection properties, `auth`, `kind` — how Floecat reaches and authenticates to an upstream catalog | `CatalogIntegration` |
| `source` — which upstream namespaces are selected | `CatalogOverlay` |
| `destination` — where the selection lands in Floecat | `CatalogOverlay`, through a reference to an existing `Catalog` |
| `policy` — refresh cadence, retention, pause | Overlay demand or a referenced policy, deferred from the initial API |
| scheduler state — leases, attempts, progress, last success | Integration-owned scheduler state, separate from the Overlay resource |

Connectors will be deprecated and replaced by these two resources. One integration can serve many
overlays, and overlays from one or more integrations can target the same Floecat catalog. This is the
reuse a single `Connector` cannot express: today every destination needs its own connector, and
therefore its own copy of the endpoint and credentials.

This is a decomposition of the connector resource, not a new metadata architecture. Captured
metadata continues to materialize into the existing `Catalog` / `Namespace` / `Table` / `View` /
`Snapshot` resources, and every downstream consumer — name resolution, the metagraph, `TableRoot`
and pins, statistics visibility, garbage collection, transactions, and the Iceberg REST gateway —
continues to read those resources unchanged. Nothing in this work introduces a parallel object
hierarchy, a second table identity, or a second name-resolution path.

## Initial stack delivery boundaries

| Change | Boundary | Explicitly deferred |
| --- | --- | --- |
| This document | Architecture decisions and delivery boundaries | Production behavior |
| Catalog graph view rename | Rename the existing metadata graph `CatalogOverlay` contract to `CatalogGraphView`, freeing `CatalogOverlay` for the mapping resource | Resource or persistence behavior |
| Durability primitives | Reserved resource identity, receipt-in-batch idempotency, companion pointer operations, mutation-store reads, and strongly consistent lifecycle reads | Integration and Overlay resources |
| Account deletion fence | Durable account-deletion fencing and resumable descendant cleanup | Integration- and Overlay-specific CRUD |
| Catalog Integration API | Integration CRUD, typed authentication, write-only credential storage and rotation, dependencies, cascade deletion, idempotency, and optimistic concurrency | Upstream connectivity, provider discovery, validation RPCs, capture, reconciliation, and query visibility |
| Catalog Overlay API | Overlay CRUD, references to existing target catalogs, namespace selection, Integration and Catalog dependencies, contribution lifecycle, idempotency, and optimistic concurrency | Capture, reconciliation, and query visibility |
| Shell CLI | Integration and Overlay CRUD commands, target-Catalog binding, typed authentication input, write-only credential input, authentication rotation, pagination, name resolution, namespace filters, cascade, and etag preconditions | Connectivity validation, discovery, capture, reconciliation, and query visibility |
| Catalog-access SPI | Connector-independent catalog client SPI plus an Iceberg REST provider with validation, discovery, table and view loading, OAuth2, SigV4, and renewable AWS credential support | Wiring Catalog Integration resources to the SPI, Integration validation/listing RPCs, persistence, scheduling, and capture |
| Integration validation and discovery | Wire persisted Integration credentials to the SPI; validate catalog authentication, credential vending, and storage access; lazily list upstream namespaces, tables, and views | Persistence of discovery results, Overlay reconciliation, scheduling, and capture |
| Overlay metadata reconciliation | A synchronous Overlay RPC wires persisted Integration credentials to the SPI and materializes selected namespaces, table definitions, and views into an existing Catalog with generation fencing, collision checks, and contribution-scoped cleanup | Durable scheduling, snapshot/file capture, validation, and statistics |

## Goals

The catalog integration model provides a way to:

- authenticate Floecat to an upstream catalog once, and reuse that connection across many overlays;
- select upstream namespaces and expose them in an existing Floecat catalog;
- combine non-conflicting contributions from multiple overlays and integrations in one catalog while
  retaining independently managed catalog content;
- discover and validate upstream namespaces, tables, and views without depending on Connector
  resources; and
- materialize external namespace, table-definition, and view metadata into ordinary Floecat
  resources on demand, then add durable snapshot/file capture and scheduling separately.

The Integration path will not use, wrap, synthesize, or fall back to Connector resources.
Connectors will remain a separate operational path until they are retired through an explicit
later migration, not through runtime fallback.

## Resource and ownership model

```text
CatalogIntegration "prod-glue"          connection and authentication
 └── CatalogOverlay "crm-import"        selection -> Catalog "analytics"

CatalogIntegration "finance-unity"      a different connection and authentication
 └── CatalogOverlay "finance-import"    selection -> Catalog "analytics"

Catalog "analytics"                     independently managed target
 ├── Namespace ["crm"]                  materialized contribution from "crm-import"
 ├── Namespace ["finance"]              materialized contribution from "finance-import"
 └── Namespace ["local"]                directly managed Floecat content
```

An overlay is a mapping resource that references exactly one Integration and one existing Floecat
`Catalog`. It does not own the target Catalog. Multiple overlays may target the same Catalog, and
the Catalog retains its independent identity, name, writability, and lifecycle. An overlay owns only
the materialized contributions produced by its mapping. Name resolution, listing, pinning, planning,
and garbage collection operate on ordinary Catalog, Namespace, Table, View, and Snapshot resources
with no overlay-specific read path.

### Catalog integration

`CatalogIntegration` owns the upstream protocol, HTTP(S) endpoint, non-secret provider connection
properties, authentication configuration, and credential lifecycle. Connection properties carry
protocol-specific values such as the Iceberg REST warehouse; secret-bearing properties are rejected.
Its immutable resource ID is operational identity; its display name is mutable metadata and must be
unique among integrations in the account.

The API initially supports Iceberg REST and Unity integration types. Authentication is required and
represented by typed protobuf messages for OAuth client credentials, bearer tokens, AWS assume role,
AWS access keys, and AWS SigV4. Unity initially accepts only OAuth client credentials or bearer
authentication. The base service validates structural compatibility; endpoint and provider support
remain the responsibility of the catalog-access adapter and provider.

Integration type is immutable through update. The catalog URI and connection properties are mutable
with an etag precondition; publishing either update advances the Integration generation so
reconciliation and durable work that observed the previous connection configuration cannot publish
afterward. Existing overlays and materialized resource identities remain attached to the same
Integration. A replacing create is required only to change the Integration type, produces a new
resource identity, and is rejected while overlays depend on the existing Integration because
replacement must not silently retarget those overlays.

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
`CatalogIntegrationCredentialStore` provides the typed service-side resolution primitive used by
the catalog-access adapter.

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

`CatalogOverlay` binds one Integration and one selection of upstream namespaces to an existing
Floecat `Catalog`. It is the direct replacement for the connector's `source` plus `destination`
pair. The target is explicit because Catalog lifecycle remains independent from Overlay lifecycle.

```text
CatalogOverlay
  display_name         independently managed Overlay name
  integration_id       which upstream catalog and credentials to use (immutable through update)
  catalog_id           existing Floecat destination (immutable through update)
  include_namespaces   upstream selection
  exclude_namespaces   upstream selection
```

Creating an overlay verifies that both referenced resources exist in the same account and publishes
dependencies on the Integration and target Catalog atomically with the Overlay. A Catalog cannot be
deleted while an Overlay references it. Renaming either resource does not rename the other because
the binding uses immutable resource identity rather than display name. Overlay display names remain
unique among Overlays in the account but do not reserve Catalog names.

The target Catalog remains directly manageable. Clients may create local namespaces and relations in
it, and other overlays may contribute non-conflicting resources. Materialized Tables and Views carry
typed Integration and Overlay provenance and reject direct mutation while managed by the Overlay.
Namespaces are shared structural containers rather than exclusively Overlay-owned resources.
Namespace claim state records every Overlay using a path and whether the container was originally
created by Overlay materialization. A claim prevents structural deletion or movement of the
Namespace while the Overlay uses it, but does not make the Namespace or Catalog generally read-only.
Releasing a claim deletes the Namespace only when no other Overlay claims it, it contains no
relations, and it was created for Overlay materialization; a pre-existing or directly managed
Namespace is never deleted merely because an Overlay stops using it.

A destination relation path has one writer. Reconciliation fails with a conflict rather than
overwriting a local relation or a relation managed by another Overlay. Selecting the same upstream
object through two Overlays is valid when they materialize to different Catalogs; after namespace
remapping is introduced, distinct destination paths will also be valid. Selecting it into the same
destination path is a mapping conflict. Namespace containers may be shared as long as their child
relation names do not collide.

The Integration and Catalog bindings are immutable through update because changing either retargets
everything the Overlay has materialized. A replacing create produces a new Overlay identity and may
choose different bindings after the previous Overlay's contributions are retired.

Deleting an Overlay installs a deletion fence, removes only Tables, Views, snapshot state, and
namespace claims managed by that Overlay, then removes its dependencies and resource record. It
never deletes the target Catalog or unrelated content.

## Namespace selection

Selection is expressed as include and exclude namespace paths in the upstream catalog's namespace
space.

- Paths are ordered from external catalog root to namespace leaf and matched case-sensitively.
- An empty include list selects the whole external namespace tree.
- An included path selects that namespace and all descendants.
- An excluded path removes that namespace and all descendants; exclusion wins when both match.
- Paths are normalized and deduplicated without flattening segment boundaries.
- A selected upstream namespace maps to the same path in the target Catalog, preserving segment
  boundaries. Namespace remapping is deferred.
- Newly discovered namespaces matching the stored selection materialize on the next synchronous
  reconciliation.

Selection is updatable through the administrative API. A client that can only express selection at
creation time changes it by replacing the overlay, which produces a new identity under the same
name. Reconciliation makes the selected namespace, table-definition, and view metadata visible;
snapshot-backed table visibility remains deferred to durable capture.

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

The service adapter translates OAuth client credentials, bearer tokens, and explicit static AWS
SigV4 credentials from the Integration protobuf and `CatalogIntegrationCredentialStore` onto the
SPI's authentication schemes. Ambient and assume-role AWS resolution are not yet wired into this
adapter and fail explicitly. Integration authentication is required, so the adapter never selects
the SPI's `NONE` scheme. Dedicated Integration RPCs validate the catalog and storage credential
boundaries and provide paginated, read-only upstream namespace and object listing. Scheduling
remains deferred.

The catalog client owns external I/O only. Capture planning, Floecat persistence, scheduling, and
name resolution remain outside it.

## Materialized metadata and deferred capture

Synchronous metadata reconciliation materializes ordinary Floecat resources in the Overlay's target
Catalog. A selected upstream namespace uses or creates a `Namespace`; a selected upstream table
becomes a `Table` definition and initial `TableRoot`; and a selected upstream view becomes a `View`.
These are the same logical resources the Connector path produces today and use the existing
repositories and name-resolution paths.

This phase deliberately does not create `Snapshot` resources, file groups, validation results, or
statistics. Those require extending the durable reconciler from its current Connector-rooted job
identity to an Integration/Overlay-rooted plan. That protocol and snapshot/file capture belong in a
separate follow-on change.

Upstream identity is recorded on the existing `UpstreamRef`. The Connector path stores
`connector_id` there; the Integration path stores `catalog_integration_id` and
`catalog_overlay_id`. The provider's external identity and stability flag are stored with the
materialized object. Provider-stable IDs preserve a Floecat resource identity across an upstream
rename or namespace move; when a provider has no stable ID, the normalized full upstream path is an
explicitly unstable identity and rename is observed as delete plus create.

Two Overlays that select the same upstream table and map it to different Catalogs produce two
Floecat Tables. Once namespace remapping exists, distinct destination paths will behave the same way.
Their Snapshots, statistics, validation records, and visibility state are persisted independently
under each Floecat Table identity. No persisted Snapshot, statistics, or validation object is shared
across those Tables. Mapping both selections to the same destination relation path is a conflict, not
an implicit deduplication mechanism.

## Reconciliation publication and failure semantics

Reconciliation is generation-fenced but is not one atomic transaction over an unbounded Catalog.
Its publication order provides a monotonic safety contract:

1. Validate the Integration and complete discovery for the entire selected scope without mutating
   Floecat state.
2. Preflight the complete desired inventory against the target Catalog, including destination name
   conflicts and Overlay provenance.
3. Create or update desired namespace claims, Tables, and Views while the observed Integration and
   Overlay generations remain current.
4. Only after all desired resources are published, retire managed relations absent from that
   complete inventory, release unused namespace claims, and prune eligible empty namespace
   containers.

A validation, discovery, or preflight failure publishes nothing. A failure while publishing desired
resources may leave successful creates or updates visible, but it does not retire prior resources;
a retry converges them to the desired inventory. A failure during retirement may leave some stale
resources temporarily visible, but every retired resource was absent from a completed discovery and
all desired resources were published first. A retry completes cleanup.

"Stale" means absent from the complete inventory produced for the same Overlay selection and
Integration generation. Results from a partial discovery, a different Overlay, or an older
configuration generation can never classify a resource as stale. Stale work may finish external I/O
but cannot publish after its fence is invalidated.

## Table validation and visibility

The later durable-capture phase will make an active Overlay establish capture demand for the Tables
its selection matches. The Integration-owned scheduler computes the effective union of active
Overlay selections. The initial sharing guarantee applies only to upstream discovery and other
external I/O that can be safely coalesced; it does not share persisted Floecat objects. Any later
optimization that scans an upstream object once must still publish independently fenced results for
each destination Table. The synchronous metadata RPC in the current phase does not enqueue that
work.

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

Views have no file-validation state. Catalog-access providers remain responsible for enumerating and
loading upstream View metadata, but they do not resolve View SQL or determine whether its base Tables
are query-visible. Dependency resolution and enforcement of base-Table visibility belong to the
ordinary name-resolution and planner/runtime path, outside the Integration and Overlay contracts.

## Refresh and reconciliation ownership

Scheduling, freshness, and retention remain deferred. Overlays will state freshness, retention, and
pause demand directly or through a reusable policy; operational scheduler state is separate from the
Overlay resource. Integrations own upstream connection work. For active Overlays on one Integration,
the scheduler computes one effective plan:

- discovery scope is the union of selected namespaces, including matching tables and views, while
  file validation and statistics collection apply to the matching tables;
- refresh interval is the shortest requested interval;
- retained history satisfies the longest requested retention; and
- pausing an overlay removes its demand, while pausing an integration stops all new upstream work.

Sharing applies to upstream I/O: two Overlays selecting the same upstream namespace from one
Integration may issue one discovery pass, then materialize independently into their configured
destination paths. Persisted Tables and their descendant state remain destination-owned.

The synchronous metadata reconciler fences every resource publication on the observed Integration
and Overlay pointer generations and the absence of account, Integration, Overlay, and target Catalog
deletion markers. A future capture-plan generation must extend equivalent fencing to every planned
and leased durable job. URI, credentials, selection, pause, replacement, target deletion, or Overlay
deletion changes invalidate older work.

## Lifecycle and garbage collection

| Operation | Current behavior | Deferred behavior |
| --- | --- | --- |
| Rename integration | Atomic rename with optimistic preconditions | No additional behavior required |
| Update integration endpoint | Atomic URI update with optimistic preconditions; advances the generation that fences reconciliation while preserving dependent Overlay identities | Optional validation before publication |
| Rename overlay | Atomic Overlay-only rename with optimistic preconditions; the target Catalog name is unchanged | No additional behavior required |
| Rename target catalog | Ordinary Catalog rename; Overlay bindings remain valid by resource ID | No additional behavior required |
| Replace integration | New identity; rejected while dependent overlays exist | Validation may run before publication |
| Rotate credentials | Atomic new credential generation; old generation retired after publication | Reclaim acknowledgement-uncertain orphan generations |
| Create or replace overlay | Attaches to an existing Catalog; atomically stores the selection plus Integration and Catalog dependencies; replacement fences and retires only the prior Overlay's contributions | Trigger or join durable snapshot/file capture into the target Catalog |
| Reconcile overlay | Completes discovery and collision preflight, publishes desired namespace claims, Table definitions, and Views, then retires stale managed contributions behind generation fences | Schedule reconciliation and capture Snapshots, files, validation, and statistics |
| Delete overlay | Installs an Overlay deletion fence, explicitly retires its materialized relations and namespace claims, then removes the Overlay dependencies, resource, and fence; the target Catalog remains | Cancel and drain durable jobs |
| Delete integration | Rejected while overlays exist | Fence integration-owned jobs |
| Delete target catalog | Rejected while Overlays reference it, even when it is otherwise empty | Fence Catalog-targeted jobs |
| Cascade integration delete | Durable Integration and Overlay deletion fences, explicit retirement of each dependent Overlay's contributions, Overlay and Integration resource deletion, and credential cleanup; target Catalogs remain | Cancel and drain durable jobs |
| Delete account | Durable deletion fence, then cleanup of overlays, integrations, and integration credentials | No additional behavior required |

Overlay lifecycle code explicitly deletes its logical `Table` and `View` resources, releases its
Namespace claims, prunes only eligible empty Overlay-created Namespaces, and purges Table
snapshot/root pointer state. It never deletes the target Catalog, another Overlay's resources, or
directly managed content. Existing pointer and CAS-blob garbage collection remains responsible for
reclaiming unreachable immutable blobs. Garbage collection is not a substitute for retiring the
logical resource pointers. Validation-error records will be retained with their Table and atomically
replaced or removed when a later validation generation is published.

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
- **Fenced dependencies and cascades.** Integration and target Catalog dependency counts, cascade
  deletion, and lifecycle markers are asserted inside the same pointer transaction as the mutation
  they guard, so a concurrent Overlay create cannot slip beneath a delete that has already checked
  for dependents.
- **Strongly consistent reads where absence is load-bearing.** A cascade or dependency check that
  misses a row produces an orphan or a wrongly permitted delete, so those paths do not read
  secondary indexes through an eventually consistent path.
- **Mutation reads on raw stores.** Prerequisite and post-commit reads inside a mutation protocol
  go to the stores being mutated, never through caches or read adapters.

The cost is real and concentrated in shared code rather than in the new resources: most of it lands
in the generic resource repository, the idempotency guard, account deletion, and the storage read
interfaces. Three consequences are large enough to describe separately.

### Delivery sequencing

The durability primitives land as their own reviewed change before the account-deletion fence and
the Integration and Overlay resources that depend on them. Keeping this cross-cutting work separate
makes its effect on every resource type independently reviewable and keeps the resource/API changes
focused on their own contracts.

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

SPI implementations must define the variants explicitly. Backends whose ordinary reads are already
strongly consistent may delegate explicitly; `storage/aws` requests DynamoDB consistent reads.

## Create-conflict behavior

Create requests carry an explicit conflict mode so a caller can choose between failing, replacing,
and returning the existing resource:

- **error if exists** — the default; a name collision is an error.
- **replace** — publishes a new resource identity under the same name, atomically retiring the old
  one. Replacement is itself state-idempotent and cannot be combined with an idempotency key.
- **return existing** — returns the current resource unchanged when the name is already taken.

Replacement rather than update is how a caller changes an Overlay's immutable Integration or target
Catalog binding, or an Integration's type. An Integration URI is ordinary mutable configuration and
does not require replacement.

## Follow-on API evolution

After the initial delivery, follow-on changes will:

1. Add validation, capability, namespace-listing, and object-listing RPCs. Validation exercises both
   catalog authentication and usable storage-credential vending; object listing distinguishes
   namespaces, tables, and views.
2. Add synchronous, generation-fenced Overlay metadata reconciliation into an existing target
   Catalog. This materializes Namespace claims, Table definitions, and Views, but not Snapshots or
   files.
3. Extend the durable reconciler with Integration/Overlay-rooted jobs and capture `Snapshot`, file,
   validation, and statistics state beneath the already materialized tables.
4. Add Overlay freshness, retention, and pause demand, separate scheduler runtime state, Table
   validation-error persistence, visibility and override handling, and Integration-owned scheduling
   with durable job-generation fencing.
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
The Overlay references the Connector's existing destination Catalog; no Catalog ownership transfer
occurs. Existing Connector-managed Tables and their descendants may be adopted in place by replacing
their Connector provenance with Integration and Overlay provenance during the separately reviewed
migration, so they need not be re-materialized.

## Initial CLI name resolution

The CLI will initially resolve Integration and Overlay display names by listing the corresponding
resource type. The duplication will remain isolated and will not require expanding the API until
another consumer needs a generic resolver.
