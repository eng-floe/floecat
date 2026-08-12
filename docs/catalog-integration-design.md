# Catalog Integration Architecture and Delivery Plan

Status: accepted; the resource, authentication, CLI, and first catalog-access layers are
implemented in the current PR stack.

This document records both the target architecture and the boundary of each stacked change. It is
aligned with the Catalog Integration and Catalog Overlay SQL specification. It distinguishes code
implemented in the following stacked changes from work that remains deferred.

## Stack implementation map

| Change | Implemented boundary | Explicitly not implemented |
| --- | --- | --- |
| #424, Catalog Integration and Overlay APIs | CRUD resources, typed authentication, write-only credential storage and rotation, shared SQL catalog-name reservation, idempotency, optimistic concurrency, dependencies, cascade deletion, and atomic persistence primitives | Upstream connectivity, provider discovery, validation RPCs, capture, reconciliation, and query visibility |
| #425, Shell CLI | Integration and Overlay CRUD commands, typed authentication input, write-only credential input, authentication rotation, pagination, name resolution, namespace filters, cascade, and etag preconditions | Connectivity validation, discovery, capture, reconciliation, and query visibility |
| #446, this document | Architecture decisions and delivery boundaries | Production behavior |
| #440, catalog-access SPI | Connector-independent catalog client SPI plus an Iceberg REST provider with validation, discovery, table loading, OAuth2, SigV4, and renewable AWS credential support | Wiring Catalog Integration resources to the SPI, Integration validation/listing RPCs, persistence, scheduling, and capture |

## Goals

The catalog integration model provides a SQL-compatible way to:

- authenticate Floecat to an upstream catalog;
- select upstream namespaces beneath a read-only SQL catalog name;
- discover and validate upstream objects without depending on Connector resources; and
- eventually capture external metadata once while exposing it through one or more overlays.

The Integration path does not use, wrap, synthesize, or fall back to Connector resources.
Connectors currently remain a separate operational path and are retired through an explicit later
migration, not through runtime fallback.

## Resource and ownership model

```text
CatalogIntegration                 connection and authentication
 ├── captured namespace/table      future shared metadata and statistics
 ├── CatalogOverlay "sales"        top-level SQL catalog and namespace selection
 └── CatalogOverlay "finance"      another projection of the same integration
```

### Catalog integration

`CatalogIntegration` owns the upstream protocol, HTTP(S) endpoint, non-secret authentication
configuration, credential lifecycle, and eventually the canonical identity of captured external
objects. Its immutable resource ID is operational identity; its display name is mutable SQL-facing
metadata.

The API currently supports Iceberg REST and Unity integration types. Authentication is required and
is represented by typed protobuf messages for OAuth client credentials, bearer tokens, AWS assume
role, AWS access keys, and AWS SigV4. Unity currently accepts only OAuth client credentials or
bearer authentication. The base service validates structural compatibility; endpoint/provider
support is intentionally left to the catalog-access adapter and provider.

Type and catalog URI are immutable through update. `CREATE OR REPLACE` creates a new resource
identity. It is rejected while overlays depend on the existing integration because replacement
must not silently retarget those overlays.

### Authentication and credentials

Persisted resources contain only non-secret authentication configuration,
`credentials_configured`, and a server-managed credential generation. Secret values are supplied
on create or through the dedicated authentication-update RPC and are never returned.

The service derives its internal secret key from integration ID and credential generation. No
secret-manager reference is exposed in the public resource. `CatalogIntegrationCredentialStore`
provides the typed service-side resolution primitive required by a later catalog-access adapter.

Credential publication has these guarantees:

- a new immutable generation is reserved with atomic `putIfAbsent` before the resource CAS;
- the old generation is retired only after the new resource generation is published;
- a definite publication failure removes the prepared secret;
- an acknowledgement-uncertain publication retains the secret because the resource CAS may have
  succeeded; and
- idempotent create may carry credentials, excludes secret bytes from its fingerprint, and returns
  the first successfully published value for that key.

Reclamation of retained, unreachable credential generations is not implemented in this stack. A
later orphan-reclamation PR will add that mechanism; this document does not claim it exists today.

### Catalog overlay

`CatalogOverlay` is the read-only top-level SQL catalog backed by one integration. It does not point
to, create, mutate, empty, or delete a separate Floecat `Catalog` resource. Its display name is the
SQL catalog name.

Catalog and CatalogOverlay create, rename, replacement, and delete operations use one shared
account-level name reservation, so they cannot expose the same top-level SQL name. An overlay keeps
an immutable integration binding through update. Replacement may choose a different binding
because it creates a new overlay identity.

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

The current API and CLI implement storage and mutation of these selections. They do not yet make
the selected namespaces query-visible.

## Catalog access boundary

PR #440 implements the Connector-independent boundary anticipated by this design:

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

The Iceberg REST provider in #440 implements:

- connection validation using a namespace-list request;
- structured namespace and table enumeration;
- provider-neutral table metadata and stable Iceberg table UUID identity when available;
- anonymous, OAuth2, and AWS SigV4 client authentication;
- separate catalog-signing and storage credential scopes; and
- renewable, process-local AWS credential registrations with serialized refresh and terminal
  failure handling.

The SPI validates persistable configuration so secrets, credential-provider handles, user-info,
and secret-bearing headers cannot cross the configuration boundary. Secret values are supplied
separately through `ResolvedCatalogCredentials`.

The remaining adapter must translate the Integration protobuf authentication variants and resolved
`CatalogIntegrationCredentials` onto the SPI's authentication schemes. Integration authentication
is currently required, so the adapter need not select the SPI's `NONE` scheme. It must also perform
provider-specific compatibility checks. #440 does not yet call
`CatalogIntegrationCredentialStore`, expose Integration validation/discovery RPCs, or schedule
work.

The catalog client owns external I/O only. Capture planning, Floecat persistence, overlay binding,
scheduling, and query resolution remain outside it.

## Canonical captured metadata

This section is target design, not current implementation. External metadata will be captured once
per integration rather than copied per overlay. A canonical key is:

```text
(account_id, integration_id, object_kind, upstream_object_id)
```

Provider-stable object IDs are preferred. When a provider has no stable ID, the normalized full
upstream path is an explicitly unstable identity and rename is observed as delete plus create.
Display names and overlay selection are never part of canonical identity.

Canonical tables will own schemas, snapshots, manifests, file groups, and statistics. A lightweight
overlay binding will associate a canonical object with its SQL-visible path. This requires a new
integration-owned external-object reference rather than adding an integration ID to the existing
Connector-rooted `UpstreamRef`.

## Refresh and reconciliation ownership

This section is also deferred target design. Overlays will state user-facing freshness and
retention demand; integrations will own upstream work. For active overlays on one integration, the
scheduler will compute one effective plan:

- capture scope is the union of selected namespaces;
- refresh interval is the shortest requested interval;
- retained history satisfies the longest requested retention; and
- pausing an overlay removes its visibility and demand, while pausing an integration stops all new
  upstream work.

A future integration configuration generation and capture-plan generation will fence every planned
and leased job. Stale work may finish external I/O but must not publish results after configuration,
selection, pause, replacement, or deletion changes.

## Lifecycle and garbage collection

| Operation | Current stack | Later behavior |
| --- | --- | --- |
| Rename integration or overlay | Atomic rename with optimistic preconditions and shared SQL-name checks for overlays | No additional behavior required |
| Replace integration | New identity; rejected while dependent overlays exist | Validation may run before publication |
| Rotate credentials | Atomic new credential generation; old generation retired after publication | Reclaim acknowledgement-uncertain orphan generations |
| Delete overlay | Removes resource pointers and integration dependency atomically | Fence jobs, remove bindings, retain shared captures still in use |
| Delete integration | Rejected while overlays exist | Fence integration-owned jobs and captured publication |
| Cascade integration delete | Durable deletion fence, dependent overlay deletion, resource deletion, and credential cleanup | Remove bindings and make unreferenced captures GC-eligible |
| Delete account | Integration credentials are included in account cleanup | Include future bindings and captures |

Captured metadata GC is not implemented. Future capture data becomes eligible only when no retained
binding or query pin references it, no current-generation job can publish to it, and its retention
grace period has elapsed.

## Remaining API evolution

The lower PRs already provide the SQL-facing CRUD and credential primitives. Remaining changes are:

1. Wire Integration resources and the typed credential resolver to the catalog-access SPI.
2. Add validation, capability, namespace-listing, and object-listing service RPCs required by the
   SQL functions.
3. Add integration-scoped canonical object identity and lightweight overlay bindings.
4. Add overlay policy/state and integration-owned scheduling with generation fencing.
5. Add query visibility, retained capture GC, credential orphan reclamation, migration, and
   Connector retirement in separately reviewed changes.

Because backwards compatibility is not a project requirement at this stage, contracts should be
replaced when semantics differ rather than accumulating aliases or hidden fallback behavior.

## Legacy Connector disposition

Connectors currently remain operational as a separate API while the Integration path is completed.
This is temporary coexistence, not a compatibility layer:

- Integration and Overlay services do not create or read Connector resources.
- The catalog-access SPI does not import Connector protos or delegate to Connector services.
- Future Integration validation and reconciliation must not call Connector RPCs.
- Migration and Connector removal require their own reviewed change.

There is deliberately no runtime fallback from an Integration to a Connector. Unsupported provider
or authentication combinations must fail explicitly.

## Delivery plan and PR boundaries

1. **Complete — #424:** CRUD, SQL identity, authentication/credential lifecycle, atomic storage,
   idempotency, dependency, cascade, and cleanup primitives.
2. **Complete — #425:** CLI coverage for those APIs, including typed authentication and write-only
   credential input for create and rotation commands.
3. **This change — #446:** keep the architecture and delivery record aligned with implemented code.
4. **Implemented later in the stack — #440:** neutral catalog-access SPI and Iceberg REST vertical
   slice.
5. **Next:** Integration-to-SPI adapter and SQL-required validation/discovery RPCs.
6. **Later:** canonical capture/bindings, scheduling, query visibility, garbage collection,
   migration, and Connector removal.

The CLI currently resolves Integration and Overlay display names by listing the corresponding
resource type. The duplication is isolated and does not require expanding the API until another
consumer needs a generic resolver.
