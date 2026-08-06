# Catalog Integration Architecture Decisions

Status: accepted for implementation after the inert Integration and Overlay CRUD foundation.

This document defines the target architecture for catalog integrations. It is intentionally a
design boundary: it fixes resource ownership, namespace mapping, capture identity, scheduling, and
lifecycle semantics before those behaviors are added to the production APIs.

## Goals

The catalog integration model will provide a simpler way to:

- authenticate Floecat to an upstream catalog;
- map upstream catalog namespaces into SQL-visible Floecat catalogs and namespaces;
- define freshness and retention requirements; and
- capture external metadata once while exposing it through one or more overlays.

The new path does not use, wrap, synthesize, or fall back to Connector resources. Connectors remain
a separate legacy path during migration and will eventually be deprecated.

## Resource and ownership model

The target model has two resource layers plus shared captured metadata:

```text
CatalogIntegration                 connection, authentication, canonical upstream identity
 ├── captured namespace/table      shared metadata, snapshots, file groups, and statistics
 ├── CatalogOverlay "sales"        top-level SQL catalog, mappings, visibility, and demand
 └── CatalogOverlay "finance"      another top-level projection of the same integration
```

### Catalog integration

`CatalogIntegration` owns the upstream catalog protocol, endpoint, authentication configuration,
credential lifecycle, and canonical identity of captured external objects. Its immutable resource
ID is used by capture and reconciliation; display names are never operational identity.

The integration type identifies a catalog protocol or provider, such as Iceberg REST, Unity
Catalog, or AWS Glue. It does not identify the table format. A provider can expose more than one
table format, and format-specific behavior belongs to the discovered table metadata and catalog
client capabilities.

Secret values are stored outside the integration protobuf. Reads expose only the configured
authentication scheme, non-secret options, whether credentials are configured, and a credential
generation. Credential writes use a dedicated request that never returns secret material.

### Catalog overlay

`CatalogOverlay` is a read-only top-level SQL catalog backed by one integration. Its display name is
the catalog name. It owns namespace mappings, visibility state, and refresh/retention demand; it
does not point to or mutate a separate `Catalog` resource.

Several overlays may expose different projections of the same integration under different
top-level catalog names. The account-level catalog namespace must reject collisions between overlay
display names and other top-level catalogs. Captured namespaces and relations inside an overlay are
read-only; writable local objects belong in a separate regular catalog.

## Namespace selection and mapping

The current include/exclude paths are sufficient for inert CRUD but not for the target product.
The behavioral contract replaces them with explicit source-to-target prefix mappings:

```proto
message NamespaceMapping {
  NamespacePath source_prefix = 1;
  NamespacePath target_prefix = 2;
  bool recursive = 3;
}

message CatalogOverlaySpec {
  optional string display_name = 1;
  ResourceId integration_id = 2;
  repeated NamespaceMapping namespace_mappings = 3;
  repeated NamespacePath exclude_namespaces = 4;
  CatalogOverlayState state = 5;
  CatalogOverlayPolicy policy = 6;
}
```

The protobuf above is illustrative. The inert CRUD field numbers remain contiguous; numbering for
new fields and request separation are finalized when the contract is implemented. The semantics
are fixed:

- Paths are ordered from catalog root to namespace leaf, trimmed, and matched case-sensitively.
- A recursive mapping maps every descendant by appending its suffix to `target_prefix`.
- A non-recursive mapping exposes only the exact source namespace.
- Exclusions are expressed in upstream namespace space and take precedence over mappings.
- Newly discovered namespaces that match an active mapping become visible automatically.
- Empty source or target prefixes represent the upstream or overlay catalog root, respectively.
- Source mappings within one overlay may not overlap. Target prefixes may not overlap. Rejecting
  ambiguity is preferable to order-dependent precedence.
- Two overlays may expose the same canonical table under different catalogs or names without
  duplicating captured metadata.

For example, mapping `production.sales` to `sales` recursively exposes
`production.sales.orders` as `sales.orders`. An exclusion for
`production.sales.private` suppresses that namespace and all descendants.

## Shared captured metadata

External metadata is captured once per integration and is not stored as catalog-owned table copies.
A canonical captured object key consists of:

```text
(account_id, integration_id, object_kind, upstream_object_id)
```

Provider-stable object IDs are preferred. When a provider has no stable ID, the normalized full
upstream path is the fallback identity and an upstream rename is observed as delete plus create.
Display names and overlay mappings are never part of canonical identity.

Canonical tables own their schemas, snapshots, manifests, file groups, and statistics. An overlay
binding is lightweight state that associates a canonical object with a path inside the overlay's
top-level catalog. Its identity includes the overlay ID, canonical object ID, and exposed path.
Query resolution returns the canonical table identity plus the binding through which it was
resolved, so several overlay catalogs can safely share one captured object and one set of
statistics.

This requires an explicit replacement for the current connector-rooted `UpstreamRef`; adding an
integration ID beside a connector ID would create two competing identities. The integration path
will use a new canonical external-object reference. Connector-backed tables retain their existing
reference until the explicit migration phase.

## Catalog access boundary

Catalog connectivity is extracted behind a neutral library before integration-driven capture is
implemented. The boundary uses catalog concepts rather than Connector RPC concepts. Its expected
shape is:

```text
CatalogConnectionConfig
CatalogAuthentication
ResolvedCatalogCredentials
CatalogClient
CatalogClientProvider
CatalogClientFactory
CatalogCapabilities
```

No type in this boundary imports Connector protobufs or carries a Connector resource ID. The first
vertical slice is Iceberg REST. The existing Iceberg Connector remains operational on its separate
Connector SPI and does not delegate to this library. Integration code likewise never delegates to a
Connector resource or service. Unity Catalog and Glue support follow as separate provider
implementations.

The client boundary covers connection validation, namespace and relation enumeration, stable
identity lookup, and provider metadata access. Capture planning, scheduling, Floecat persistence,
overlay binding, and query resolution remain outside the catalog client.

## Refresh and reconciliation ownership

Overlays state user-facing freshness and retention requirements; integrations own upstream work.
For all active overlays on an active integration, the scheduler computes one effective capture
plan:

- capture scope is the union of namespaces selected by active overlays;
- refresh interval is the shortest requested interval;
- retained history is sufficient for the longest requested retention; and
- pausing an overlay removes its visibility and policy demand, while pausing an integration stops
  all new upstream work for it.

Changing connection configuration or credentials advances an integration configuration generation.
Changing mappings, policy, or overlay state advances the effective capture-plan generation. Every
planned and leased job carries both generations. Publishing captured data or bindings is conditional
on those generations still being current. A stale job may finish external I/O, but it cannot publish
results or repopulate a changed or deleted overlay.

Reconciliation jobs and deduplication keys use integration and canonical external-object identity.
There is no Connector fallback if an integration provider is unavailable or unsupported; the
integration reports an actionable validation or reconciliation error.

## Lifecycle and garbage collection

Lifecycle operations have the following effects:

| Operation | Required behavior |
| --- | --- |
| Pause overlay | Hide its bindings and remove its policy demand; retain shared captures. |
| Delete overlay | Fence its jobs, remove its top-level catalog bindings and policy demand, and retain shared captures still in use. |
| Pause integration | Stop new work and fence publication from work planned before the pause. |
| Rotate credentials | Atomically advance the credential/configuration generation; never expose the old or new secret. |
| Delete integration | Reject while overlays exist unless explicit cascade was requested. |
| Cascade integration delete | Delete overlays, cancel/fence jobs, delete credentials, then make unreferenced captures GC-eligible. |

Captured metadata becomes eligible for garbage collection only when no active or retained overlay
binding references it, no query pin protects it, no in-flight current-generation job can publish to
it, and the configured retention grace period has elapsed. Deleting an overlay never immediately
deletes shared snapshots or statistics used by another overlay.

## Proposed API evolution

The inert CRUD resources remain the foundation. Behavioral implementation evolves them with:

1. A typed, integration-owned catalog connection and authentication contract, dedicated credential
   mutation, validation/capabilities RPCs, and credential generations.
2. Namespace mappings and overlay refresh/retention policy, replacing selection-only filters.
3. Canonical captured-resource and overlay-binding contracts independent of Connector protos.
4. Configuration and capture-plan generations used by every reconciliation job and publish step.
5. Explicit cascade requests, dependency reporting, job fencing, credential cleanup, and retained
   metadata garbage collection.

Because backwards compatibility is not a project requirement at this stage, fields should be
replaced where the semantics differ rather than preserving aliases or hidden fallback behavior.

## Legacy Connector disposition

Connectors remain operational as a separate legacy API while the new path is built. This is a
temporary coexistence decision, not a compatibility layer:

- Integration and Overlay services do not create or read Connector resources.
- Integration validation and reconciliation do not call Connector RPCs.
- New integration contracts and neutral catalog-access types do not import Connector protos.
- Existing Connector-backed tables and jobs continue to work unchanged until an explicit migration.
- Migration maps connectivity and authentication to integrations and top-level catalog
  projection/filtering to overlays, then retires Connector APIs and persisted state in a separately
  reviewed change.

There is deliberately no runtime fallback from an Integration to a Connector. Silent fallback would
make ownership, credentials, scheduling, and failure semantics impossible to reason about.

## Implementation sequence

The next changes should retain clear review boundaries:

1. Introduce the neutral catalog-access SPI and an Iceberg REST implementation while leaving the
   operational legacy Iceberg Connector on its separate Connector SPI.
2. Add integration authentication, secret persistence, credential rotation, capability discovery,
   validation, and their CLI surface. Do not schedule reconciliation yet.
3. Add integration-scoped canonical capture identity and lightweight overlay bindings, including
   collision and name-resolution tests.
4. Move scheduling and job identity to integrations with generation fencing. Do not add Connector
   fallback.
5. Complete query visibility, cascade lifecycle, garbage collection, migration, and finally
   Connector deprecation/removal in separate changes.

The Integration/Overlay CLI currently resolves display names by listing the corresponding resource
type, so its integration and overlay helpers contain similar pagination and ambiguity handling.
That duplication is small and isolated. It should remain in the inert CLI change rather than
expanding the earlier API PR. A generic resource resolver is worthwhile only when the Directory API
supports these resource kinds or another CLI resource needs the same behavior.
