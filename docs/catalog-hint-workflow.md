# Shared hint workflow

This repository now has one compute layer that produces Floe engine hints plus two sinks (scanners and decorators) that consume the same data. Here's how the flow works end-to-end:

```
          Request headers           EngineContext
        ┌──────────────┐        ┌──────────────┐
        │  planner /   │ ─────>│ EngineContext │
        │  bundle RPC  │        └──────────────┘
        └──────────────┘               │
                │                      ▼
                ▼              SystemDefinitionRegistry
   Builder fetch →            (kind-level cache + fingerprint)
                ▼                      │
                │                      ▼
        SystemNodeRegistry ─────────▶ BuiltinNodes (kind+version cache)
               /│\
              / │ \                   
             /  │  \  (FloeHintResolver is external; both scanners and decorator call it)
        Scanners Decorator ...
           │        │
  pg_* rows produced   Catalog bundle decorated
```

## 1. EngineContext and system catalog layers

`EngineContext` is built once from request headers; missing headers map to `floecat_internal`. `SystemDefinitionRegistry` caches the raw plugin snapshot per normalized kind (no provider merges). `SystemNodeRegistry` fetches that snapshot, seeds it with `FloecatInternalProvider` (the shared `information_schema`), overlays the plugin catalog when available, and, if overlays are enabled, merges any `SystemObjectScannerProvider.definitions(kind, version)` entries before filtering by `EngineSpecificMatcher`. Layer‑1 fingerprints reflect the raw snapshot, layer‑2 fingerprints reflect the merged catalog. Headers that mention an unknown engine kind still fall back to `floecat_internal`: the provider returns the fallback catalog, the cache key is the requested kind, and `SystemNodeRegistry` treats the result as `floecat_internal`, so overlays/decorators are skipped while the shared `information_schema` layer remains visible.

## 2. Shared hint resolver

`FloeHintResolver` lives under `extensions/floedb/.../hints/`. Every scanner plus the decorator calls it with:

* `MetadataResolutionContext` (which contains the active `EngineContext`, overlay map, and `CatalogOverlay`),
* the node being described (`NamespaceNode`, `FunctionNode`, `GraphNode`, `TypeNode`, `SchemaColumn`, …),
* optional resolution helpers (type resolver, parsed `LogicalType`).

The resolver:

* looks for existing Floe payloads via `ScannerUtils`.
* applies overlay values if present.
* fills deterministic defaults for OIDs, typmods, lengths, collision, etc. When overlays are missing, ScannerUtils now defers to a pluggable `EngineOidGenerator` (currently configured through `floecat.extensions.floedb.engine.oid-generator=default`), and there is a payload-aware `fallbackOid(id, payloadType)` overload so different hint categories for the same resource hash to distinct values. The default generator compresses its UUID into ~31 bits, so collisions can still occur at scale; any persistence layer that stores generated OIDs may need to detect duplicates and retry with a new payload tag or salt. If you retry with a new salt (or payload tag), be sure to persist the resulting OID alongside the object before you rely on it again—otherwise you break the deterministic guarantee that future scans expect.
* returns the canonical `Floe*Specific` proto (e.g., `FloeColumnSpecific`, `FloeRelationSpecific`).

It never mutates scan builders or catalog responses, and all callers share the same defaults.

## 3. PG scanners

Each `pg_*` scanner:

* extracts the current `MetadataResolutionContext` (and thus the normalized `EngineContext`) and parses the logical type once, reusing a cached `TypeResolver` (these resolvers are request-scoped and typically stored per relation/decoration, not globally).
* calls `FloeHintResolver` (e.g., `columnSpecific`) with the context + resolver + parsed type.
* projects the returned FloeColumnSpecific fields into the row (attname, atttypid, atttypmod, attnum, attnotnull, attisdropped, attcollation, atthasdef) and synthesizes safe defaults for the remaining PG-only columns (attlen, attalign, attstorage, attndims).

The scanner never invents OIDs/typmods itself; it relies entirely on the shared resolver. When overlays are missing, the resolver still returns safe defaults (typmod = -1, attlen = -1, attalign = 'i', attstorage = 'p', attndims = 0, attcollation = 0).

## 4. Catalog bundle decoration

When `UserObjectBundleService` builds responses it:

* collects cached relation/column metadata from `RelationDecoration` + `ColumnDecoration`.
* asks `EngineMetadataDecorator` (e.g., `FloeEngineSpecificDecorator`) for Floe hints when overlays are enabled.
* the decorator calls `FloeHintResolver`, asks the matching `FloePayloads.Descriptor` for the payloadType string (`Descriptor.type()`) and, when replaying overlays, optionally decodes persisted bytes via `Descriptor.decode()`, uses `toEngineSpecific` to build the wrapper, and attaches the `EngineSpecific` blob to the bundle.
* emits one `ColumnResult` per requested column: `COLUMN_STATUS_OK` with `column` when decoration succeeded and required payloads are present, or `COLUMN_STATUS_FAILED` with `failure` (`code`, `message`, optional `details`) when decoration failed.
* if snapshot-aware stats exist for the relation, the bundle includes them in `RelationInfo.stats` (a new `RelationStats` proto carrying best-effort `row_count` and `total_size_bytes`). This field is optional: resolvers may see `RelationInfo.stats` unset for views, system tables, or any unpinned relation, and should treat missing stats as a best-effort decoration rather than required data.

The decorator is a pure sink: it never parses schemas nor recomputes defaults, so the bundle payloads match exactly what `pg_*` scanners would have emitted for the same column in the same engine context.

## 5. Persisting engine hints

Decorators now stage hint payloads during `decorateRelation` / `decorateColumn` and persist in the
`completeRelation(...)` lifecycle callback once column outcomes are known. `FloeEngineSpecificDecorator`
receives the configured `EngineHintPersistence` bean via CDI injection (the service layer wires
`EngineHintPersistenceImpl` as the implementation) and writes committed payloads through
`EngineHintPersistenceImpl`. The implementation consults `EngineHintMetadata` to build the canonical
`engine.hint.<payloadType>` or `engine.hint.column.<payloadType>.<columnId>` key, encodes `(engineKind,
engineVersion, payload)` as a semicolon-delimited string, and updates either the table or view repository only
when the stored value actually changes. Because column hints now carry the stable `columnId`, the loader +
scanner flows can distinguish columns that share payload types even when new columns shift the ordinals.
`UserObjectBundleService` computes the READY column ID set from `ColumnResult` and passes it to
`completeRelation(...)`, so failed columns are reported to callers but their staged hints are discarded
instead of being persisted.
Payload types are normalized (trimmed and lower-cased) and limited to `[a-z0-9._+-]` to keep the key format
stable; semicolons are forbidden because they would break the encoded `(engineKind;engineVersion;payload)` value.

This staged flow adds short-lived memory proportional to staged payload bytes for a single relation
(`O(ready_columns * payload_size)`), then clears the buffers immediately after `completeRelation(...)`.

Those properties are loaded back into the `MetaGraph` because `NodeLoader` eventually calls
`EngineHintMetadata.hintsFromProperties(...)` and `EngineHintMetadata.columnHints(...)` for every catalog, namespace, table, and view, so both relation- and column-level hints reappear even when no decorator runs. Column payloads now key by the stable `columnId` from `SchemaColumn`/`ColumnIdAlgorithm` rather than the ordinal/attnum, so loaders and decorators agree on the identifier. The live request path still benefits from the `EngineHintManager` cache that sits atop the provider pipeline.

Because persisted hints must stay in sync with the schema they describe, any update that touches the logical
definition (currently `schema_json` and `upstream`/`upstream.*` fields) clears the `engine.hint.*` entries before the
service writes the table/view metadata. That forces the next decorator run to treat the object as new, recomputing
fresh OIDs and payloads rather than reusing stale values. These change paths cover the current schema API—the cleaner
runs when one of those canonical fields is present in the request mask, so other schema-affecting updates (e.g., column
lists, format changes, partitioning metadata) need to include `schema_json` or `upstream.*` in their mask (or call the
cleaner directly) for hints to be cleared. Upstream may also carry metadata unrelated to column layout (credentials,
location, notes), so the cleaner can regenerate hints even when only those fields change; refine the mask if you want
to avoid that extra churn.

### Compatibility & normalization guarantees

`engine.hint.*` is persistent metadata, so its format is part of the public contract:

* Relation keys use `engine.hint.<payloadType>` and column keys use `engine.hint.column.<payloadType>.<columnId>`.
  Payload types are trimmed, lower-cased (Locale.ROOT), and restricted to the `[a-z0-9._+-]` character set, which makes
  the stored keys effectively case-insensitive. Any deviation (e.g., a legacy builder that emits uppercase or extra
  whitespace) is normalized when reading via `EngineHintMetadata`.
* Column hints are scoped by the stable `columnId` instead of the ordinal (`attnum`), so adding/removing columns or
  reordering them no longer shifts the stored key. The id comes from the catalog's `ColumnIdAlgorithm`, so it survives
  common column mutations.
* Changing a payload type (for example, introducing `floe.relation+proto.v2`) simply writes to a new key; the old
  hint stays in metadata until the schema cleaner removes it, but the runtime flow couples new payloads to the new key,
  so scanners/decorators never mix versions.
* Schema updates that run through the cleaner (schema_json, upstream definitions) clear every `engine.hint.*` entry
  before other metadata writes, ensuring stale hints don't survive even if the stored payloads drift away from the actual
  schema.

To keep hints alive even if someone else updates the catalog/column metadata concurrently, the persistence helpers
now retry once with a fresh metadata snapshot when an optimistic update fails. That retry behaves exactly like a
single re-read/Rebuild attempt—if the second update still loses the compare-and-set, the helper logs and moves on,
so metadata writes stay best-effort while the decorators remain resilient to racing callers.

## 6. Results for the planner

The planner now receives two consistent views:

* `pg_*` scanners stream `FloeColumnSpecific` fields into rows.
* Catalog bundles attach the same Floe payloads to `RelationInfo` and to READY `ColumnResult.column` entries, while FAILED columns carry structured diagnostics (`ColumnFailure`) instead of silent omission.

Both flows rely on:

* Normalized `EngineContext` (kind+version),
* `FloeHintResolver` for overlay/default fusion,
* A shared `FloePayloads.Descriptor` registry that exposes the payloadType string and decoder for every hint.

Because the resolver caches parsed schema info and the decorator is lightweight, the entire workflow scales to large catalogs without redundant recomputation.

## 7. Optional relation statistics

Catalog bundles now attach optional relation statistics via `RelationInfo.stats`
(`core/proto/src/main/proto/query/catalog_bundle.proto`). Each `RelationStats`
message carries the best-effort `row_count` and `total_size_bytes` values that were recorded for
the pinned snapshot. Because the `RelationStats` wrapper is optional, callers must guard
`relation.hasStats()` before reading the values. When stats are present, both counters are
populated (though `0` may represent “unknown” depending on the upstream source), so downstream
code can safely rely on the reported `row_count`/`total_size_bytes` when non-zero.
Planners should treat missing stats as a decorative hint and never rely on them for correctness.

Scanners get the same best-effort information through `MetadataResolutionContext.statsProvider()`
(`core/catalog/src/main/java/ai/floedb/floecat/systemcatalog/spi/scanner/MetadataResolutionContext.java:27`,
which always returns a non-null provider). The provider itself is defined in
`core/catalog/src/main/java/ai/floedb/floecat/systemcatalog/spi/scanner/StatsProvider.java:24`
and returns `Optional<TableStatsView>`/`Optional<ColumnStatsView>` so that callers can gracefully
handle the absence of data. The service layer wires `StatsProviderFactory`
(`service/src/main/java/ai/floedb/floecat/service/query/catalog/StatsProviderFactory.java:33`)
to cache results per `(tableId, snapshotId)` without mutating `QueryContext` or creating implicit
snapshot pins. These caches only emit values for pinned tables, and the provider always returns
`Optional.empty()` when the pin is missing, so system tables/views/unpinned relations continue to
flow through the bundle/scan paths without failure. Callers that need the data can request it via the
shared provider and add the resulting `RelationStats` to the bundle; everyone else is free to ignore
the field.

Because the stats plumbing is best-effort and stateless, it can be reused by scanners, decorators,
and catalog bundles alike without introducing new failure modes.
