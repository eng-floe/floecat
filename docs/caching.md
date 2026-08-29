# Caching

## Overview
Floecat's read path is built around one observation: everything a query reads below the mutable
pointer reads is an immutable blob. Caching therefore splits into a small number of disciplines,
each with a different correctness contract, rather than one generic cache with invalidation
callbacks.

## Principles
- **Mutable edges resolve to immutable keys.** A read first resolves a mutable pointer (resource ID
  → current blob URI), then follows content-addressed references. Only the pointer resolution can
  be stale; the content it names is immutable.
- **Content-addressed means no invalidation.** The bytes at a CAS blob URI never change, so a
  decoded entry keyed by URI is right forever. Eviction exists only for memory, never correctness.
- **Caches serve content, never existence.** A resident decode may outlive the durable blob (GC can
  sweep a superseded blob while its decode is still cached). Any read whose emptiness is
  load-bearing — a liveness or integrity probe — must hit the live store.

## The four disciplines

| Discipline | Implementation | What it holds | Freshness contract |
|------------|----------------|---------------|--------------------|
| Versioned pointers | `PointerTtlCache` (`service/repo/cache/`) | Root pointer (`TableRootRepository`, `floecat.root.pointer-cache-ttl-seconds`, default 2) and table-definition metas (`GraphCacheManager`, `floecat.metadata.graph.meta-cache-ttl-seconds`, default 2) | Short TTL bounds cross-instance staleness. Version-guarded populate (`putIfFresher`) keeps the highest pointer version; absence is never cached; writes invalidate then repopulate from a live read (read-your-writes on the writing instance). The root commit funnel bypasses the cache entirely via `metaForSafeLive` — CAS needs fresh versions. TTL 0 disables. |
| Immutable decoded content | `ImmutableBlobCache` (`service/repo/cache/`) | Decoded root blobs, snapshot-manifest pages, catalog/namespace/table/view/snapshot/constraints blobs, stats generation manifests, plus derived forms: manifest-entry indexes (`headUri + "#index"`) and graph nodes (`blobUri + "#node"`) | Content-addressed, so no invalidation. Byte-weighted LRU: `serializedSize × 3` retained-heap estimate, 256 MB default, 15-minute access TTL, single-flight loads, absence never cached. Config `floecat.blob.cache.*`; kill switch `floecat.blob.cache.enabled=false`. Only `casBlobs` schemas route through it — blobs overwritten in place must never be cached by URI. |
| Membership lists | `CatalogTopologyCache` (`service/metagraph/cache/`) | Namespace refs per catalog and relation (table/view) refs per namespace, from pointer prefix scans | 15-minute access TTL (`floecat.topology.cache-ttl-minutes`; sizes `floecat.topology.ns-cache-size`=10000, `floecat.topology.rel-cache-size`=5000). DDL services evict on write, so same-process listings are immediately current; cross-instance staleness is bounded by the TTL. |
| Per-query state | `QueryContextStore` and per-query memos | `QueryContext` (pins, snapshot set, expansion map) keyed by query ID | Scoped to one query lease; consistency comes from pinning, not freshness. |

The `ImmutableBlobCache` budget is **shared across all tenants**. The previous design kept a
dedicated node cache per account so eviction pressure was isolated between tenants; folding nodes
into the process-wide cache trades that isolation for byte-accurate weighing, one budget to
operate, and cross-layer sharing (a node and the pages it came from compete honestly for the same
memory). One tenant's wide-schema or long-history scans can therefore evict another tenant's hot
entries — softened by Caffeine's TinyLFU admission policy, which favors frequently reused entries
over one-shot scan traffic. Operators should also note the knob-semantics change:
`floecat.metadata.graph.cache-max-size` used to bound cached nodes per account; it now only gates
node caching (`0` = off) and sizes the pointer-meta cache, while node memory is governed by
`floecat.blob.cache.max-weight-bytes`.

`StatsOrchestrator` additionally keeps its own byte-weighted 256 MB cache of decoded
`TargetStatsRecord`s, keyed by `(accountId, tableId, snapshotId, storageId)`; only positive hits
are cached and mutation paths invalidate explicitly (10-minute write TTL as a backstop).
Target-stats record blobs are deliberately **not** in the `ImmutableBlobCache`: they are written to
deterministic (not content-addressed) URIs and a re-capture may overwrite one in place, so
URI-keyed caching would be unsound for them.

## Deliberately live reads
These reads bypass every cache because their result is a detector, not content:

| Read | Site | Why it must be live |
|------|------|---------------------|
| Resolving-pin root guard, currency and manifest proof | `QueryContextStoreImpl.requirePinnedRootLive` | Asks whether a pinned root is still *present* before it is registered as a GC root — the bytes are immutable, but their presence is exactly what a sweep changes, so a cache hit cannot answer the question. The same read then follows a *mutable* pointer to decide whether the root is current, and proves the manifest head or chain is still live. |
| Frozen stats-manifest read | `StatsRepository.listTargetStatsInGeneration` (per scan page) | This read *is* the scan's retention guard. A cached generation ID would let a scan page "successfully" over a reclaimed generation — empty pages, silently truncated results — exactly when the guard must fire. |
| Published-generation and manifest-page checks | `StatsRepository.requirePublishedGenerationLive`, `TableRootRepository.getManifestPageLive` | Same shape: emptiness is the retention verdict. |
| Dangling-pointer verdict | `NodeLoader.reload` | Emptiness is the verdict itself: a resident decode would report a healthy node over a pointer whose blob is gone. |
| Reusable-candidate load | `SnapshotRepository.loadReusableCandidate` | Emptiness raises a retryable storage abort: the candidate is expected to be there, so a resident decode of a swept blob would let the reuse path proceed on a candidate the store no longer holds. |
| Commit funnel, pointer and blob | `TableRootCommitter` | The CAS needs an expected version no cached pointer can supply, and the base blob's emptiness is the corruption detector. |

Pinned **blob** reads are not among them. The blob a pin names is immutable and content-addressed,
so a resident decode of it *is* the pinned content rather than a stale view — the pinned table,
snapshot, schema, node and constraint loads all read through the cache. For the table, snapshot,
schema and node legs a genuinely missing pinned blob still fails as catalog-integrity corruption
through `requirePinned*`, and still enqueues the table for the resync re-drive. The constraints leg
does neither: it logs a broken-retention warning and degrades that relation to an `ERROR`
resolution, with no repair report — and on a cache hit over a swept blob it does not fire at all.

Nor is a pinned read preceded by a probe of its root. A pin whose blobs still read is coherent
whatever has happened to the live pointer meanwhile, and a probe could only report what the read
that needs the blob reports anyway.

The repository API encodes the split: `getByBlobUri` serves cached content — a present result does
**not** prove the blob still exists — while `getByBlobUriLive` bypasses the cache for reads whose
emptiness is load-bearing.

## Staleness bounds

| Observation | Bound | Governed by |
|-------------|-------|-------------|
| Cross-instance DDL visibility (which blob a definition pointer names) | ≤ 2 s | Graph meta cache TTL (`floecat.metadata.graph.meta-cache-ttl-seconds`) |
| Table currency (which root is current) | ≤ 2 s cross-instance; read-your-writes on the writing instance for update/create | Root pointer cache TTL (`floecat.root.pointer-cache-ttl-seconds`) plus invalidate-then-live-refresh on writes |
| Catalog/namespace listings | ≤ 15 min cross-instance; immediate same-process | Topology cache TTL plus evict-on-write from DDL services |
| Pinned data read within a query | None by construction | Immutable blobs plus live integrity reads |

Defaults for `floecat.root.pointer-cache-ttl-seconds` and `floecat.blob.cache.*` are set in
`service/src/main/resources/application.properties`; the graph meta TTL and `floecat.topology.*`
defaults come from their `@ConfigProperty` declarations (`UserGraph`, `CatalogTopologyCache`).
