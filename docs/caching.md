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
- **Decoded content never proves existence.** A resident decode may outlive the durable blob (GC can
  sweep a superseded blob while its decode is still cached). Any read whose emptiness is
  load-bearing — a liveness or integrity probe — must hit the live store. The complete pointer
  index is deliberately different: after its account load completes, a missing addressing key is
  authoritative absence.

## The cache disciplines

| Discipline | Implementation | What it holds | Freshness contract |
|------------|----------------|---------------|--------------------|
| Pointers | `PointerCache` behind `CachingPointerStore` | Eleven complete SQL-addressing families plus an admission-controlled remainder for every other pointer family | No expiry. The first addressing read loads the account's four durable subtrees consistently; once complete, point misses, prefix listings and counts are authoritative and cost no store reads. Complete entries never evict. Writes **publish** rather than invalidate and are version-ordered; deletes remove. A failed load or exhausted budget marks the account degraded and falls back to the store. The unqualified `PointerStore` remains authoritative by default; query-serving repositories opt into `@CachedPointerStore` once at wiring. |
| Immutable decoded content | `ImmutableBlobCache` (`service/repo/cache/`) | Decoded root blobs, snapshot-manifest pages, catalog/namespace/table/view/snapshot/constraints blobs, stats generation manifests, plus derived forms: manifest-entry indexes (`headUri + "#index"`) and graph nodes (`blobUri + "#node"`) | Content-addressed, so no invalidation. Byte-weighted, with Caffeine's TinyLFU admission rather than plain LRU: `serializedSize × 3` retained-heap estimate, 256 MB default, 15-minute access TTL, single-flight loads, absence never cached. Config `floecat.blob.cache.*`; kill switch `floecat.blob.cache.enabled=false`. Only `casBlobs` schemas route through it — blobs overwritten in place must never be cached by URI. |
| Per-query state | `QueryContextStore` and per-query memos | `QueryContext` (pins, snapshot set, expansion map) keyed by query ID | Scoped to one query lease; consistency comes from pinning, not freshness. |

The `ImmutableBlobCache` budget is **shared across all tenants**, which buys byte-accurate
weighing, one budget to operate, and cross-layer sharing — a node and the pages it came from
compete honestly for the same memory. The cost is that one tenant's wide-schema or long-history
scans can evict another tenant's hot entries, softened by Caffeine's TinyLFU admission policy,
which favours frequently reused entries over one-shot scan traffic.

`floecat.metadata.graph.cache-max-size` gates node caching (`0` = off); node memory is governed by
`floecat.blob.cache.max-weight-bytes`.

`StatsOrchestrator` additionally keeps its own byte-weighted 256 MB cache of decoded
`TargetStatsRecord`s, keyed by `(accountId, tableId, snapshotId, storageId)`; only positive hits
are cached and mutation paths invalidate explicitly (10-minute write TTL as a backstop).
Target-stats record blobs are deliberately **not** in the `ImmutableBlobCache`: they are written to
deterministic (not content-addressed) URIs and a re-capture may overwrite one in place, so
URI-keyed caching would be unsound for them.

## The shared cache contract (`core/cache`)

The future cache layers share this module's operational and telemetry vocabulary, but not one
storage-shaped interface or one resource pool. `MemoryCache<K, V>` is the in-memory primitive used
by object and hint caches and as storage inside the pointer-cache layer. The pointer layer belongs
behind `PointerStore` and owns strict version publication, race fencing, complete name indexes and
account readiness. Blob content gets a separate disk-oriented interface and volume budget, so
callers never emulate scoped disk reads, mappings or sweeping through memory-cache operations.

The module is container-free — Caffeine and protobuf, no CDI, no Quarkus. A cache is built with
`new` and wired by whoever owns the container, which is what lets the service, its tests and any
sizing harness use the same arithmetic.

| Piece | What it is |
|-------|------------|
| `MemoryCache<K, V>` | Read-through `get`; batch `getAll`, which owns miss detection, loading and safe publication; uncounted `peek`; unconditional `put`; `evict` by key and `evictPartition` by caller-supplied membership; `bytes()`/`entryCount()` for the budget. A load racing a mutation cannot restore its stale value. Partition eviction is an infrequent O(n) scan of resident keys. No expiry: staleness is bounded by publication, not by a clock. Pointer version ordering deliberately is not part of this generic contract. |
| `CaffeineMemoryCache` | The one implementation. W-TinyLFU admission, so a wide listing or a statistics sweep does not flush the hot set. Refuses a non-positive budget at construction. |
| `CacheWeights` | Retained-heap estimate: entry machinery plus the key's bytes plus a walk of the value (`WeightedValue` first, then protobuf, text, `byte[]`, maps and collections). A shape it cannot walk throws rather than taking a flat default, so a value retaining megabytes cannot be charged a kilobyte. |
| `CacheFamily` | The independently budgeted in-memory families that actually use this module — `POINTER` today. Each is its own cache, never a tag inside a shared one, so a burst in the fastest-moving family cannot evict the slowest. Add `OBJECT` and `HINT` when those implementations land; do not add disk blob caching to this enum. The tag is both the metric dimension and the config segment. |
| `CacheBudget` / `CacheBudgetResolver` | One total split across the families. Pure arithmetic in `CacheBudget.split`; `CacheBudgetResolver` (`service/cache/`) reads the configuration and runs it at startup. |
| `CacheEvents` | The common event baseline: `hit` (with how long it took to serve, so a caller that waited on someone else's load is not an instant hit), `miss`, `loadTime`, `loadFailed`, `loadDiscarded`, `admissionRejected` and `evicted`. Bulk reads report hits and misses per distinct key and one duration per loader invocation. A disk cache can reuse these metrics and add mapping/sweep signals without implementing `MemoryCache`. The module reports events; the container names the metrics. |

Budgets resolve from the container rather than from a compiled-in figure. The JVM already sizes its
heap from the container memory limit, so `floecat.cache.heap-share` (0.5) of the maximum heap
follows the container without reading cgroups; `floecat.cache.total-bytes` pins the total instead
and skips the derivation. Each family then claims `floecat.cache.<tag>.max-bytes` if set, otherwise
`floecat.cache.<tag>.share`, resolved by tag over `CacheFamily.values()` — so a new cache is
configured by adding its two properties and nothing else. A claim that resolves to zero bytes —
including a share small enough to round away against a small total — fails at startup, as do
claims that together exceed the total. A family whose configuration is *absent* is the allowed
case and takes nothing; what refuses that is the cache built for it, which will not accept a
budget of zero.

Only implemented in-memory families appear in `CacheFamily`; this avoids publishing configuration
and metric dimensions for caches that do not exist yet. The existing `ImmutableBlobCache` remains
under `floecat.blob.cache.max-weight-bytes`. A future disk blob cache can reuse the cache module's
telemetry vocabulary while exposing its own lifecycle-shaped interface and volume budget.

`floecat.cache.pointer.share` is 0.096, from the reference sizing scenario: a 100,000-table account
at 100 columns needs 0.32 GB of addressing out of the 3.34 GB the memory caches hold between them
(Pointers, Objects and Hints). The share resolves against that total, not against the heap — the
heap is one `heap-share` step above it. That is a starting point, not a law.
Addressing is width-independent — it costs the same whatever the columns look like — so a
proportional share over-allocates it on a wide catalog and starves it on a narrow one, where the
same fixed need is a much larger fraction of a much smaller total. `max-bytes` is what pins it
against that, and exceeding the budget costs store reads rather than wrong answers.

The pointer cache is the first specialized layer built on the shared in-memory contract; the other
layers above are unchanged. Its independent durable subtrees load through a bounded metadata
fan-out; `floecat.cache.pointer.load-parallelism=0` derives the bound from the processors available
to the JVM, while a positive value pins it. Complete-index events, including eager-load duration,
carry the logical account tag through the same `CacheEvents` contract.

Cache selection stops at the repository boundary. Service and graph callers ask repositories for
objects or lightweight refs; they never inject a cached, authoritative, or raw pointer-store view.
Repositories use the cached view for query reads, while mutation and GC repositories retain the
authoritative default. `ConsistentReadRulesTest` enforces both that boundary and the absence of
per-call consistency selection.

## What a cache reports

A cache built on the `core/cache` contract publishes the same series, tagged by cache name, so
those are comparable and a new one brings its telemetry with it. `ImmutableBlobCache` predates the
contract and publishes its own subset; `graph-cache` is node-load timing, not a cache.

| question | series |
|---|---|
| Is it on? | `floecat_core_cache_enabled` |
| Is it being used, and is it working? | `floecat_core_cache_hits` / `..._misses` |
| What does a miss cost? | `floecat_core_cache_latency`, recorded per load |
| Are loads failing? | `floecat_core_cache_errors` |
| How full is it? | `floecat_core_cache_weighted_size_bytes` against `..._max_weight_bytes` |
| How many entries? | `floecat_core_cache_entries` |
| Is the budget too small? | `floecat_core_cache_evictions` and `..._evicted_weight_bytes` |
| Has it stopped warming? | `floecat_core_cache_loads_discarded` — a load whose value was not retained because a write may have raced it |
| Is the budget rejecting valid entries? | `floecat_core_cache_admission_rejected` |
| Are pointer indexes ready? | `floecat_core_cache_accounts`, tagged `result=loading|complete|degraded` |

Hits and misses are counted as they happen rather than derived from a running total, because a rate
computed from a cumulative gauge cannot tell an idle cache from one that is missing everything.

Nothing expires in the pointer cache. The eviction series count only capacity-driven removals;
explicit deletes, prefix sweeps, and authoritative-read repairs are not included. A non-zero
eviction rate therefore directly signals size pressure. The weight alongside the count
distinguishes many small evictions from a few large ones.

## Turning the pointer cache off

`floecat.cache.pointer.enabled=false` installs the raw pointer store instead of the caching
decorator, so the read path becomes the pre-cache one. Off means the decorator is not there, not
that a cache is there holding nothing.

A budget of zero is *not* the switch — it is refused at startup, because a cache sized zero reports
a 0% hit rate that reads as a cache which is not helping rather than one that was turned off.

There is also no flag that stops publishing while leaving the cache populated, and there should not
be: publishing is what keeps a pointer current — nothing expires — so a populated cache that is not
published to serves whatever it last loaded, indefinitely.

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
| Every pointer read in the GC | `PointerGc`, `CasBlobGc`, `ReconcileJobGc`, `TransactionGc` | The GC deletes based on what it reads. A stale canonical pointer makes a live name pointer look orphaned and CAS-deletes it; a stale root pointer puts a superseded blob in the mark set and omits the current one, which is then swept. `ConsistentReadRulesTest` holds the line. |

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
| Cross-instance DDL visibility (which blob a definition pointer names) | **unbounded by time** | Nothing expires. A publish refreshes the replica that made the write; another replica holding the same key keeps its value until it writes that key or reads it authoritatively. Reads that cannot tolerate this do not take it: the commit funnel, the pin guards and the whole GC receive the unqualified authoritative store view, which repairs the cached entry on the way back. |
| Table currency (which root is current) | none within the replica that committed; cross-instance as above | `CachingPointerStore` publishing under the store, version-guarded |
| Catalog/namespace listings | none within the owning replica; degraded accounts read through | Complete pointer indexes maintained by `CachingPointerStore` |
| Pinned data read within a query | None by construction | Immutable blobs plus live integrity reads |

Cache budgets derive from the container: `floecat.cache.total-bytes` defaults to a share of the
maximum heap, which the JVM already sizes from the container memory limit, and each cache takes a
share of that. The knobs the pointer cache reads are `floecat.cache.total-bytes`,
`floecat.cache.heap-share`, `floecat.cache.pointer.share` and `floecat.cache.pointer.max-bytes`,
the last pinning an absolute size instead of a share, plus
`floecat.cache.pointer.load-parallelism`; a share outside `(0, 1]` fails at startup.
`heap-share` and `pointer.share` carry defaults in
`service/src/main/resources/application.properties`, alongside `floecat.blob.cache.*`;
`total-bytes` and `pointer.max-bytes` are unset, and each is a share until it is given a value.
