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

## Warm and cold reads

A warm read reuses the process-local value. A cold read, a disabled cache, or a cache miss follows
the same repository path and returns the same answer; only latency and store cost change. Caffeine
coordinates concurrent requests for the same immutable key. Durable mutations create a new
identity; they do not replace a value in an existing key.

## The cache disciplines

| Discipline | Implementation | What it holds | Freshness contract |
|------------|----------------|---------------|--------------------|
| Pointers | `PlanningPointerIndex` behind `IndexedPointerStore` | All planner pointer records for an account, including current roots, names, snapshots, constraints, stats and hint-resource pointers | Not a cache. A partition is either `LOADING` or `COMPLETE`. While loading, reads use durable KV; after completion, point reads, listings and counts are served from the sorted in-memory index and absence is authoritative. A point mutation commits to durable KV and publishes the result while holding the account read lock and that key's lock; prefix and account-wide mutations use the account write lock. Operational pointers remain on the durable adapter. |
| Objects | `ObjectCache` | Decoded relation metadata, mapped schemas, constraints, immutable generation-scoped snapshot facts and target-stat records | Entries are keyed by immutable content or generation identity. A live/newest stats read is read-through and is never retained. Account eviction removes every object entry for that account. |
| Blobs | `DiskBlobCache` behind `BlobCacheAccess` | Immutable serialized CAS bodies, manifest pages, generation manifests and reusable-artifact bundles/indexes on local NVMe | Files are addressed by immutable URI or pointer/version identity, written through a staging file and atomic rename. A miss can fill the disk cache or bypass filling for wide scans. Corrupt entries are discarded and reloaded; mmap content stays pinned until its scoped read closes. The disk budget and kill switch are `floecat.cache.blob.disk.*`; it is independent of the heap budget. |
| Per-query state | `QueryContextStore` and per-query memos | `QueryContext` (pins, snapshot set, expansion map) keyed by query ID | Scoped to one query lease; consistency comes from pinning, not freshness. |

An owned pointer partition can be warmed in the background when ownership is granted. The first
read also schedules the warm if no ownership notification was received. Reads never wait for this
work: while the partition is `LOADING`, they use durable KV. Mutations take the partition write
lock for a prefix or account-wide operation, so they wait for a load already in progress; point
mutations use the read side plus a per-key lock, so unrelated tables in the same account can mutate
in parallel. Every mutation commits to KV before publishing the new pointer. A failed warm leaves
the partition `LOADING` and the next read continues using KV.

The pointer index has one account gate and one lock per planner key. Point reads use the gate's
read side and do not wait for another table's mutation. A point mutation takes its key locks in
sorted order, performs the durable operation, then publishes or removes the entry before releasing
them. Sorting is only needed for multi-key mutations; it prevents two batches from acquiring the
same key set in opposite orders. Listing, counting, batch reads, warming, reload, and account
deletion take the account write side because they need one stable ordered view. There is no
application-level in-flight request map or version fence in this path: the partition gate and
key-lock ownership define the ordering.

Standalone Floecat uses `ALWAYS_OWNED`, because there is no competing owner. A managed deployment
provides the `PlanningPointerIndex.Ownership` implementation and connects ownership handoff to the
index: revoke ownership before routing the account away, then drop the old partition; after the
new owner is granted, start warming it. The index never assumes ownership from a cache hit.

`ObjectCache` stores immutable-generation target statistics by `(accountId, tableId, snapshotId,
generation, target identity)`. A live/newest result has no stable identity and is therefore read
through without retention. Exact-body target-stat URIs are immutable once published and may be
served by the disk blob cache; legacy logical-only URIs use pointer/version identity instead, so a
re-capture that rewrites one cannot return stale bytes.

`DiskBlobCache` has no request-level load map. Each miss reads the durable source directly; `FILL`
publishes the returned bytes through a staged file and atomic rename, while `BYPASS_FILL` returns
the bytes without admission. The key is immutable, so a late fill can only recreate an old,
unreachable identity; it cannot overwrite the bytes named by a newer pointer. Account deletion
retirements use the disk partition lock to prevent a deleted account from being admitted again in
that process. That lock protects file lifecycle and mappings; it is not a generic cache fence.

## The shared cache contract (`core/cache`)

The cache module provides one small read-through primitive. Callers use repositories and do not
choose cache tiers or coordinate in-flight requests. Caffeine owns same-key load coordination; the
application adds no second load map or version fence for immutable entries.

The future cache layers share this module's operational and telemetry vocabulary, but not one
storage-shaped interface or one resource pool. `MemoryCache<K, V>` is the in-memory primitive used
by object and hint caches. The pointer layer belongs behind `PointerStore` and owns ordered
publication under its partition lock, complete name indexes and account readiness. Blob content gets a separate disk-oriented interface and volume budget, so
callers never emulate scoped disk reads, mappings or sweeping through memory-cache operations.

The module is container-free — Caffeine and protobuf, no CDI, no Quarkus. A cache is built with
`new` and wired by whoever owns the container, which is what lets the service, its tests and any
sizing harness use the same arithmetic.

| Piece | What it is |
|-------|------------|
| `MemoryCache<K, V>` | Read-through `get`; batch `getAll`; uncounted `peek`; `evict` by key and `evictPartition` by caller-supplied membership; `bytes()`/`entryCount()` for the budget. Values are immutable and keyed by durable identity, so there is no generic replacement, publication fence, or in-flight map. Eviction is an infrequent O(n) memory-hygiene scan. Pointer version ordering deliberately is not part of this generic contract. |
| `CaffeineMemoryCache` | The one implementation. W-TinyLFU admission, so a wide listing or a statistics sweep does not flush the hot set. Refuses a non-positive budget at construction. |
| `CacheWeights` | Retained-heap estimate: entry machinery plus the key's bytes plus a walk of the value (`WeightedValue` first, then protobuf, text, `byte[]`, maps and collections). A shape it cannot walk throws rather than taking a flat default, so a value retaining megabytes cannot be charged a kilobyte. |
| `CacheFamily` | The independently budgeted in-memory families that use this module. Pointer planning state is not a `MemoryCache` family: it is a complete index with no eviction budget. `OBJECT` is decoded metadata and `HINT` is decoded engine-specific metadata. Disk blob caching has its own volume budget. |
| `CacheBudget` / `CacheBudgetResolver` | One total split across the families. Pure arithmetic in `CacheBudget.split`; `CacheBudgetResolver` (`service/cache/`) reads the configuration and runs it at startup. |
| `CacheEvents` | The common event baseline: `hit`, `miss`, `loadTime`, `loadFailed` and `evicted`. Bulk reads report misses for the keys passed to their source loader and one duration per loader invocation. A disk cache can reuse the telemetry vocabulary while exposing its own lifecycle-shaped interface. The module reports events; the container names the metrics. |

A memory-cache loader may read durable storage and assemble its value, but it must not call
`get` or `getAll` recursively on the same cache. Caffeine coordinates one mapping function per key
and its underlying map rejects recursive updates. Resolve another cached dependency first, then
enter the loader for the value that depends on it. This is a loader rule, not an application-level
in-flight mechanism.

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

Only implemented in-memory families appear in `CacheFamily`; the disk blob cache has its own
physical-volume budget and lifecycle-shaped interface. It reuses the cache telemetry vocabulary
where useful but does not pretend that files and heap entries share one budget.

Pointer planning state is not budgeted by the generic memory-cache contract. The index is the
account's current in-memory representation and must stay complete; if it cannot be loaded, the
durable adapter remains the answer until the next load attempt. Callers do not select a cached or
authoritative view: every service path receives `IndexedPointerStore`, which chooses the in-memory
index for planner keys and durable KV for operational keys.

## What a cache reports

A cache built on the `core/cache` contract publishes the same series, tagged by cache name, so
those are comparable and a new one brings its telemetry with it. The disk blob cache adds mapping,
corruption and sweep signals because those are file-lifecycle events rather than heap-cache events.

| question | series |
|---|---|
| Is it on? | `floecat_core_cache_enabled` |
| Is it being used, and is it working? | `floecat_core_cache_hits` / `..._misses` |
| What does a miss cost? | `floecat_core_cache_latency`, recorded per load |
| Are loads failing? | `floecat_core_cache_errors` |
| How full is it? | `floecat_core_cache_weighted_size_bytes` against `..._max_weight_bytes` |
| How many entries? | `floecat_core_cache_entries` |
| Is the budget too small? | `floecat_core_cache_evictions` and `..._evicted_weight_bytes` |
| Are pointer indexes ready? | `floecat.service.planning.pointer.partitions`, tagged `result=loading|complete` |
| How is pointer warming behaving? | `floecat.service.planning.pointer.warm.*`; failures are also logged with `account_id` |
| Which pointer entries are resident? | `floecat.service.planning.pointer.entries` |

Hits and misses are counted as they happen rather than derived from a running total, because a rate
computed from a cumulative gauge cannot tell an idle cache from one that is missing everything.
An account moves from `loading` to `complete` after its durable planner rows have been loaded. A
load failure leaves it on the durable path; the next read can retry the load.

Warm-up metrics are intentionally aggregate: account IDs are not metric labels. A failed warm-up
also emits a structured log containing the account ID, elapsed time, and exception, so an operator
can identify the affected account without creating one time series per account.

Nothing expires in the pointer index. The eviction series count only capacity-driven removals in
the memory-cache families; explicit deletes and prefix sweeps are not included. A non-zero
eviction rate therefore directly signals size pressure. The weight alongside the count
distinguishes many small evictions from a few large ones.

There is no independent pointer-cache switch. Pointer reads always go through `IndexedPointerStore`;
the durable adapter is used automatically while an account index is loading and for operational
keys. This keeps the read and mutation path identical in tests and production.

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
| Cross-instance DDL visibility (which blob a definition pointer names) | bounded by ownership handoff | One Floecat owner accepts writes for an account. A new owner loads the durable partition before serving it; the old owner must stop accepting writes before handoff. |
| Table currency (which root is current) | none within the owning replica; cross-instance changes require the owner contract | `IndexedPointerStore` publishes after the durable CAS while holding the account lock |
| Catalog/namespace listings | none after the account index is complete; loading accounts use durable KV | `PlanningPointerIndex` sorted account partitions |
| Pinned data read within a query | None by construction | Immutable blobs plus live integrity reads |

Cache budgets derive from the container: `floecat.cache.total-bytes` defaults to a share of the
maximum heap, which the JVM already sizes from the container memory limit, and each memory cache takes a
share of that. `heap-share` carries its default in
`service/src/main/resources/application.properties`, alongside the disk blob settings;
`total-bytes` is unset and is derived when the object and hint memory caches are wired.
