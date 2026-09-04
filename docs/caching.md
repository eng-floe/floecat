# Caching

## Overview
Floecat's read path separates mutable addressing, current decoded metadata, and serialized blob
residency. Each has a different correctness contract, so they share metrics and budgeting
vocabulary without being forced through one storage-shaped interface.

## Principles
- **Cache identity carries freshness.** Immutable bodies use their content URI. A mutable body uses
  process incarnation, pointer key, pointer version, and URI, so rewriting a deterministic URI
  cannot make old bytes reachable from the new pointer. Mutable entries deliberately start cold
  after restart because delete/recreate may reuse pointer version one; immutable entries remain
  reusable across restarts.
- **Serialized residency never proves existence.** A resident body may outlive the durable blob (GC
  can sweep a superseded blob while its cached copy remains). Any read whose emptiness is
  load-bearing — a liveness or integrity probe — must hit the live store. The complete pointer
  index is deliberately different: after its account load completes, a missing addressing key is
  authoritative absence.

## Warm and cold reads

A warm read reuses the process-local value. A cold read, a disabled cache, a cache miss, or a
temporarily degraded cache follows the same repository path and returns the same answer; only
latency and store cost change. Concurrent requests for the same value share one source load.
Durable mutations become visible to the cache only after they commit, and an older in-flight read
cannot replace a newer value after eviction.

## The cache disciplines

| Discipline | Implementation | What it holds | Freshness contract |
|------------|----------------|---------------|--------------------|
| Pointers | `PointerCache` behind `CachingPointerStore` | Twelve complete query families, including per-engine hint pointers, plus an admission-controlled remainder for every other pointer family | No expiry. The first query-pointer read loads the account's five durable subtrees consistently; once complete, point misses, prefix listings and counts are authoritative and cost no store reads. Complete entries never evict. Writes **publish** rather than invalidate and are version-ordered; deletes remove. A failed load or exhausted budget marks the account degraded and falls back to the store. The unqualified `PointerStore` remains authoritative by default; query-serving repositories opt into `@CachedPointerStore` once at wiring. |
| Current SQL objects | `ObjectCache` | Engine-neutral assembled relations and mapped schemas, current constraint bundles, and small snapshot facts | Immutable identities need no invalidation. Stats writers publish facts under the generation identity committed to the table root; pinned generations remain isolated. Entries may evict because a miss is a safe reload. Account deletion evicts the account partition. |
| Engine hints | `HintCache` | Opaque relation and column payloads for one user relation and exact `(engine kind, engine version)` | One content-addressed resource per engine version. The relation blob URI in the resource rejects hints for an older relation shape; the hint blob URI is the decoded-cache identity. Runtime writes merge through an authoritative CAS read and are fenced by the live relation pointer. Older property-backed hints remain a read fallback. System hints are already materialized in immutable `SystemNodeRegistry` snapshots and do not use this cache. |
| Serialized blobs | `DiskBlobCache` behind `BlobCacheAccess` | CAS metadata, snapshot-manifest pages, stats generation manifests, target-stats records and reusable artifact bundles/index objects | Local NVMe, no resident entry index and no TTL. URI-keyed for immutable bodies, including newly written target-stat records; target records with logical-only URIs remain process-and-pointer-version-keyed while they are readable. Checksummed atomic files, single-flight fills, scoped mmap reads, fenced account-partition eviction, and background budget sweeping. Immutable range reads use a resident whole body when available or admit only the requested range. Bulk listings consume hits but do not fill misses. |
| Per-query state | `QueryContextStore` and per-query memos | `QueryContext` (pins, snapshot set, expansion map) keyed by query ID | Scoped to one query lease; consistency comes from pinning, not freshness. |

The blob-cache budget is **shared across tenants** on the local volume. Entries are physically
partitioned by account so account deletion can remove the whole partition while its durable
deletion fence is held. A deleted account-id partition remains non-admitting for the rest of the
process, so a writer that loses the deletion race cannot recreate local state afterward; account
ids are immutable and are not reused. One-shot bulk reads do not admit misses, preventing an
unfiltered stats scan from replacing the hot disk set. Derived graph nodes and decoded planner
stats are no longer cached separately: assembled query metadata belongs to `ObjectCache`, and
serialized stats belong to `DiskBlobCache`.

Mapped reads avoid an intermediate full-body `byte[]` while hashing and parsing large bundles.
The returned protobuf still owns decoded fields, including heap-backed `ByteString` sketch data,
because the repository API outlives the scoped mapping. True zero-copy sketch forwarding requires
a response-scoped ownership API that keeps `BlobCache.Content` open through gRPC serialization; it
is not safe to alias mapped memory and close the arena at repository return. Compression likewise
belongs to the `SketchPayload.data` format contract, not this cache envelope; this layer preserves
those bytes unchanged.

## The shared cache contract (`core/cache`)

The cache module coordinates overlapping reads and mutations internally. Callers use repositories
and do not choose locks, load slots, or cache tiers.

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
| `CaffeineMemoryCache` | The one implementation. W-TinyLFU admission, so a wide listing or a statistics sweep does not flush the hot set. Cold loads are single-flight and may compose other keys from the same cache. Refuses a non-positive budget at construction. |
| `CacheWeights` | Retained-heap estimate: entry machinery plus the key's bytes plus a walk of the value (`WeightedValue` first, then protobuf, text, `byte[]`, maps and collections). A shape it cannot walk throws rather than taking a flat default, so a value retaining megabytes cannot be charged a kilobyte. |
| `BlobCache` / `DiskBlobCache` | Scoped serialized whole-body and range reads with fill or bypass-fill intent, unconditional publication, key/account eviction, mmap lifetime tracking, and sweeping. A range hit can slice a resident whole body; a miss admits only that exact range, avoiding a full-object fetch for a small block. Files are addressed directly from hashed identities, so restart does not require rebuilding a heap index. Disk failures fail open to the source store; corrupt entries are discarded and refilled. |
| `CacheFamily` | Stable telemetry identities: `POINTER`, `OBJECT`, `HINT`, and `BLOB`. Pointer, Object and Hint use the shared heap budget; Blob has an independent physical-volume budget. |
| `CacheBudget` / `CacheBudgetResolver` | One total split across the families. Pure arithmetic in `CacheBudget.split`; `CacheBudgetResolver` (`service/cache/`) reads the configuration and runs it at startup. |
| `CacheEvents` | The common event baseline: `hit` (with how long it took to serve, so a caller that waited on someone else's load is not an instant hit), `miss`, `loadTime`, `loadFailed`, `loadDiscarded`, `admissionRejected`, `writeThrough` and `evicted`. Write-through reports whether the cache applied the publication or skipped it through a safety guard. Bulk reads report hits and misses per distinct key and one duration per loader invocation. A disk cache can reuse these metrics and add mapping/sweep signals without implementing `MemoryCache`. The module reports events; the container names the metrics. |

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

Only implemented families appear in `CacheFamily`. `CacheBudgetResolver` assigns heap only to
families with heap share/max-byte configuration; Blob is constructed from
`floecat.cache.blob.disk.*` and therefore cannot accidentally consume the in-memory budget.

`floecat.cache.pointer.share` is 0.123, from the reference sizing scenario: a 100,000-table account
at 100 columns and two engine versions needs about 0.42 GB of query pointers out of the 3.44 GB the
memory caches hold between them (Pointers, Objects and Hints). This includes the 0.10 GB of hint
pointers required to make an absent hint authoritative without a KV read. The share resolves
against that total, not against the heap — the
heap is one `heap-share` step above it. That is a starting point, not a law.
Addressing is width-independent — it costs the same whatever the columns look like — so a
proportional share over-allocates it on a wide catalog and starves it on a narrow one, where the
same fixed need is a much larger fraction of a much smaller total. `max-bytes` is what pins it
against that, and exceeding the budget costs store reads rather than wrong answers.

`floecat.cache.object.share` is 0.752. Objects holds one engine-neutral assembled `RelationInfo`
and its mapped `SchemaDescriptor` per immutable relation identity, mapped schemas by their real
mapping inputs, decoded constraint bundles by immutable content URI, and the two small snapshot
facts used by relation assembly. A table relation key hashes the definition and schema identities
before any ingredient is loaded, so a warm relation skips pinned-schema resolution and
mapping entirely. Pinned schema reads use those same immutable pin identities, so planner schema
RPCs also avoid resolving their table and snapshot ingredients on a hit. Names, projection, stats
attachment, pin identity, and engine decoration are
applied after lookup, so a single cached relation survives renames, data-only ingests, and requests
from different engines. Constraints use their own content-keyed entry because they are served by a
separate RPC and are not part of `RelationInfo`; changing them does not invalidate the relation.
Mapped schema and assembled relation entries use the measured mapped-proto
retained-heap multiplier; relation entries conservatively charge their schema reference again so
eviction order cannot make retained memory invisible to the budget.

Absent constraints and stats are not cached. Current snapshot facts distinguish the mutable live
view from immutable pinned stats generations, preserving stable-plan semantics when generations
overlap for the same snapshot. Historical and time-travel facts read through without admission, so
they cannot displace current SQL metadata. Writers publish facts only under the exact generation
identity successfully committed to the table root and evict the mutable live entry when that
identity or the table record is unavailable. That avoids guessing which generation won a concurrent
publication race. Immutable relation, schema, constraint, and current-generation fact entries need
no mutation invalidation. Account deletion drops the account partition while the deletion fence is
held.

`floecat.cache.hint.share` is 0.125. Hints uses the same `MemoryCache` implementation and metric
contract as Objects, but a separate budget because its entries change with engine versions rather
than relation or ingest identities. `MetaGraph` attaches only the requested engine's hints to user
relations; callers never select storage or cache behavior. A warm lookup still resolves the hint
pointer through Pointers, then finds the decoded body by immutable URI, so it performs no KV or S3
read. The runtime persistence adapter compares submitted payloads with that decoded entry before
entering the authoritative mutation path, so reusing a complete warm hint does not re-read or
rewrite it. Table/view deletion removes every engine-version pointer, and account deletion fences
and evicts the full partition.

The pointer, object, and hint caches are the specialized layers built on the shared in-memory
contract. The pointer cache's independent durable subtrees load through a bounded metadata
fan-out; `floecat.cache.pointer.load-parallelism=0` derives the bound from the processors available
to the JVM, while a positive value pins it. A failed or capacity-rejected index remains store-backed
until the first read after `floecat.cache.pointer.degraded-retry-seconds`, when one caller retries
the complete load and concurrent callers continue through the store. Complete-index events,
including eager-load duration, carry the logical account tag through the same `CacheEvents`
contract.

Cache selection stops at the repository boundary. Service and graph callers ask repositories for
objects or lightweight refs; they never inject a cached, authoritative, or raw pointer-store view.
Repositories use the cached view for query reads, while mutation and GC repositories retain the
authoritative default. `ConsistentReadRulesTest` enforces both that boundary and the absence of
per-call consistency selection.

## What a cache reports

A cache built on the `core/cache` contract publishes the same baseline series, tagged by family,
so the layers are comparable. Disk adds only lifecycle signals that do not apply to heap caches.

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
| Are write-through publications being applied or safety-guarded? | `floecat_core_cache_write_through{result="applied\|skipped"}` |
| Are pointer indexes ready? | `floecat_core_cache_accounts`, tagged `result=loading|complete|degraded` |
| Is local disk content corrupt? | `floecat_core_cache_corruptions` |
| Is the sweeper reclaiming space? | `floecat_core_cache_sweep_reclaimed_bytes` |
| Are mapped files preventing reclamation? | `floecat_core_cache_live_mappings` |

Hits and misses are counted as they happen rather than derived from a running total, because a rate
computed from a cumulative gauge cannot tell an idle cache from one that is missing everything.
An account retrying a degraded index moves through `degraded` to `loading` and then `complete` in
the readiness counts; another failure returns it to `degraded`.

Nothing expires in the pointer cache. The eviction series count only capacity-driven removals;
explicit deletes, prefix sweeps, and authoritative-read repairs are not included. A non-zero
eviction rate therefore directly signals size pressure. The weight alongside the count
distinguishes many small evictions from a few large ones.

## Turning memory caches off

`floecat.cache.pointer.enabled=false` installs the raw pointer store instead of the caching
decorator, so the read path becomes the pre-cache one. Off means the decorator is not there, not
that a cache is there holding nothing.

`floecat.cache.object.enabled=false` keeps the same object-facing APIs but loads every relation,
schema, constraint bundle, and snapshot-facts value directly. Callers do not select cached versus
uncached reads themselves.

`floecat.cache.hint.enabled=false` keeps the same graph-facing API and loads the current hint body
directly. It does not revert storage to relation properties; that map is compatibility input for
data written by older releases only.

`floecat.cache.blob.disk.enabled=false` preserves the same repository API and reads serialized
bodies directly from object storage. The disk path is not opened when disabled.

A query-serving deployment should enable the disk tier after mounting capacity-managed local
storage at `floecat.cache.blob.disk.path`. It remains opt-in at the application level because this
repository does not own the production volume mount, and creating `/mnt/nvme/floecat` alone cannot
prove that it is backed by NVMe rather than the container root filesystem. The local Compose
service enables a bounded 1 GiB ephemeral cache under `/tmp`; that exercises the production path
but is not a performance substitute for NVMe. Reconciler executors remain source-backed.

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
share of that. The shared knobs are `floecat.cache.total-bytes` and
`floecat.cache.heap-share`. Each family accepts `floecat.cache.<tag>.share`,
`floecat.cache.<tag>.max-bytes`, and `floecat.cache.<tag>.enabled`; the max pins an absolute size
instead of a share. Pointer-specific knobs are
`floecat.cache.pointer.load-parallelism` and
`floecat.cache.pointer.degraded-retry-seconds`; a share outside `(0, 1]` fails at startup.
`heap-share`, `pointer.share`, and `object.share` carry defaults in
`service/src/main/resources/application.properties`. The independent disk knobs are
`floecat.cache.blob.disk.enabled`, `.path`, `.max-bytes`, `.mmap-threshold-bytes`,
`.access-touch-interval-seconds`, and `.sweep-interval`; `total-bytes` and the per-heap-family
`max-bytes` properties are unset.
