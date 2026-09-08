# Cache concurrency contract

This is the concurrency contract shared by the cache layers. It is deliberately expressed as a
small state machine so a new cache implementation does not need a new race argument.

## One protocol

Each cache key has one of these states:

```text
ABSENT ── acquire ──> LOADING(epoch, owner)
  ▲                         │
  │                         ├─ source result + current epoch
  │                         │       └─ publish RESIDENT
  │                         │
  │                         └─ retired epoch
  │                                 └─ return to joined callers, do not publish
  │
RESIDENT ── evict ──> ABSENT
RESIDENT ── put ────> RESIDENT(new value)
```

Every read-through implementation follows the same sequence:

1. Sample the key fence before probing the resident tier.
2. Probe the resident tier.
3. On a miss, atomically acquire the key's load slot. The first caller is the owner; later callers
   join its future.
4. The owner reads the source without holding a cache lock.
5. The owner publishes only while its load slot and fence are still current.
6. The owner completes the slot and removes ownership. A failed or absent load never leaves a
   resident entry.

The source result is allowed to return to a caller that started before an eviction. It is not
allowed to repopulate the cache after the eviction. This is the distinction between request
completion and cache publication.

## Mutations and partitions

`put` and `evict` retire the current key slot before changing the resident value. A partition
eviction closes load registration, retires all matching slots, advances every matching fence, and
then removes resident entries. A caller that arrives after the operation therefore cannot join a
load that started before it.

The coordinator owns this ordering. Implementations provide only the resident operation: Caffeine
entry publication, disk-file publication, or authoritative-index mutation.

## Scalar and batch reads

`get` and `getAll` use the same per-key slots. A batch owns only the keys for which it won the slot;
its source loader is called once for that owned subset. Keys owned by scalar or other batch callers
join those futures. This gives one rule for scalar/batch and batch/batch races, while retaining the
source efficiency of one bulk read.

`peek` is intentionally outside this protocol: it never loads, never joins, and never reports a
hit or miss. Request paths use `get` or `getAll`, not a loop of peeks.

## Migration boundary

The concurrency foundation is implemented in `core/cache` by `LoadCoordinator` and
`CaffeineMemoryCache` in this change. The Object and Hint adapters consume it through
`MemoryCache`; the disk Blob adapter and the authoritative Pointer index migrate their
owner/publication paths in their stacked changes. Until then, their mmap, generation, readiness,
and index locks remain adapter-specific, but must preserve the same owner/epoch/publication rules.

## Layer-specific adaptations

The protocol is shared; the resident medium is not:

- **Objects and Hints** use `MemoryCache` and the shared load coordinator. Their keys are immutable
  identities, so a miss is a safe reload.
- **Blobs** use the disk-oriented `BlobCache` interface and the same owner/epoch/publication rule.
  Partition generations, fill/bypass-fill, mapped-file references, and sweeping remain disk
  concerns. A blob key must identify immutable content.
- **Pointers** use the protocol for the evictable remainder. Complete addressing indexes are
  authoritative state: account readiness and the partition read/write lock decide whether a miss
  means absence. Durable mutations commit first, then publish the committed value or removal.

No caller needs to know which lock, future, map, file, or readiness state implements the tier.

## Loader rule

A loader may compose different cache keys. It must not synchronously load the same key it currently
owns; that dependency fails fast rather than deadlocking. The metadata dependency graph is acyclic:
relation objects may load schemas and constraints, but those loaders do not load the relation again.

## Observable guarantees

The shared contract tests cover the same cases for every implementation that can be adapted to the
contract:

- one owner and many followers;
- scalar versus batch and batch versus batch;
- exact eviction during a load;
- partition eviction during a load;
- publication racing a load;
- absence and loader failure cleanup;
- recursive same-key loading;
- nested loads on different keys.

Metrics follow the same ownership model: hits and misses are per requested key, source duration is
per owner invocation, and `loadDiscarded` records a result that could not be retained because its
epoch was retired.
