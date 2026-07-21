/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestPage;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.cache.PointerTtlCache;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The per-table immutable {@link TableRoot} and its snapshot-manifest pages.
 *
 * <p>The root itself is a CAS'd, content-addressed record (one pointer per table, see {@link
 * TableScopedPointerRepository}); every commit writes a new root blob and CASes the pointer.
 * Manifest pages are pointer-less immutable blobs referenced only from roots: content-addressed, so
 * a rewrite of identical content is an idempotent overwrite, and validation is existence at the
 * ref's version (the content hash).
 */
@ApplicationScoped
public class TableRootRepository extends TableScopedPointerRepository<TableRoot> {

  private static final String CONTENT_TYPE = "application/x-protobuf";

  private final PointerStore pointerStore;
  private final BlobStore blobStore;
  // Nullable (tests): decoded-content cache for the immutable root blobs and manifest pages.
  private final ImmutableBlobCache blobCache;

  // Root-POINTER cache (the one deliberately-stale read, product-approved): tableId key ->
  // MutationMeta (pointer version + blob uri + etag), expireAfterWrite(ttl). Same-process writes
  // invalidate (read-your-writes on the writing instance); cross-instance a CURRENT resolution may
  // be <=TTL behind — snapshot-consistent, never torn (the root blob it names is immutable).
  //
  // The cached meta serves CURRENCY (which root is current) and pin-identity capture only. It is
  // deliberately NOT a liveness witness: the CAS GC min-age fence (floecat.gc.cas.min-age-ms,
  // default 30s) measures age since the blob was WRITTEN, not since it was last referenced, so an
  // old root passes the fence the instant it is superseded — a recent observation proves nothing
  // about the blob still existing. Liveness checks (PinValidator, requirePinned*) therefore always
  // HEAD the live store. READ path only: the commit funnel uses metaForSafeLive (a stale CAS
  // expected-version would only lose and burn retries). TTL 0 disables; tests use the ttl-0
  // constructors.
  private final PointerTtlCache<String> pointerCache;

  public TableRootRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(pointerStore, blobStore, null, 0L);
  }

  public TableRootRepository(
      PointerStore pointerStore, BlobStore blobStore, ImmutableBlobCache blobCache) {
    this(pointerStore, blobStore, blobCache, 0L);
  }

  @Inject
  public TableRootRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      ImmutableBlobCache blobCache,
      @ConfigProperty(name = "floecat.root.pointer-cache-ttl-seconds", defaultValue = "2")
          long pointerCacheTtlSeconds) {
    super(
        pointerStore,
        blobStore,
        Schemas.TABLE_ROOT,
        TableRoot::parseFrom,
        TableRoot::toByteArray,
        blobCache);
    this.pointerStore = pointerStore;
    this.blobStore = blobStore;
    this.blobCache = blobCache;
    this.pointerCache = new PointerTtlCache<>(pointerCacheTtlSeconds);
  }

  private static String pointerCacheKey(ResourceId tableId) {
    return tableId.getAccountId() + "\0" + tableId.getId();
  }

  @Override
  public MutationMeta metaForSafe(ResourceId tableId) {
    if (!pointerCache.enabled()) {
      return super.metaForSafe(tableId);
    }
    String key = pointerCacheKey(tableId);
    MutationMeta hit = pointerCache.getIfPresent(key);
    if (hit != null) {
      return hit;
    }
    MutationMeta live = super.metaForSafe(tableId);
    // Never cache absence (this family's absent form is a blank blob uri): a table about to get
    // its FIRST root must see it on the next read. The version guard lives in PointerTtlCache.
    if (!live.getBlobUri().isBlank()) {
      pointerCache.putIfFresher(key, live);
    }
    return live;
  }

  /** Live pointer meta for the COMMIT funnel: bypasses the TTL cache (CAS needs fresh versions). */
  public MutationMeta metaForSafeLive(ResourceId tableId) {
    pointerCache.invalidate(pointerCacheKey(tableId));
    return super.metaForSafe(tableId);
  }

  @Override
  public Optional<TableRoot> get(ResourceId tableId) {
    if (!pointerCache.enabled()) {
      return super.get(tableId);
    }
    // Warm path: cached pointer names the immutable root blob; the blob comes from the decoded
    // cache — zero remote reads. Falls back to the live pointer read when the meta is absent.
    MutationMeta meta = metaForSafe(tableId);
    if (meta.getBlobUri().isBlank()) {
      // metaForSafe just read the live pointer (absence is never cached): no pointer, no root.
      return Optional.empty();
    }
    Optional<TableRoot> root = getByBlobUri(meta.getBlobUri());
    if (root.isPresent()) {
      return root;
    }
    // A vanished blob under a cached pointer: evict the dead meta (or every read for the rest of
    // the TTL pays cache probe + 404 + live read — and other metaForSafe consumers inherit the
    // poisoned entry), then re-read live rather than reporting a torn state.
    invalidatePointer(tableId);
    return super.get(tableId);
  }

  @Override
  public boolean createIfAbsent(TableRoot value) {
    boolean created = super.createIfAbsent(value);
    refreshPointerAfterWrite(value.getTableId(), created);
    return created;
  }

  @Override
  public boolean update(TableRoot value, long expectedPointerVersion) {
    boolean updated = super.update(value, expectedPointerVersion);
    refreshPointerAfterWrite(value.getTableId(), updated);
    return updated;
  }

  @Override
  public boolean deleteWithPrecondition(ResourceId tableId, long expectedPointerVersion) {
    // Delete has no fresh meta to out-version a straggling reader's merge, so a pre-delete meta
    // can be reinserted for <=TTL. Acceptable: root deletion happens only on DROP/account
    // cascades, where readers already race the cascade's other teardown within the same bound.
    boolean deleted = super.deleteWithPrecondition(tableId, expectedPointerVersion);
    invalidatePointer(tableId);
    return deleted;
  }

  /**
   * Invalidate, then (on an applied write) repopulate from a LIVE read. The version-guarded merge
   * in {@link #metaForSafe} makes this the read-your-writes guarantee: the fresh, higher-version
   * meta wins over any straggling reader's pre-commit meta regardless of arrival order — a bare
   * invalidate alone could be overwritten by a stale put that lands after it.
   */
  private void refreshPointerAfterWrite(ResourceId tableId, boolean applied) {
    if (!pointerCache.enabled()) {
      return;
    }
    invalidatePointer(tableId);
    if (applied) {
      // UNCONDITIONALLY live, never through metaForSafe: a straggling reader can insert its
      // pre-commit meta in the invalidate→read window, and metaForSafe would trust that hit and
      // skip the live read — leaving the stale entry to serve for the full TTL. Reading live and
      // merging lets the fresh, higher-version meta out-win the straggler in either order.
      // Residual (documented, TTL-bounded): if THIS fresh entry expires/evicts before an
      // extremely late straggler merges into the then-empty map, the stale meta can serve for at
      // most one further TTL — the same bound the cross-instance contract already accepts.
      MutationMeta live = super.metaForSafe(tableId);
      if (!live.getBlobUri().isBlank()) {
        pointerCache.putIfFresher(pointerCacheKey(tableId), live);
      }
    }
  }

  private void invalidatePointer(ResourceId tableId) {
    pointerCache.invalidate(pointerCacheKey(tableId));
  }

  /**
   * Unconditional root-pointer removal for DROP / account-cascade purges. Purges MUST route here
   * rather than deleting the key straight off the pointer store: the repository owns the pointer
   * cache, and a bypassed delete leaves the dropped table's root serving from cache on the deleting
   * instance for a further TTL — breaking the same-process read-your-writes contract. Root blobs
   * are deliberately left behind for CasBlobGc (a pinned query may still read them).
   */
  public void purgeRoot(ResourceId tableId) {
    pointerStore.delete(Keys.tableRootByTable(tableId.getAccountId(), tableId.getId()));
    invalidatePointer(tableId);
  }

  /** Loads a root directly from its immutable blob URI (a pinned root, not the live pointer). */
  public Optional<TableRoot> getByBlobUri(String blobUri) {
    return repo.getByBlobUri(blobUri);
  }

  /**
   * Cache-bypassing root read for the COMMIT funnel: its emptiness is the dangling-pointer
   * corruption detector, which must fire deterministically — a warm decoded root must not mask a
   * swept blob (and a CAS retry must not flip behavior as the entry evicts).
   */
  public Optional<TableRoot> getByBlobUriLive(String blobUri) {
    return repo.getByBlobUriLive(blobUri);
  }

  /** The etag of the root blob at {@code blobUri}, or {@code null} when absent (pin validation). */
  public String blobEtag(String blobUri) {
    return repo.blobEtag(blobUri);
  }

  /**
   * Writes one immutable manifest page and returns its content-addressed ref. Identical content
   * maps to an identical URI, so concurrent writers of the same page converge instead of
   * conflicting.
   */
  public BlobRef putManifestPage(String accountId, String tableId, SnapshotManifestPage page) {
    byte[] bytes = page.toByteArray();
    String sha = Hashing.sha256Hex(bytes);
    String uri = Keys.snapshotManifestBlobUri(accountId, tableId, sha);
    blobStore.put(uri, bytes, CONTENT_TYPE);
    if (blobCache != null) {
      // Write-through: the writer holds the decoded page; the next reader (often the same commit's
      // read-back, or the first query after it) should not pay a cold fetch for content we have.
      blobCache.put(uri, page);
    }
    return BlobRef.newBuilder().setUri(uri).setVersion(sha).build();
  }

  /** Loads a manifest page by ref. Empty when the blob is gone (a swept superseded page). */
  public Optional<SnapshotManifestPage> getManifestPage(BlobRef ref) {
    if (ref == null || ref.getUri().isEmpty()) {
      return Optional.empty();
    }
    if (blobCache != null && blobCache.enabled()) {
      return blobCache.get(ref.getUri(), this::loadManifestPage);
    }
    return loadManifestPage(ref.getUri());
  }

  /**
   * Read-path index of a manifest chain — {@code snapshotId → entry} — keyed by the chain's HEAD
   * URI. The head is content-addressed and every next-page ref inside it is too, so the head URI
   * pins the entire chain's content: the index is immutable and needs no invalidation. Built once
   * per head by one page walk (through the decoded page cache); pin creation's per-query entry
   * lookup becomes a map probe. Returns {@code null} when caching is off — callers fall back to the
   * page walk, which also keeps the fail-closed missing-page behavior (the walk throws).
   */
  public Map<Long, SnapshotManifestEntry> manifestEntryIndex(BlobRef head) {
    if (blobCache == null || !blobCache.enabled() || head == null || head.getUri().isEmpty()) {
      return null;
    }
    // Probe-then-build-then-put, deliberately NOT a loading get: the build walks pages through
    // this SAME cache (getManifestPage), and a nested compute inside a Caffeine compute is
    // prohibited — a same-bin hash collision between the "#index" key and a page key would throw
    // "Recursive update" or livelock, nondeterministically. A duplicate concurrent build is
    // harmless (the index is deterministic and immutable); the pages themselves stay single-flight.
    String indexKey = head.getUri() + "#index";
    Map<Long, SnapshotManifestEntry> hit = blobCache.probe(indexKey);
    if (hit != null) {
      return hit;
    }
    Map<Long, SnapshotManifestEntry> index = new java.util.HashMap<>();
    // forEachEntry walks through getManifestPage (decoded-cache-backed) and THROWS on a missing
    // page — fail-closed manifest reads are preserved, and the failure is never cached.
    SnapshotManifests.forEachEntry(this, head, e -> index.put(e.getSnapshotId(), e));
    // The local map never leaks mutable; an unmodifiable VIEW avoids copying a wide index.
    Map<Long, SnapshotManifestEntry> built = Collections.unmodifiableMap(index);
    blobCache.put(indexKey, built);
    return built;
  }

  /**
   * Cache-bypassing page read for MUTATION chain walks (they run inside the commit funnel, which
   * reads live — a resident decode must not mask a swept page from the fail-closed manifest-read
   * contract). Read-path walks keep the cached {@link #getManifestPage}.
   */
  public Optional<SnapshotManifestPage> getManifestPageLive(BlobRef ref) {
    if (ref == null || ref.getUri().isEmpty()) {
      return Optional.empty();
    }
    return loadManifestPage(ref.getUri());
  }

  private Optional<SnapshotManifestPage> loadManifestPage(String uri) {
    try {
      byte[] bytes = blobStore.get(uri);
      if (bytes == null) {
        return Optional.empty();
      }
      return Optional.of(SnapshotManifestPage.parseFrom(bytes));
    } catch (StorageNotFoundException e) {
      return Optional.empty();
    } catch (StorageAbortRetryableException e) {
      throw new BaseResourceRepository.AbortRetryableException(
          "manifest page read retryable: " + uri);
    } catch (InvalidProtocolBufferException e) {
      throw new BaseResourceRepository.CorruptionException("manifest page parse failed: " + uri, e);
    }
  }
}
