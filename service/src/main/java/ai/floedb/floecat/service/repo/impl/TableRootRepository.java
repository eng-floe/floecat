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

import ai.floedb.floecat.cache.BlobCache;
import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestPage;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.CachedPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

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
  // Serialized-content cache for immutable root blobs and manifest pages.
  private final BlobCacheAccess blobCache;

  public TableRootRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(pointerStore, pointerStore, blobStore, BlobCacheAccess.disabled());
  }

  public TableRootRepository(
      PointerStore pointerStore, BlobStore blobStore, BlobCacheAccess blobCache) {
    this(pointerStore, pointerStore, blobStore, blobCache);
  }

  @Inject
  public TableRootRepository(
      PointerStore pointerStore,
      @CachedPointerStore PointerStore pointerReads,
      BlobStore blobStore,
      BlobCacheAccess blobCache) {
    super(
        pointerStore,
        pointerReads,
        blobStore,
        Schemas.TABLE_ROOT,
        TableRoot::parseFrom,
        TableRoot::toByteArray,
        blobCache);
    this.pointerStore = pointerStore;
    this.blobStore = blobStore;
    this.blobCache = java.util.Objects.requireNonNull(blobCache, "blobCache");
  }

  /**
   * Unconditional root-pointer removal for DROP / account-cascade purges. Root blobs are
   * deliberately left behind for CasBlobGc, since a pinned query may still read them.
   */
  public void purgeRoot(ResourceId tableId) {
    pointerStore.delete(Keys.tableRootByTable(tableId.getAccountId(), tableId.getId()));
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
    if (blobCache.enabled()) {
      blobCache.putImmutable(uri, bytes);
    }
    return BlobRef.newBuilder().setUri(uri).setVersion(sha).build();
  }

  /** Loads a manifest page by ref. Empty when the blob is gone (a swept superseded page). */
  public Optional<SnapshotManifestPage> getManifestPage(BlobRef ref) {
    if (ref == null || ref.getUri().isEmpty()) {
      return Optional.empty();
    }
    if (blobCache.enabled()) {
      Optional<BlobCache.Content> content =
          blobCache.immutable(
              ref.getUri(), BlobCache.Fill.FILL, () -> loadManifestPageBytes(ref.getUri()));
      if (content.isEmpty()) return Optional.empty();
      try (BlobCache.Content body = content.orElseThrow()) {
        return Optional.of(SnapshotManifestPage.parseFrom(body.buffer()));
      } catch (InvalidProtocolBufferException e) {
        throw new BaseResourceRepository.CorruptionException(
            "manifest page parse failed: " + ref.getUri(), e);
      }
    }
    return loadManifestPage(ref.getUri());
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

  /**
   * Verifies that every page a root is about to publish exists in live storage. The commit and CAS
   * GC coordinate through {@code TableBlobReachabilityGuard}, so a successful validation cannot be
   * invalidated by an ownerless-blob delete before the root pointer becomes visible.
   */
  public void requireManifestChainLive(ResourceId tableId, BlobRef head) {
    String requiredPrefix =
        Keys.snapshotManifestBlobPrefix(tableId.getAccountId(), tableId.getId());
    BlobRef cursor = head;
    Set<String> visited = new HashSet<>();
    while (cursor != null && !cursor.getUri().isBlank()) {
      String uri = cursor.getUri();
      if (!uri.startsWith(requiredPrefix)) {
        throw new BaseResourceRepository.CorruptionException(
            "manifest page is outside table scope: " + uri);
      }
      if (!visited.add(uri)) {
        throw new BaseResourceRepository.CorruptionException("manifest page cycle at " + uri);
      }
      SnapshotManifestPage page =
          getManifestPageLive(cursor)
              .orElseThrow(
                  () ->
                      new BaseResourceRepository.CorruptionException(
                          "manifest page missing: " + uri));
      cursor = page.hasPrevPageRef() ? page.getPrevPageRef() : null;
    }
  }

  /**
   * Verifies only a chain head. Production root mutations create any new prefix pages while the
   * table publication guard is held and link them to the still-current chain, so walking the entire
   * immutable history again would add O(snapshot history) remote reads to every commit.
   */
  public void requireManifestHeadLive(ResourceId tableId, BlobRef head) {
    if (head == null || head.getUri().isBlank()) {
      return;
    }
    String uri = head.getUri();
    String requiredPrefix =
        Keys.snapshotManifestBlobPrefix(tableId.getAccountId(), tableId.getId());
    if (!uri.startsWith(requiredPrefix)) {
      throw new BaseResourceRepository.CorruptionException(
          "manifest page is outside table scope: " + uri);
    }
    if (getManifestPageLive(head).isEmpty()) {
      throw new BaseResourceRepository.CorruptionException("manifest page missing: " + uri);
    }
  }

  private Optional<SnapshotManifestPage> loadManifestPage(String uri) {
    byte[] bytes = loadManifestPageBytes(uri);
    if (bytes == null) return Optional.empty();
    try {
      return Optional.of(SnapshotManifestPage.parseFrom(bytes));
    } catch (InvalidProtocolBufferException e) {
      throw new BaseResourceRepository.CorruptionException("manifest page parse failed: " + uri, e);
    }
  }

  private byte[] loadManifestPageBytes(String uri) {
    try {
      return blobStore.get(uri);
    } catch (StorageNotFoundException e) {
      return null;
    } catch (StorageAbortRetryableException e) {
      throw new BaseResourceRepository.AbortRetryableException(
          "manifest page read retryable: " + uri);
    }
  }
}
