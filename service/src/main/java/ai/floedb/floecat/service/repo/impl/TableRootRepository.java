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
import ai.floedb.floecat.catalog.rpc.SnapshotManifestPage;
import ai.floedb.floecat.catalog.rpc.TableRoot;
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
import java.util.Optional;

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

  private final BlobStore blobStore;

  @Inject
  public TableRootRepository(PointerStore pointerStore, BlobStore blobStore) {
    super(
        pointerStore, blobStore, Schemas.TABLE_ROOT, TableRoot::parseFrom, TableRoot::toByteArray);
    this.blobStore = blobStore;
  }

  /** Loads a root directly from its immutable blob URI (a pinned root, not the live pointer). */
  public Optional<TableRoot> getByBlobUri(String blobUri) {
    return repo.getByBlobUri(blobUri);
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
    return BlobRef.newBuilder().setUri(uri).setVersion(sha).build();
  }

  /** Loads a manifest page by ref. Empty when the blob is gone (a swept superseded page). */
  public Optional<SnapshotManifestPage> getManifestPage(BlobRef ref) {
    if (ref == null || ref.getUri().isEmpty()) {
      return Optional.empty();
    }
    try {
      byte[] bytes = blobStore.get(ref.getUri());
      if (bytes == null) {
        return Optional.empty();
      }
      return Optional.of(SnapshotManifestPage.parseFrom(bytes));
    } catch (StorageNotFoundException e) {
      return Optional.empty();
    } catch (StorageAbortRetryableException e) {
      throw new BaseResourceRepository.AbortRetryableException(
          "manifest page read retryable: " + ref.getUri());
    } catch (InvalidProtocolBufferException e) {
      throw new BaseResourceRepository.CorruptionException(
          "manifest page parse failed: " + ref.getUri(), e);
    }
  }
}
