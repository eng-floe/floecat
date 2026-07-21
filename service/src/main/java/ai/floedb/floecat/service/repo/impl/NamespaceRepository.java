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

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph.NamespaceRef;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.NamespaceKey;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@ApplicationScoped
public class NamespaceRepository {

  private final GenericResourceRepository<Namespace, NamespaceKey> repo;
  private final PointerStore pointerStore;

  public NamespaceRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(pointerStore, blobStore, null);
  }

  @Inject
  public NamespaceRepository(
      PointerStore pointerStore, BlobStore blobStore, ImmutableBlobCache blobCache) {
    this.pointerStore = pointerStore;
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.NAMESPACE,
            Namespace::parseFrom,
            Namespace::toByteArray,
            "application/x-protobuf",
            blobCache);
  }

  public void create(Namespace namespace) {
    repo.create(namespace);
  }

  public boolean update(Namespace namespace, long expectedPointerVersion) {
    return repo.update(namespace, expectedPointerVersion);
  }

  public boolean delete(ResourceId namespaceResourceId) {
    return repo.delete(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  public boolean deleteWithPrecondition(
      ResourceId namespaceResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()),
        expectedPointerVersion);
  }

  public Optional<Namespace> getById(ResourceId namespaceResourceId) {
    return repo.getByKey(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  public Optional<Namespace> getByPath(
      String accountId, String catalogId, List<String> pathSegments) {
    return repo.get(Keys.namespacePointerByPath(accountId, catalogId, pathSegments));
  }

  public List<Namespace> list(
      String accountId,
      String catalogId,
      List<String> parentSegmentsOrEmpty,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, parentSegmentsOrEmpty);
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(String accountId, String catalogId, List<String> parentSegmentsOrEmpty) {
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, parentSegmentsOrEmpty);
    return repo.countByPrefix(prefix);
  }

  /**
   * Page token resuming a {@link #list} scan immediately after the namespace at {@code fullPath}.
   * Lets callers that post-filter scanned rows continue exactly after the last row they emitted
   * instead of after the whole over-fetched batch.
   */
  public String listTokenAfter(String accountId, String catalogId, List<String> fullPath) {
    return pointerStore.pageTokenAfterKey(
        Keys.namespacePointerByPath(accountId, catalogId, fullPath));
  }

  public List<ResourceId> listIds(String accountId, String catalogId) {
    // empty parent path -> all namespaces in catalog
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, List.of());
    List<Namespace> namespaces =
        repo.listByPrefix(prefix, Integer.MAX_VALUE, "", new StringBuilder());
    List<ResourceId> ids = new java.util.ArrayList<>(namespaces.size());
    for (Namespace ns : namespaces) {
      ids.add(ns.getResourceId());
    }
    return ids;
  }

  /**
   * Scans the by-path pointer prefix for a catalog and returns lightweight refs without loading
   * blobs from S3. Falls back to key/blobUri parsing for legacy pointers.
   */
  public List<NamespaceRef> listRefs(String accountId, String catalogId) {
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, List.of());
    var pointers = repo.listRefsByPrefix(prefix);
    var refs = new ArrayList<NamespaceRef>(pointers.size());
    ResourceId catalogResourceId = catalogResourceId(accountId, catalogId);
    for (var p : pointers) {
      toNamespaceRef(accountId, catalogId, catalogResourceId, p).ifPresent(refs::add);
    }
    return refs;
  }

  /** Reads exact by-path namespace pointers and returns refs without fetching blobs from S3. */
  public List<NamespaceRef> listRefsByName(String accountId, String catalogId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    ResourceId catalogResourceId = catalogResourceId(accountId, catalogId);
    List<NamespaceRef> refs = new ArrayList<>(names.size());
    for (String name : names) {
      if (name == null || name.isBlank()) {
        continue;
      }
      repo.refByPointer(
              Keys.namespacePointerByPath(accountId, catalogId, List.of(name.split("\\.", -1))))
          .flatMap(p -> toNamespaceRef(accountId, catalogId, catalogResourceId, p))
          .ifPresent(refs::add);
    }
    return refs;
  }

  private static ResourceId catalogResourceId(String accountId, String catalogId) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setId(catalogId)
        .setKind(ResourceKind.RK_CATALOG)
        .build();
  }

  private static Optional<NamespaceRef> toNamespaceRef(
      String accountId,
      String catalogId,
      ResourceId catalogResourceId,
      ai.floedb.floecat.common.rpc.Pointer p) {
    List<String> pathSegments = Keys.extractNamespacePathSegments(accountId, catalogId, p.getKey());
    String name =
        !p.getDisplayName().isEmpty()
            ? p.getDisplayName()
            : pathSegments.isEmpty()
                ? Keys.extractLastSegment(p.getKey())
                : pathSegments.get(pathSegments.size() - 1);
    ResourceId rid = p.getResourceId();
    if (rid.getId().isEmpty()) {
      String rawId = Keys.extractResourceIdFromBlobUri(p.getBlobUri());
      if (rawId.isEmpty()) {
        return Optional.empty();
      }
      rid =
          ResourceId.newBuilder()
              .setAccountId(accountId)
              .setId(rawId)
              .setKind(ResourceKind.RK_NAMESPACE)
              .build();
    }
    return Optional.of(new NamespaceRef(rid, name, catalogResourceId, pathSegments));
  }

  public MutationMeta metaFor(ResourceId namespaceResourceId) {
    return repo.metaFor(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId namespaceResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId namespaceResourceId) {
    return repo.metaForSafe(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  /** Pointer-only meta (no blob HEAD, blank etag) for metadata-graph consumers. */
  public MutationMeta pointerMetaForSafe(ResourceId namespaceResourceId) {
    return repo.pointerMetaForSafe(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  /** Blob-direct read for graph hydration from resolved metadata; empty if the blob moved. */
  public Optional<Namespace> getByBlobUri(String blobUri) {
    return repo.getByBlobUri(blobUri);
  }

  /** Cache-bypassing read for liveness-bearing callers (see GenericResourceRepository). */
  public Optional<Namespace> getByBlobUriLive(String blobUri) {
    return repo.getByBlobUriLive(blobUri);
  }
}
