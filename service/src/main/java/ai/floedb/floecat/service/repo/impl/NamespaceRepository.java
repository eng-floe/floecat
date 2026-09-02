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
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph.NamespaceRef;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.NamespaceKey;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.repo.util.MetadataRepositoryFactory;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@ApplicationScoped
public class NamespaceRepository {

  private final GenericResourceRepository<Namespace, NamespaceKey> repo;
  private final PointerStore pointerStore;

  public NamespaceRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(
        pointerStore,
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.NAMESPACE,
            Namespace::parseFrom,
            Namespace::toByteArray,
            "application/x-protobuf"));
  }

  @Inject
  public NamespaceRepository(PointerStore pointerStore, MetadataRepositoryFactory repositories) {
    this(
        pointerStore,
        repositories.create(
            Schemas.NAMESPACE,
            Namespace::parseFrom,
            Namespace::toByteArray,
            "application/x-protobuf"));
  }

  private NamespaceRepository(
      PointerStore pointerStore, GenericResourceRepository<Namespace, NamespaceKey> repo) {
    this.pointerStore = pointerStore;
    this.repo = repo;
  }

  public void create(Namespace namespace) {
    repo.create(namespace);
  }

  public GenericResourceRepository.ResourceWithMeta<Namespace> createWithCompletion(
      Namespace namespace,
      Function<GenericResourceRepository.ResourceWithMeta<Namespace>, List<PointerStore.CasOp>>
          completionFactory) {
    return repo.createWithMeta(namespace, completionFactory);
  }

  /** Creates under the supplied fence and publishes companion operations in the same batch. */
  public Optional<GenericResourceRepository.ResourceWithMeta<Namespace>>
      createWithCompletionWhilePointersMatch(
          Namespace namespace,
          PointerConditions conditions,
          Function<GenericResourceRepository.ResourceWithMeta<Namespace>, List<PointerStore.CasOp>>
              completionFactory) {
    return repo.createWithMeta(namespace, conditions, completionFactory);
  }

  public Optional<MutationMeta> completeWithMetaIfUnchanged(
      Namespace namespace,
      long expectedPointerVersion,
      Function<GenericResourceRepository.ResourceWithMeta<Namespace>, List<PointerStore.CasOp>>
          completionFactory) {
    return repo.completeWithMetaIfUnchanged(namespace, expectedPointerVersion, completionFactory);
  }

  public boolean createWhilePointersMatch(Namespace namespace, PointerConditions conditions) {
    return repo.createWithMeta(namespace, conditions, null).isPresent();
  }

  /**
   * Deletes the namespace under shape markers the caller sampled before its emptiness checks.
   *
   * <p>Removes the markers with the row rather than advancing them, matching how a catalog delete
   * treats its own -- an advanced marker for a deleted namespace is a row counting nothing, and
   * namespace ids never recur so nothing would ever read it again.
   */
  public boolean deleteWhileShapeUnchanged(
      ResourceId namespaceResourceId,
      long expectedPointerVersion,
      MarkerStore.MarkerRemoval markers) {
    return repo.deleteWithPreconditionWhilePointersMatchAndDeletePointers(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()),
        expectedPointerVersion,
        markers.conditions(),
        markers.toDelete());
  }

  public Optional<MutationMeta> updateWhilePointersMatch(
      Namespace namespace, long expectedPointerVersion, PointerConditions conditions) {
    return repo.updateWithMetaWhilePointersMatchAndBumpMarkers(
        namespace, expectedPointerVersion, conditions);
  }

  public boolean delete(ResourceId namespaceResourceId) {
    return repo.delete(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  public boolean deleteWhilePointersMatch(
      ResourceId namespaceResourceId, long expectedPointerVersion, PointerConditions conditions) {
    return repo.deleteWithPreconditionWhilePointersMatchAndDeletePointers(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()),
        expectedPointerVersion,
        conditions,
        Map.of());
  }

  public Optional<Namespace> getById(ResourceId namespaceResourceId) {
    return repo.getByKey(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  public Optional<Namespace> getByPath(
      String accountId, String catalogId, List<String> pathSegments) {
    return repo.get(Keys.namespacePointerByPath(accountId, catalogId, pathSegments));
  }

  /**
   * Everything a namespace create has to assert: its catalog's child set, and its parent's when it
   * has one.
   *
   * <p>The catalog half is not optional and is the only half a TOP-LEVEL namespace has -- without
   * it a namespace created at the root of a catalog is unfenced against that catalog's delete,
   * which is the case a parent fence structurally cannot cover.
   *
   * <p>Composed here so the writers cannot drift apart. Each of them differs only in how it
   * classifies an absent catalog or parent, which is genuinely per-caller; the set of conditions is
   * not.
   */
  public PointerConditions createFence(
      MarkerStore markers, ResourceId catalogId, List<String> parentPath) {
    return markers
        .catalogChildNamespacesFence(catalogId)
        .and(
            childSetFenceForParent(
                markers, catalogId.getAccountId(), catalogId.getId(), parentPath));
  }

  /**
   * The fence for joining {@code parentPath}'s child set, or none when the path is top-level.
   *
   * <p>The parent's identity and its by-path version come from ONE read of that pointer. Resolving
   * the identity and then reading the version separately lets the path be vacated and reused in
   * between, and the conditions would then check the new occupant's path while advancing the old
   * occupant's child marker -- fencing one namespace and writing under another. A child could
   * commit under the reused path without touching the current parent's marker, and a rename or
   * delete of that parent, having already sampled an unchanged marker and an empty child scan,
   * would strand it.
   */
  public PointerConditions childSetFenceForParent(
      MarkerStore markers, String accountId, String catalogId, List<String> parentPath) {
    if (parentPath == null || parentPath.isEmpty()) {
      return PointerConditions.none();
    }
    String byPath = Keys.namespacePointerByPath(accountId, catalogId, parentPath);
    Pointer pointer =
        pointerStore
            .get(byPath)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.NotFoundException(
                        ResourceKind.RK_NAMESPACE,
                        "namespace parent path does not resolve: " + parentPath));
    return markers.childSetFenceForParent(byPath, withIdentity(pointer, parentPath));
  }

  /**
   * The namespace a by-path pointer names, from that pointer alone.
   *
   * <p>Namespace pointers carry their resource id, so no second read is needed. A pointer written
   * before that field existed is resolved through its own blob uri, which is immutable -- so the
   * identity still comes from the same observation as the version, and is folded back into the
   * pointer so the fence receives one object carrying both.
   */
  private Pointer withIdentity(Pointer pointer, List<String> parentPath) {
    if (!pointer.getResourceId().getId().isBlank()) {
      return pointer;
    }
    ResourceId resolved =
        repo.getByBlobUri(pointer.getBlobUri())
            .map(Namespace::getResourceId)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.NotFoundException(
                        ResourceKind.RK_NAMESPACE,
                        "namespace parent path does not resolve: " + parentPath));
    return pointer.toBuilder().setResourceId(resolved).build();
  }

  /**
   * How many relations a namespace holds, read strongly.
   *
   * <p>Both halves matter. Tables and views carry separate by-name prefixes, so counting one
   * reports a namespace holding the other as empty. And the count has to be consistent: callers
   * fence this answer with a marker sampled by a consistent point read, so a relation committed
   * just before that sample is already in the marker version -- an eventually-consistent count
   * would read zero, the CAS would match, and a delete would commit over a live relation.
   *
   * <p>A prefix count rather than a listing: the decision it gates is a boolean, and listing loads
   * every row's blob to answer it.
   */
  public static int relationCount(
      TableRepository tables, ViewRepository views, ResourceId catalogId, ResourceId namespaceId) {
    String accountId = catalogId.getAccountId();
    return tables.countConsistent(accountId, catalogId.getId(), namespaceId.getId())
        + views.countConsistent(accountId, catalogId.getId(), namespaceId.getId());
  }

  /**
   * Whether any namespace sits anywhere beneath {@code parentPath}.
   *
   * <p>Beneath, not immediately beneath. The by-path prefix ends at a separator and every segment
   * is percent-encoded, so it matches the whole descendant subtree and cannot match {@code
   * parentPath} itself or a sibling whose name merely starts with it.
   *
   * <p>Subtree-wide on purpose, and not merely as a convenience. A depth-one test is only
   * sufficient by induction -- a descendant can only be added by a writer that asserts its parent's
   * child marker, so the parent's marker suffices PROVIDED every namespace's parent exists. Every
   * writer now materialises its ancestors, so that induction holds going forward; it did not
   * always, so a row at {@code a/b/c} with no {@code a/b} row can still exist from before. Such a
   * row is a descendant of {@code a} that no immediate-child test can see, and renaming {@code a}
   * would then be allowed while nothing re-derives that row's by-path key -- exactly the staleness
   * this fence exists to prevent. A prefix count also costs one read where the depth-one test cost
   * a paged scan, and it holds whether or not the induction does.
   *
   * <p>Read strongly, because callers fence this answer with a marker sampled by a consistent point
   * read: a child committed just before that sample is already in the marker version, so an
   * eventually-consistent scan would report no descendants while the CAS matched.
   */
  public boolean hasDescendants(String accountId, String catalogId, List<String> parentPath) {
    return countConsistent(accountId, catalogId, parentPath) > 0;
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

  public List<Namespace> listConsistent(
      String accountId,
      String catalogId,
      List<String> parentSegmentsOrEmpty,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, parentSegmentsOrEmpty);
    return repo.listByPrefixConsistent(prefix, limit, pageToken, nextOut);
  }

  /**
   * The count read strongly, for a caller whose decision depends on emptiness.
   *
   * <p>The eventually-consistent count cannot be fenced against: a marker is sampled with a
   * consistent point read, so a namespace committed before that sample is already in the marker
   * version -- and if the index has not caught up, an emptiness check reads zero, the CAS matches
   * the version that write itself produced, and the delete commits over a live namespace.
   */
  public int countConsistent(
      String accountId, String catalogId, List<String> parentSegmentsOrEmpty) {
    return repo.countByPrefixConsistent(
        Keys.namespacePointerByPathPrefix(accountId, catalogId, parentSegmentsOrEmpty));
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

  /** The same read, past the cache, for a caller whose verdict a stale pointer would invert. */
  public MutationMeta pointerMetaForSafeConsistent(ResourceId namespaceResourceId) {
    return repo.pointerMetaForSafeConsistent(
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
