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
import ai.floedb.floecat.service.repo.util.BatchGuard;
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

  /**
   * Publishes a child namespace atomically with respect to deletion of its parent; see {@link
   * BatchGuard}.
   */
  public void create(Namespace namespace, BatchGuard parentGuard) {
    repo.create(namespace, parentGuard);
  }

  public boolean update(Namespace namespace, long expectedPointerVersion) {
    return repo.update(namespace, expectedPointerVersion);
  }

  /** Guarded update, for a reparent that publishes the namespace under a different parent. */
  public boolean update(Namespace namespace, long expectedPointerVersion, BatchGuard parentGuard) {
    return repo.update(namespace, expectedPointerVersion, parentGuard);
  }

  public boolean delete(ResourceId namespaceResourceId) {
    return repo.delete(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()));
  }

  /**
   * Removes a namespace atomically with respect to any child being published into it; see {@link
   * BatchGuard}.
   */
  public boolean delete(ResourceId namespaceResourceId, BatchGuard childrenGuard) {
    return repo.delete(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()),
        childrenGuard);
  }

  public boolean deleteWithPrecondition(
      ResourceId namespaceResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()),
        expectedPointerVersion);
  }

  /**
   * Guarded {@link #deleteWithPrecondition(ResourceId, long)}; see {@link #delete(ResourceId,
   * BatchGuard)}.
   */
  public boolean deleteWithPrecondition(
      ResourceId namespaceResourceId, long expectedPointerVersion, BatchGuard childrenGuard) {
    return repo.deleteWithPrecondition(
        new NamespaceKey(namespaceResourceId.getAccountId(), namespaceResourceId.getId()),
        expectedPointerVersion,
        childrenGuard);
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
   * The refs {@link #count} counts under {@code parentSegmentsOrEmpty}, with no blob fetch: id and
   * full path come from the by-path pointer row itself.
   *
   * <p>{@link #list} resolves each row's blob, so one present-but-unparseable namespace fails the
   * whole scan. Callers that only need identity and placement — a subtree walk, an immediate-child
   * probe — should not inherit that dependency.
   */
  public List<NamespaceRef> listRefsUnder(
      String accountId, String catalogId, List<String> parentSegmentsOrEmpty) {
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, parentSegmentsOrEmpty);
    var pointers = repo.listRefsByPrefix(prefix);
    var refs = new ArrayList<NamespaceRef>(pointers.size());
    ResourceId catalogResourceId = catalogResourceId(accountId, catalogId);
    for (var p : pointers) {
      toNamespaceRef(accountId, catalogId, catalogResourceId, p).ifPresent(refs::add);
    }
    return refs;
  }

  /**
   * The same refs, streamed a page at a time and in key order — which for by-path rows means each
   * namespace arrives before everything beneath it, and everything beneath it before any namespace
   * outside it.
   *
   * <p>That ordering is what lets a subtree walk hold O(depth) instead of the whole subtree: see
   * {@code RecursiveResourceDropper#forEachNamespaceDeepestFirst}. Callers that only need identity
   * and placement must not inherit {@link #list}'s dependency on every blob parsing, least of all
   * teardown, which runs after the account pointer is gone and cannot be retried.
   */
  public void forEachRefUnder(
      String accountId,
      String catalogId,
      List<String> parentSegmentsOrEmpty,
      java.util.function.Consumer<NamespaceRef> action) {
    forEachRefUnder(accountId, catalogId, parentSegmentsOrEmpty, action, unresolvable -> {});
  }

  /**
   * Same, with the rows that name no namespace handed to {@code onUnresolvable} instead of dropped.
   *
   * <p>A by-path row whose ref and blob URI both fail to yield an id resolves to nothing, so a
   * caller walking namespaces cannot act on it — but {@link #hasChildUnder} counts rows by key
   * shape and does count it. Silently skipping it therefore leaves a child that the emptiness gate
   * sees and the drop cannot remove, which is a permanent dead end. A caller that can reclaim such
   * a row must be able to see it.
   */
  public void forEachRefUnder(
      String accountId,
      String catalogId,
      List<String> parentSegmentsOrEmpty,
      java.util.function.Consumer<NamespaceRef> action,
      java.util.function.Consumer<ai.floedb.floecat.common.rpc.Pointer> onUnresolvable) {
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, parentSegmentsOrEmpty);
    ResourceId catalogResourceId = catalogResourceId(accountId, catalogId);
    repo.forEachRefByPrefix(
        prefix,
        pointer -> {
          var ref = toNamespaceRef(accountId, catalogId, catalogResourceId, pointer);
          if (ref.isPresent()) {
            action.accept(ref.get());
          } else {
            onUnresolvable.accept(pointer);
          }
        });
  }

  /**
   * Whether {@code parentPath} has at least one direct child namespace.
   *
   * <p>Streams pages and returns on the first hit rather than draining the prefix like {@link
   * #listRefsUnder}: the prefix covers the whole subtree, so materializing it turns an existence
   * check into work proportional to everything underneath. By-path keys sort so a direct child is
   * usually in the first page. Blob-free, so an unparseable child is still counted rather than
   * failing the probe.
   */
  public boolean hasChildUnder(String accountId, String catalogId, List<String> parentPath) {
    String prefix = Keys.namespacePointerByPathPrefix(accountId, catalogId, parentPath);
    var seenTokens = new java.util.HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      for (var row :
          pointerStore.listPointersByPrefix(prefix, CHILD_PROBE_PAGE_SIZE, token, next)) {
        if (Keys.extractNamespacePathSegments(accountId, catalogId, row.getKey()).size()
            == parentPath.size() + 1) {
          return true;
        }
      }
      token = next.toString();
      if (token.isBlank()) {
        return false;
      }
      // A store that returns a non-advancing cursor would spin here forever with nothing
      // observable.
      if (!seenTokens.add(token)) {
        throw new IllegalStateException(
            "pointer scan did not advance; repeated page token: " + token);
      }
    }
  }

  private static final int CHILD_PROBE_PAGE_SIZE = 200;

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

  /**
   * The ref at an exact path, from its by-path pointer row alone.
   *
   * <p>{@link #getByPath} parses the namespace's blob, so it throws {@code CorruptionException} for
   * a namespace that exists but cannot be read. Callers that only need identity — bumping a marker,
   * say — should not fail for that reason, least of all when they run after destructive work.
   */
  public Optional<NamespaceRef> refByPath(
      String accountId, String catalogId, List<String> pathSegments) {
    return repo.refByPointer(Keys.namespacePointerByPath(accountId, catalogId, pathSegments))
        .flatMap(
            p -> toNamespaceRef(accountId, catalogId, catalogResourceId(accountId, catalogId), p));
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
