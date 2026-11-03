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

package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.DurableDeletionFence;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The container half of a cascading catalog or namespace delete.
 *
 * <p>Deletes rows that give relations their place in the tree -- namespaces and overlays -- and
 * nothing beneath them. A relation whose namespace is gone stops resolving at once ({@code
 * MetaGraph}) and is reclaimed by {@code PointerGc}, so the cost of a delete grows with the number
 * of namespaces, not with the number of tables.
 *
 * <p>Every namespace is durably fenced before any row in the captured subtree is removed. Relation
 * and child-namespace writers assert that their immediate namespace fence is absent. A crash thus
 * leaves a resumable deletion rather than reopening writes into a partially removed tree.
 */
@ApplicationScoped
public class CascadeDeletion {
  private static final int PAGE_SIZE = 200;

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject CatalogOverlayRepository overlays;
  @Inject MarkerStore markerStore;

  /** Runs the complete synchronous portion of a catalog cascade behind one durable fence. */
  public MutationMeta deleteCatalog(
      ResourceId catalogId, MutationMeta catalogMeta, Runnable requireOverlayDelete) {
    PreparedCatalogDeletion prepared =
        prepareCatalogDeletion(catalogId, catalogMeta, requireOverlayDelete, null, () -> {});
    if (!catalogRepo.deleteForCascade(
        catalogId,
        catalogMeta.getPointerVersion(),
        prepared.fence().version(),
        prepared.childMarkers())) {
      throw BaseResourceRepository.AbortRetryableException.lostFence("catalog cascade deletion");
    }
    return catalogMeta;
  }

  /**
   * Fences a catalog and removes its overlays and namespace tree, leaving final deletion to the
   * caller.
   *
   * <p>An owning overlay uses this form so its own row can remain as the transactional driver: the
   * final batch removes that overlay, the emptied catalog, both fences and the catalog markers
   * together. All other callers pass no retained overlay and use {@link #deleteCatalog}.
   *
   * <p>{@code requireOverlayDelete} runs before anything is removed. {@code validateBeforeDelete}
   * runs after the Catalog fence is installed but before cleanup begins; a rejection releases a
   * fence installed by this attempt. A caller may therefore treat either rejection as leaving the
   * catalog and its overlays intact.
   */
  public PreparedCatalogDeletion prepareCatalogDeletion(
      ResourceId catalogId,
      MutationMeta catalogMeta,
      Runnable requireOverlayDelete,
      ResourceId retainedOverlayId,
      Runnable validateBeforeDelete) {
    boolean hadOverlays = hasDeletableOverlays(catalogId, retainedOverlayId);
    if (hadOverlays) requireOverlayDelete.run();

    DurableDeletionFence.Fence fence =
        catalogRepo
            .acquireCascadeDeletionFence(catalogId, catalogMeta.getPointerVersion())
            .orElseThrow(
                () ->
                    new BaseResourceRepository.AbortRetryableException(
                        "catalog changed while cascade deletion was fenced"));

    try {
      validateBeforeDelete.run();
    } catch (RuntimeException rejected) {
      if (fence.newlyInstalled()) {
        catalogRepo.releaseCascadeDeletionFence(catalogId, fence.version());
      }
      throw rejected;
    }

    if (hasDeletableOverlays(catalogId, retainedOverlayId)) {
      if (!hadOverlays) {
        try {
          requireOverlayDelete.run();
        } catch (RuntimeException denied) {
          if (fence.newlyInstalled()) {
            catalogRepo.releaseCascadeDeletionFence(catalogId, fence.version());
          }
          throw denied;
        }
      }
      deleteCatalogOverlays(catalogId, retainedOverlayId);
    }
    deleteDescendantNamespaces(catalogId, List.of());

    MarkerStore.MarkerRemoval childMarkers = markerStore.catalogChildSetMarkers(catalogId);
    if (!onlyRetainedOverlayRemains(catalogId, retainedOverlayId)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "overlay created during catalog cascade deletion");
    }
    if (namespaceRepo.countConsistent(catalogId.getAccountId(), catalogId.getId(), List.of()) > 0) {
      throw new BaseResourceRepository.AbortRetryableException(
          "namespace created during catalog cascade deletion");
    }
    return new PreparedCatalogDeletion(fence, childMarkers);
  }

  /** Runs the complete synchronous portion of a namespace cascade behind one durable fence. */
  public MutationMeta deleteNamespace(Namespace namespace, MutationMeta namespaceMeta) {
    ResourceId namespaceId = namespace.getResourceId();
    DurableDeletionFence.Fence fence =
        namespaceRepo
            .acquireCascadeDeletionFence(namespaceId, namespaceMeta.getPointerVersion())
            .orElseThrow(
                () ->
                    new BaseResourceRepository.AbortRetryableException(
                        "namespace changed while cascade deletion was fenced"));
    List<String> path = new ArrayList<>(namespace.getParentsList());
    path.add(namespace.getDisplayName());
    deleteDescendantNamespaces(namespace.getCatalogId(), path);

    MarkerStore.MarkerRemoval shapeMarkers = markerStore.namespaceShapeMarkers(namespaceId);
    if (namespaceRepo.hasDescendants(
        namespace.getCatalogId().getAccountId(), namespace.getCatalogId().getId(), path)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "namespace gained a child during cascade deletion");
    }
    if (!namespaceRepo.deleteForCascade(
        namespaceId,
        namespace.getCatalogId(),
        namespaceMeta.getPointerVersion(),
        fence.version(),
        shapeMarkers)) {
      throw BaseResourceRepository.AbortRetryableException.lostFence("namespace cascade deletion");
    }
    return namespaceMeta;
  }

  /**
   * Deletes every namespace strictly beneath {@code path} in the catalog, deepest first.
   *
   * <p>Relations under them are left for GC. Retryable when a child namespace or a concurrent
   * update lands mid-way; the rows already deleted stay deleted, so a retry makes progress.
   */
  public void deleteDescendantNamespaces(ResourceId catalogId, List<String> path) {
    String accountId = catalogId.getAccountId();
    List<ResourceWithMeta<Namespace>> subtree = new ArrayList<>();
    String token = "";
    do {
      var next = new StringBuilder();
      subtree.addAll(
          namespaceRepo.listWithMetaConsistent(
              accountId, catalogId.getId(), path, PAGE_SIZE, token, next));
      token = next.toString();
    } while (!token.isEmpty());

    // Fence the whole observed subtree first. Once a namespace is fenced, neither relations nor
    // child namespaces can join it; a child that committed just before its parent was fenced is
    // detected below and picked up by the durable retry.
    List<FencedNamespace> fenced = new ArrayList<>(subtree.size());
    for (ResourceWithMeta<Namespace> namespace : subtree) {
      DurableDeletionFence.Fence fence =
          namespaceRepo
              .acquireCascadeDeletionFence(
                  namespace.value().getResourceId(), namespace.meta().getPointerVersion())
              .orElseThrow(
                  () ->
                      new BaseResourceRepository.AbortRetryableException(
                          "namespace changed while cascade deletion was fenced"));
      fenced.add(new FencedNamespace(namespace, fence));
    }

    fenced.sort(
        Comparator.comparingInt(
                (FencedNamespace entry) -> entry.namespace().value().getParentsCount())
            .reversed());
    for (FencedNamespace namespace : fenced) {
      deleteChildlessNamespace(catalogId, namespace);
    }
  }

  /**
   * Deletes every overlay of the catalog, except {@code retainedOverlayId} when given, without
   * retiring what it materialized.
   *
   * <p>Retiring would walk every relation in the catalog to find the overlay's own, and the
   * namespace cascade that follows deletes all of them anyway. The deletion fence still goes in
   * first, so the overlay reconciler stops materializing before the row goes.
   */
  private void deleteCatalogOverlays(ResourceId catalogId, ResourceId retainedOverlayId) {
    while (true) {
      var dependents =
          overlays.listByCatalogWithMetaConsistent(
              catalogId.getAccountId(), catalogId.getId(), PAGE_SIZE, "", new StringBuilder());
      var deletable =
          dependents.stream()
              .filter(row -> !row.value().getResourceId().equals(retainedOverlayId))
              .toList();
      if (deletable.isEmpty()) {
        return;
      }
      for (var dependent : deletable) {
        ResourceId overlayId = dependent.value().getResourceId();
        long version = dependent.meta().getPointerVersion();
        DurableDeletionFence.Fence fence =
            overlays
                .acquireDeletionFence(overlayId, version)
                .orElseThrow(
                    () ->
                        new BaseResourceRepository.AbortRetryableException(
                            "overlay changed during catalog cascade deletion"));
        if (!overlays.deleteWithFence(overlayId, version, fence.version())) {
          throw new BaseResourceRepository.AbortRetryableException(
              "overlay changed during catalog cascade deletion");
        }
      }
    }
  }

  private boolean hasDeletableOverlays(ResourceId catalogId, ResourceId retainedOverlayId) {
    int count = overlays.countByCatalog(catalogId.getAccountId(), catalogId.getId());
    if (retainedOverlayId == null) return count > 0;
    boolean retained =
        overlays
            .getById(retainedOverlayId)
            .map(overlay -> overlay.getCatalogId().equals(catalogId))
            .orElse(false);
    return count > (retained ? 1 : 0);
  }

  private boolean onlyRetainedOverlayRemains(ResourceId catalogId, ResourceId retainedOverlayId) {
    int count = overlays.countByCatalog(catalogId.getAccountId(), catalogId.getId());
    if (retainedOverlayId == null) return count == 0;
    return count == 1
        && overlays
            .getById(retainedOverlayId)
            .map(overlay -> overlay.getCatalogId().equals(catalogId))
            .orElse(false);
  }

  /**
   * Deletes one namespace that must have no child namespaces, whatever relations it holds.
   *
   * <p>The same protocol as a plain namespace delete -- markers sampled before the check they
   * guard, then removed with the row -- without the relation check. A namespace already gone is
   * success: a concurrent cascade got there first.
   */
  private void deleteChildlessNamespace(ResourceId catalogId, FencedNamespace fenced) {
    ResourceWithMeta<Namespace> entry = fenced.namespace();
    Namespace namespace = entry.value();
    ResourceId namespaceId = namespace.getResourceId();
    var markers = markerStore.namespaceShapeMarkers(namespaceId);
    var path = new ArrayList<>(namespace.getParentsList());
    path.add(namespace.getDisplayName());
    if (namespaceRepo.hasDescendants(catalogId.getAccountId(), catalogId.getId(), path)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "namespace gained a child during cascade deletion");
    }
    if (!namespaceRepo.deleteForCascade(
        namespaceId,
        catalogId,
        entry.meta().getPointerVersion(),
        fenced.fence().version(),
        markers)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "namespace changed during cascade deletion");
    }
  }

  private record FencedNamespace(
      ResourceWithMeta<Namespace> namespace, DurableDeletionFence.Fence fence) {}

  public record PreparedCatalogDeletion(
      DurableDeletionFence.Fence fence, MarkerStore.MarkerRemoval childMarkers) {}
}
