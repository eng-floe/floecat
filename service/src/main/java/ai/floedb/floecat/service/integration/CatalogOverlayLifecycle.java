/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package ai.floedb.floecat.service.integration;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_DEPENDENT_OVERLAYS;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.service.catalog.impl.CascadeDeletion;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.DurableDeletionFence;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Shared ownership rules for direct and cascading overlay deletion. */
@ApplicationScoped
class CatalogOverlayLifecycle {
  private static final int PAGE_SIZE = 100;

  @Inject CatalogOverlayRepository overlays;
  @Inject CatalogRepository catalogs;
  @Inject CascadeDeletion cascadeDeletion;

  /** Rejects a destructive operation before it installs a fence or retires managed content. */
  void preflightOwnedCatalogRemoval(CatalogOverlay owner, String corr) {
    preflightOwnedCatalogRemoval(owner, corr, null);
  }

  void preflightOwnedCatalogRemoval(
      CatalogOverlay owner, String corr, ResourceId deletingIntegrationId) {
    if (!owner.getOwnsCatalog()) return;
    if (ownedCatalog(owner).isEmpty()) return;
    ensureUnshared(owner, corr, deletingIntegrationId);
  }

  /** Completes an already-fenced overlay delete, including its owned catalog when applicable. */
  boolean deleteFenced(
      CatalogOverlay owner,
      long overlayPointerVersion,
      DurableDeletionFence.Fence overlayFence,
      String corr) {
    return deleteFenced(owner, overlayPointerVersion, overlayFence, corr, null);
  }

  boolean deleteFenced(
      CatalogOverlay owner,
      long overlayPointerVersion,
      DurableDeletionFence.Fence overlayFence,
      String corr,
      ResourceId deletingIntegrationId) {
    if (!owner.getOwnsCatalog()) {
      return overlays.deleteWithFence(
          owner.getResourceId(), overlayPointerVersion, overlayFence.version());
    }

    Optional<ResourceWithMeta<Catalog>> catalog = ownedCatalog(owner);
    if (catalog.isEmpty()) {
      return overlays.deleteWithFence(
          owner.getResourceId(), overlayPointerVersion, overlayFence.version());
    }
    OwnedCatalogRemoval removal =
        prepareOwnedCatalogRemoval(owner, catalog.get(), overlayFence, corr, deletingIntegrationId);
    return overlays.deleteWithFenceAndCompanions(
        owner.getResourceId(),
        overlayPointerVersion,
        overlayFence.version(),
        removal.conditions(),
        removal.deletes(),
        removal.companions());
  }

  private Optional<ResourceWithMeta<Catalog>> ownedCatalog(CatalogOverlay owner) {
    return catalogs.getByIdWithMeta(owner.getCatalogId());
  }

  /**
   * Rejects a shared catalog. The cascade runs this before it removes anything, so a rejected
   * attempt withdraws the overlay fence it installed: the overlay was never partly deleted and must
   * stay updatable and reconcilable. A fence an earlier attempt installed is left alone, since that
   * attempt may have made progress this one cannot see.
   */
  private void requireUnshared(
      CatalogOverlay owner,
      DurableDeletionFence.Fence overlayFence,
      String corr,
      ResourceId deletingIntegrationId) {
    try {
      ensureUnshared(owner, corr, deletingIntegrationId);
    } catch (RuntimeException rejected) {
      if (overlayFence.newlyInstalled()) {
        overlays.releaseDeletionFence(owner.getResourceId(), overlayFence.version());
      }
      throw rejected;
    }
  }

  private void ensureUnshared(CatalogOverlay owner, String corr, ResourceId deletingIntegrationId) {
    int dependentOverlays;
    if (deletingIntegrationId == null) {
      dependentOverlays =
          Math.max(
              0,
              overlays.countByCatalog(
                      owner.getCatalogId().getAccountId(), owner.getCatalogId().getId())
                  - 1);
    } else {
      dependentOverlays = countExternalDependents(owner, deletingIntegrationId);
    }
    if (dependentOverlays > 0) {
      throw GrpcErrors.conflict(
          corr,
          CATALOG_DEPENDENT_OVERLAYS,
          Map.of("dependent_overlays", Integer.toString(dependentOverlays)));
    }
  }

  private int countExternalDependents(CatalogOverlay owner, ResourceId deletingIntegrationId) {
    int count = 0;
    String token = "";
    do {
      var next = new StringBuilder();
      var dependents =
          overlays.listByCatalogWithMetaConsistent(
              owner.getCatalogId().getAccountId(),
              owner.getCatalogId().getId(),
              PAGE_SIZE,
              token,
              next);
      count +=
          (int)
              dependents.stream()
                  .filter(row -> !row.value().getIntegrationId().equals(deletingIntegrationId))
                  .count();
      token = next.toString();
    } while (!token.isEmpty());
    return count;
  }

  /**
   * Fences and empties an owned catalog, returning what the overlay's final batch must carry to
   * remove the catalog with it.
   */
  OwnedCatalogRemoval prepareOwnedCatalogRemoval(
      CatalogOverlay owner,
      ResourceWithMeta<Catalog> catalog,
      DurableDeletionFence.Fence overlayFence,
      String corr) {
    return prepareOwnedCatalogRemoval(owner, catalog, overlayFence, corr, null);
  }

  private OwnedCatalogRemoval prepareOwnedCatalogRemoval(
      CatalogOverlay owner,
      ResourceWithMeta<Catalog> catalog,
      DurableDeletionFence.Fence overlayFence,
      String corr,
      ResourceId deletingIntegrationId) {
    ResourceId catalogId = owner.getCatalogId();
    CascadeDeletion.PreparedCatalogDeletion prepared =
        cascadeDeletion.prepareCatalogDeletion(
            catalogId,
            catalog.meta(),
            () -> requireUnshared(owner, overlayFence, corr, deletingIntegrationId),
            owner.getResourceId());
    Map<String, Long> deletes = new HashMap<>(prepared.childMarkers().toDelete());
    deletes.put(
        Keys.catalogDeletionMarker(catalogId.getAccountId(), catalogId.getId()),
        prepared.fence().version());
    return new OwnedCatalogRemoval(
        prepared.childMarkers().conditions(), deletes, catalogs.prepareDeleteOps(catalogId));
  }

  /**
   * The conditions, pointer deletes and companion operations that remove an emptied owned catalog
   * in the same batch as its overlay's row.
   */
  record OwnedCatalogRemoval(
      PointerConditions conditions,
      Map<String, Long> deletes,
      List<PointerStore.CasOp> companions) {
    OwnedCatalogRemoval {
      deletes = Map.copyOf(deletes);
      companions = List.copyOf(companions);
    }

    /**
     * {@code base} together with this removal's conditions. A caller's version check or marker bump
     * on a key this removal governs is dropped: the removal deletes that key or requires it absent,
     * which already excludes a concurrent writer, and one batch cannot both advance a key and
     * remove it.
     */
    PointerConditions alongside(PointerConditions base) {
      Set<String> removed = new HashSet<>(deletes.keySet());
      companions.forEach(op -> removed.add(op.key()));
      removed.addAll(conditions.requiredVersions().keySet());
      removed.addAll(conditions.requiredAbsent());
      removed.addAll(conditions.markerVersions().keySet());
      Map<String, Long> versions = new HashMap<>(base.requiredVersions());
      Map<String, Long> markers = new HashMap<>(base.markerVersions());
      versions.keySet().removeAll(removed);
      markers.keySet().removeAll(removed);
      return new PointerConditions(versions, base.requiredAbsent(), markers).and(conditions);
    }
  }
}
