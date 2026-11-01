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
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
      CatalogOverlay owner, long overlayPointerVersion, long fenceVersion, String corr) {
    return deleteFenced(owner, overlayPointerVersion, fenceVersion, corr, null);
  }

  boolean deleteFenced(
      CatalogOverlay owner,
      long overlayPointerVersion,
      long fenceVersion,
      String corr,
      ResourceId deletingIntegrationId) {
    if (!owner.getOwnsCatalog()) {
      return overlays.deleteWithFence(owner.getResourceId(), overlayPointerVersion, fenceVersion);
    }

    Optional<ResourceWithMeta<Catalog>> catalog = ownedCatalog(owner);
    if (catalog.isEmpty()) {
      return overlays.deleteWithFence(owner.getResourceId(), overlayPointerVersion, fenceVersion);
    }
    CascadeDeletion.PreparedCatalogDeletion prepared =
        cascadeDeletion.prepareCatalogDeletion(
            owner.getCatalogId(),
            catalog.get().meta(),
            () -> ensureUnshared(owner, corr, deletingIntegrationId),
            owner.getResourceId());
    Map<String, Long> deletes = new java.util.HashMap<>(prepared.childMarkers().toDelete());
    deletes.put(
        Keys.catalogDeletionMarker(
            owner.getCatalogId().getAccountId(), owner.getCatalogId().getId()),
        prepared.fence().version());
    return overlays.deleteWithFenceAndCompanions(
        owner.getResourceId(),
        overlayPointerVersion,
        fenceVersion,
        prepared.childMarkers().conditions(),
        Map.copyOf(deletes),
        catalogs.prepareDeleteOps(owner.getCatalogId()));
  }

  private Optional<ResourceWithMeta<Catalog>> ownedCatalog(CatalogOverlay owner) {
    return catalogs.getByIdWithMeta(owner.getCatalogId());
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

  void prepareOwnedCatalogRemoval(
      CatalogOverlay current,
      ResourceWithMeta<Catalog> catalog,
      Map<String, Long> requiredVersions,
      Set<String> requiredAbsent,
      Map<String, Long> markerVersions,
      Map<String, Long> pointerVersionsToDelete,
      List<PointerStore.CasOp> companions,
      String corr) {
    CascadeDeletion.PreparedCatalogDeletion prepared =
        cascadeDeletion.prepareCatalogDeletion(
            current.getCatalogId(),
            catalog.meta(),
            () -> ensureUnshared(current, corr, null),
            current.getResourceId());
    requiredVersions.remove(
        Keys.catalogPointerById(
            current.getCatalogId().getAccountId(), current.getCatalogId().getId()));
    markerVersions.remove(
        Keys.catalogOverlaysMarker(
            current.getCatalogId().getAccountId(), current.getCatalogId().getId()));
    requiredVersions.putAll(prepared.childMarkers().conditions().requiredVersions());
    requiredAbsent.addAll(prepared.childMarkers().conditions().requiredAbsent());
    pointerVersionsToDelete.putAll(prepared.childMarkers().toDelete());
    pointerVersionsToDelete.put(
        Keys.catalogDeletionMarker(
            current.getCatalogId().getAccountId(), current.getCatalogId().getId()),
        prepared.fence().version());
    companions.addAll(catalogs.prepareDeleteOps(current.getCatalogId()));
  }
}
