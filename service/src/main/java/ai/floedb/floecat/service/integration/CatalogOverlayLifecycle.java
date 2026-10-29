/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package ai.floedb.floecat.service.integration;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_DEPENDENT_OVERLAYS;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.service.catalog.impl.CascadeDeletion;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Shared ownership rules for direct and cascading overlay deletion. */
@ApplicationScoped
class CatalogOverlayLifecycle {
  @Inject CatalogOverlayRepository overlays;
  @Inject CatalogRepository catalogs;
  @Inject CascadeDeletion cascadeDeletion;

  /** Rejects a destructive operation before it installs a fence or retires managed content. */
  void preflightOwnedCatalogRemoval(CatalogOverlay owner, String corr) {
    if (!owner.getOwnsCatalog()) return;
    requireOwnedCatalog(owner);
    ensureUnshared(owner, corr);
  }

  /** Completes an already-fenced overlay delete, including its owned catalog when applicable. */
  boolean deleteFenced(
      CatalogOverlay owner, long overlayPointerVersion, long fenceVersion, String corr) {
    if (!owner.getOwnsCatalog()) {
      return overlays.deleteWithFence(owner.getResourceId(), overlayPointerVersion, fenceVersion);
    }

    ResourceWithMeta<Catalog> catalog = requireOwnedCatalog(owner);
    CascadeDeletion.PreparedCatalogDeletion prepared =
        cascadeDeletion.prepareCatalogDeletion(
            owner.getCatalogId(),
            catalog.meta(),
            () -> ensureUnshared(owner, corr),
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

  private ResourceWithMeta<Catalog> requireOwnedCatalog(CatalogOverlay owner) {
    return catalogs
        .getByIdWithMeta(owner.getCatalogId())
        .orElseThrow(
            () ->
                new BaseResourceRepository.CorruptionException(
                    "owned overlay catalog is missing: " + owner.getCatalogId().getId(), null));
  }

  private void ensureUnshared(CatalogOverlay owner, String corr) {
    int dependentOverlays =
        overlays.countByCatalog(owner.getCatalogId().getAccountId(), owner.getCatalogId().getId());
    if (dependentOverlays > 1) {
      throw GrpcErrors.conflict(
          corr,
          CATALOG_DEPENDENT_OVERLAYS,
          Map.of("dependent_overlays", Integer.toString(dependentOverlays - 1)));
    }
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
            () -> ensureUnshared(current, corr),
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
