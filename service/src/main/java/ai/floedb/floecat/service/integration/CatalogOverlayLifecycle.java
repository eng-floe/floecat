/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package ai.floedb.floecat.service.integration;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_DEPENDENT_OVERLAYS;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_NOT_EMPTY;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.service.repo.util.MarkerStore;
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
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject ViewRepository views;
  @Inject MarkerStore markerStore;

  /** Rejects a destructive operation before it installs a fence or retires managed content. */
  void preflightOwnedCatalogRemoval(CatalogOverlay owner, String corr) {
    if (!owner.getOwnsCatalog()) return;
    Catalog catalog = requireOwnedCatalog(owner);
    ensureUnshared(owner, corr);
    if (hasContentRetirementWouldLeave(owner)) {
      throw GrpcErrors.conflict(
          corr, CATALOG_NOT_EMPTY, Map.of("display_name", catalog.getDisplayName()));
    }
  }

  /** Completes an already-fenced overlay delete, including its owned catalog when applicable. */
  boolean deleteFenced(
      CatalogOverlay owner,
      long overlayPointerVersion,
      long fenceVersion,
      boolean ownsFence,
      String corr) {
    if (!owner.getOwnsCatalog()) {
      return overlays.deleteWithFence(owner.getResourceId(), overlayPointerVersion, fenceVersion);
    }

    Catalog catalog = requireOwnedCatalog(owner);
    var childMarkers = markerStore.catalogChildSetMarkers(owner.getCatalogId());
    try {
      ensureUnshared(owner, corr);
      if (namespaces.countConsistent(
              owner.getCatalogId().getAccountId(), owner.getCatalogId().getId(), List.of())
          > 0) {
        throw GrpcErrors.conflict(
            corr, CATALOG_NOT_EMPTY, Map.of("display_name", catalog.getDisplayName()));
      }
    } catch (RuntimeException blocked) {
      if (ownsFence) cancelFenceOrRetry(owner, overlayPointerVersion, fenceVersion);
      throw blocked;
    }
    boolean deleted =
        overlays.deleteWithFenceAndCompanions(
            owner.getResourceId(),
            overlayPointerVersion,
            fenceVersion,
            childMarkers.conditions(),
            childMarkers.toDelete(),
            catalogs.prepareDeleteOps(owner.getCatalogId()));
    if (!deleted && ownsFence) {
      cancelFenceOrRetry(owner, overlayPointerVersion, fenceVersion);
    }
    return deleted;
  }

  private Catalog requireOwnedCatalog(CatalogOverlay owner) {
    return catalogs
        .getById(owner.getCatalogId())
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
    var childMarkers = markerStore.catalogChildSetMarkers(current.getCatalogId());
    ensureUnshared(current, corr);
    if (namespaces.countConsistent(
            current.getCatalogId().getAccountId(), current.getCatalogId().getId(), List.of())
        > 0) {
      throw GrpcErrors.conflict(
          corr, CATALOG_NOT_EMPTY, Map.of("display_name", catalog.value().getDisplayName()));
    }
    requiredVersions.remove(
        Keys.catalogPointerById(
            current.getCatalogId().getAccountId(), current.getCatalogId().getId()));
    markerVersions.remove(
        Keys.catalogOverlaysMarker(
            current.getCatalogId().getAccountId(), current.getCatalogId().getId()));
    requiredVersions.putAll(childMarkers.conditions().requiredVersions());
    requiredAbsent.addAll(childMarkers.conditions().requiredAbsent());
    pointerVersionsToDelete.putAll(childMarkers.toDelete());
    companions.addAll(catalogs.prepareDeleteOps(current.getCatalogId()));
  }

  private boolean hasContentRetirementWouldLeave(CatalogOverlay owner) {
    String token = "";
    do {
      var next = new StringBuilder();
      var page =
          namespaces.listConsistent(
              owner.getCatalogId().getAccountId(),
              owner.getCatalogId().getId(),
              List.of(),
              100,
              token,
              next);
      for (Namespace namespace : page) {
        if (!CatalogOverlayReconciler.ownedBy(namespace.getPropertiesMap(), owner)
            || hasUnmanagedTables(namespace, owner)
            || hasUnmanagedViews(namespace, owner)) {
          return true;
        }
      }
      token = next.toString();
    } while (!token.isEmpty());
    return false;
  }

  private boolean hasUnmanagedTables(Namespace namespace, CatalogOverlay owner) {
    String token = "";
    do {
      var next = new StringBuilder();
      var page =
          tables.listConsistent(
              owner.getCatalogId().getAccountId(),
              owner.getCatalogId().getId(),
              namespace.getResourceId().getId(),
              100,
              token,
              next);
      if (page.stream()
          .anyMatch(table -> !CatalogOverlayReconciler.ownedBy(table.getPropertiesMap(), owner))) {
        return true;
      }
      token = next.toString();
    } while (!token.isEmpty());
    return false;
  }

  private boolean hasUnmanagedViews(Namespace namespace, CatalogOverlay owner) {
    String token = "";
    do {
      var next = new StringBuilder();
      var page =
          views.listConsistent(
              owner.getCatalogId().getAccountId(),
              owner.getCatalogId().getId(),
              namespace.getResourceId().getId(),
              100,
              token,
              next);
      if (page.stream()
          .anyMatch(view -> !CatalogOverlayReconciler.ownedBy(view.getPropertiesMap(), owner))) {
        return true;
      }
      token = next.toString();
    } while (!token.isEmpty());
    return false;
  }

  void cancelFenceOrRetry(CatalogOverlay owner, long overlayPointerVersion, long fenceVersion) {
    if (!overlays.cancelDeletion(owner.getResourceId(), overlayPointerVersion, fenceVersion)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "overlay changed while its deletion fence was being cancelled");
    }
  }
}
