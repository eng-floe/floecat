/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.integration;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.DurableDeletionFence;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Transactional driver for the synchronous part of a Catalog Integration cascade. */
@ApplicationScoped
class CatalogIntegrationCascadeDeletion {
  private static final int PAGE_SIZE = 100;

  @Inject CatalogIntegrationRepository integrations;
  @Inject CatalogOverlayRepository overlays;
  @Inject CatalogOverlayReconciler overlayReconciler;
  @Inject CatalogOverlayLifecycle overlayLifecycle;
  @Inject MarkerStore markerStore;

  MutationMeta delete(
      ResourceId integrationId,
      MutationMeta integrationMeta,
      String correlationId,
      Runnable requireOwnedCatalogDelete) {
    preflightOwnedCatalogs(integrationId, correlationId, requireOwnedCatalogDelete);
    DurableDeletionFence.Fence fence =
        integrations
            .acquireCascadeDeletionFence(integrationId, integrationMeta.getPointerVersion())
            .orElseThrow(
                () ->
                    new BaseResourceRepository.AbortRetryableException(
                        "integration changed while cascade deletion was fenced"));

    while (true) {
      var dependents =
          overlays.listByIntegrationWithMetaConsistent(
              integrationId.getAccountId(),
              integrationId.getId(),
              PAGE_SIZE,
              "",
              new StringBuilder());
      if (dependents.isEmpty()) break;
      for (var dependent : dependents) {
        ResourceId overlayId = dependent.value().getResourceId();
        if (dependent.value().getOwnsCatalog()) {
          try {
            requireOwnedCatalogDelete.run();
            overlayLifecycle.preflightOwnedCatalogRemoval(
                dependent.value(), correlationId, integrationId);
          } catch (StatusRuntimeException rejected) {
            if (fence.newlyInstalled()) {
              integrations.cancelCascadeDeletion(
                  integrationId, integrationMeta.getPointerVersion(), fence.version());
            }
            throw rejected;
          }
        }
        DurableDeletionFence.Fence overlayFence =
            overlays
                .acquireDeletionFence(overlayId, dependent.meta().getPointerVersion())
                .orElseThrow(
                    () ->
                        new BaseResourceRepository.AbortRetryableException(
                            "overlay changed while integration cascade deletion was fenced"));
        boolean deleted;
        if (dependent.value().getOwnsCatalog()) {
          try {
            deleted =
                overlayLifecycle.deleteFenced(
                    dependent.value(),
                    dependent.meta().getPointerVersion(),
                    overlayFence,
                    correlationId,
                    integrationId);
          } catch (StatusRuntimeException rejected) {
            if (fence.newlyInstalled()) {
              integrations.cancelCascadeDeletion(
                  integrationId, integrationMeta.getPointerVersion(), fence.version());
            }
            throw rejected;
          }
        } else {
          overlayReconciler.retireMaterializedResources(dependent.value());
          deleted =
              overlays.deleteWithFence(
                  overlayId, dependent.meta().getPointerVersion(), overlayFence.version());
        }
        if (!deleted) {
          throw new BaseResourceRepository.AbortRetryableException(
              "overlay changed during integration cascade deletion");
        }
      }
    }

    if (overlays.countByIntegration(integrationId.getAccountId(), integrationId.getId()) != 0) {
      throw new BaseResourceRepository.AbortRetryableException(
          "overlays appeared during integration cascade deletion");
    }
    long markerVersion = markerStore.catalogIntegrationOverlaysMarkerVersion(integrationId);
    if (!integrations.deleteWithPreconditionForCascadeDeletion(
        integrationId, integrationMeta.getPointerVersion(), markerVersion, fence.version())) {
      throw new BaseResourceRepository.AbortRetryableException(
          "integration changed while cascade deletion completed");
    }
    return integrationMeta;
  }

  private void preflightOwnedCatalogs(
      ResourceId integrationId, String correlationId, Runnable requireOwnedCatalogDelete) {
    String token = "";
    do {
      var next = new StringBuilder();
      var dependents =
          overlays.listByIntegrationWithMetaConsistent(
              integrationId.getAccountId(), integrationId.getId(), PAGE_SIZE, token, next);
      for (var dependent : dependents) {
        if (!dependent.value().getOwnsCatalog()) continue;
        requireOwnedCatalogDelete.run();
        overlayLifecycle.preflightOwnedCatalogRemoval(
            dependent.value(), correlationId, integrationId);
      }
      token = next.toString();
    } while (!token.isEmpty());
  }
}
