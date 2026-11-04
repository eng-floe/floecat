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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Transactional driver for the synchronous part of a Catalog Integration cascade. */
@ApplicationScoped
class CatalogIntegrationCascadeDeletion {
  private static final int PAGE_SIZE = 100;

  @Inject CatalogIntegrationRepository integrations;
  @Inject CatalogOverlayRepository overlays;
  @Inject CatalogOverlayReconciler overlayReconciler;
  @Inject MarkerStore markerStore;

  MutationMeta delete(ResourceId integrationId, MutationMeta integrationMeta) {
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
        DurableDeletionFence.Fence overlayFence =
            overlays
                .acquireDeletionFence(overlayId, dependent.meta().getPointerVersion())
                .orElseThrow(
                    () ->
                        new BaseResourceRepository.AbortRetryableException(
                            "overlay changed while integration cascade deletion was fenced"));
        overlayReconciler.retireMaterializedResources(dependent.value());
        if (!overlays.deleteWithFence(
            overlayId, dependent.meta().getPointerVersion(), overlayFence.version())) {
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
}
