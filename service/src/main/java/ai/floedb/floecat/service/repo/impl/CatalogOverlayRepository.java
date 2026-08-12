/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.service.repo.model.CatalogOverlayKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@ApplicationScoped
public class CatalogOverlayRepository {

  private final GenericResourceRepository<CatalogOverlay, CatalogOverlayKey> repo;

  @Inject
  public CatalogOverlayRepository(PointerStore pointerStore, BlobStore blobStore) {
    repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.CATALOG_OVERLAY,
            CatalogOverlay::parseFrom,
            CatalogOverlay::toByteArray,
            "application/x-protobuf");
  }

  public void create(CatalogOverlay overlay) {
    repo.create(overlay);
  }

  public ResourceWithMeta<CatalogOverlay> createWithMeta(CatalogOverlay overlay) {
    return repo.createWithMeta(overlay);
  }

  public boolean createAttached(
      CatalogOverlay overlay,
      Map<String, Long> requiredPointerVersions,
      Set<String> requiredAbsentPointers,
      long expectedIntegrationMarkerVersion) {
    return createAttachedWithMeta(
            overlay,
            requiredPointerVersions,
            requiredAbsentPointers,
            expectedIntegrationMarkerVersion)
        .isPresent();
  }

  public Optional<ResourceWithMeta<CatalogOverlay>> createAttachedWithMeta(
      CatalogOverlay overlay,
      Map<String, Long> requiredPointerVersions,
      Set<String> requiredAbsentPointers,
      long expectedIntegrationMarkerVersion) {
    return createAttachedWithMetaAndCompanions(
        overlay,
        requiredPointerVersions,
        requiredAbsentPointers,
        expectedIntegrationMarkerVersion,
        null);
  }

  /**
   * Creates an overlay, advancing its integration's dependency marker and publishing the caller's
   * companion operations in the same atomic pointer transaction. The companions carry the catalog
   * the overlay owns, and an idempotency receipt when the create is keyed, so an overlay can never
   * be visible without its catalog.
   */
  public Optional<ResourceWithMeta<CatalogOverlay>> createAttachedWithMetaAndCompanions(
      CatalogOverlay overlay,
      Map<String, Long> requiredPointerVersions,
      Set<String> requiredAbsentPointers,
      long expectedIntegrationMarkerVersion,
      Function<ResourceWithMeta<CatalogOverlay>, List<PointerStore.CasOp>> companions) {
    String integrationMarker =
        Keys.catalogIntegrationOverlaysMarker(
            overlay.getResourceId().getAccountId(), overlay.getIntegrationId().getId());
    return repo.createWithMeta(
        overlay,
        new PointerConditions(
            requiredPointerVersions,
            requiredAbsentPointers,
            Map.of(integrationMarker, expectedIntegrationMarkerVersion)),
        companions);
  }

  /**
   * Deletes an overlay and the catalog it owns in one atomic pointer transaction. The caller
   * supplies the owned catalog's delete operations; an empty list means that catalog is already
   * gone, which a retried cascade treats as work already done.
   */
  public boolean deleteWithOwnedCatalog(
      ResourceId overlayId,
      long expectedPointerVersion,
      List<PointerStore.CasOp> ownedCatalogDeletes) {
    return repo.deleteWithPreconditionAndCompanions(
        key(overlayId), expectedPointerVersion, ownedCatalogDeletes);
  }

  public boolean update(CatalogOverlay overlay, long expectedPointerVersion) {
    return repo.update(overlay, expectedPointerVersion);
  }

  public Optional<MutationMeta> updateWithMetaUnlessIntegrationDeleting(
      CatalogOverlay overlay, long expectedPointerVersion) {
    return updateWithMetaUnlessIntegrationDeleting(overlay, expectedPointerVersion, List.of());
  }

  /**
   * Updates an overlay and publishes the caller's companion operations in the same transaction. A
   * rename carries the owned catalog's rename operations here, so the overlay and its catalog can
   * never end up under different names.
   */
  public Optional<MutationMeta> updateWithMetaUnlessIntegrationDeleting(
      CatalogOverlay overlay, long expectedPointerVersion, List<PointerStore.CasOp> companions) {
    String fence =
        Keys.catalogIntegrationDeletionMarker(
            overlay.getResourceId().getAccountId(), overlay.getIntegrationId().getId());
    return repo.updateWithMetaWhilePointersMatchAndBumpMarkers(
        overlay,
        expectedPointerVersion,
        new PointerConditions(Map.of(), Set.of(fence), Map.of()),
        companions);
  }

  public Optional<ResourceWithMeta<CatalogOverlay>> replaceIdentityAttachedWithMeta(
      CatalogOverlay current,
      long expectedPointerVersion,
      CatalogOverlay replacement,
      Map<String, Long> requiredPointerVersions,
      Set<String> requiredAbsentPointers,
      Map<String, Long> integrationMarkerVersions,
      List<PointerStore.CasOp> companions) {
    return repo.replaceIdentityWithMeta(
        current,
        expectedPointerVersion,
        replacement,
        new PointerConditions(
            requiredPointerVersions, requiredAbsentPointers, integrationMarkerVersions),
        Map.of(),
        companions);
  }

  public boolean deleteWithPrecondition(ResourceId overlayId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(key(overlayId), expectedPointerVersion);
  }

  public boolean delete(ResourceId overlayId) {
    return repo.delete(key(overlayId));
  }

  public void deleteOrConfirmAbsent(ResourceId overlayId) {
    repo.deleteOrConfirmAbsent(key(overlayId));
  }

  public Optional<CatalogOverlay> getById(ResourceId overlayId) {
    return repo.getByKey(key(overlayId));
  }

  public Optional<ResourceWithMeta<CatalogOverlay>> getByIdWithMeta(ResourceId overlayId) {
    return repo.getByKeyWithMeta(key(overlayId));
  }

  public Optional<CatalogOverlay> getByName(String accountId, String displayName) {
    return repo.get(Keys.catalogOverlayPointerByName(accountId, displayName));
  }

  public Optional<ResourceWithMeta<CatalogOverlay>> getByNameWithMeta(
      String accountId, String displayName) {
    return repo.getWithMeta(Keys.catalogOverlayPointerByName(accountId, displayName));
  }

  public List<CatalogOverlay> list(
      String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(
        Keys.catalogOverlayPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public List<CatalogOverlay> listConsistent(
      String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefixConsistent(
        Keys.catalogOverlayPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public List<ResourceWithMeta<CatalogOverlay>> listWithMeta(
      String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefixWithMeta(
        Keys.catalogOverlayPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public List<CatalogOverlay> listByIntegration(
      String accountId, String integrationId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(
        Keys.catalogOverlayPointerByIntegrationPrefix(accountId, integrationId),
        limit,
        pageToken,
        nextOut);
  }

  public List<ResourceWithMeta<CatalogOverlay>> listByIntegrationWithMeta(
      String accountId, String integrationId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefixWithMeta(
        Keys.catalogOverlayPointerByIntegrationPrefix(accountId, integrationId),
        limit,
        pageToken,
        nextOut);
  }

  public List<ResourceWithMeta<CatalogOverlay>> listByIntegrationWithMetaConsistent(
      String accountId, String integrationId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefixWithMetaConsistent(
        Keys.catalogOverlayPointerByIntegrationPrefix(accountId, integrationId),
        limit,
        pageToken,
        nextOut);
  }

  public int count(String accountId) {
    return repo.countByPrefix(Keys.catalogOverlayPointerByNamePrefix(accountId));
  }

  public int countByIntegration(String accountId, String integrationId) {
    return repo.countByPrefixConsistent(
        Keys.catalogOverlayPointerByIntegrationPrefix(accountId, integrationId));
  }

  public MutationMeta metaFor(ResourceId overlayId) {
    return repo.metaFor(key(overlayId));
  }

  public MutationMeta metaForSafe(ResourceId overlayId) {
    return repo.metaForSafe(key(overlayId));
  }

  private static CatalogOverlayKey key(ResourceId id) {
    return new CatalogOverlayKey(id.getAccountId(), id.getId());
  }
}
