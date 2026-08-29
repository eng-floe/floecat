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
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.AccountDeletionFence;
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
  private final PointerStore pointerStore;

  @Inject
  public CatalogOverlayRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.pointerStore = pointerStore;
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
      Map<String, Long> parentMarkerVersions) {
    return createAttachedWithMeta(
            overlay, requiredPointerVersions, requiredAbsentPointers, parentMarkerVersions)
        .isPresent();
  }

  public Optional<ResourceWithMeta<CatalogOverlay>> createAttachedWithMeta(
      CatalogOverlay overlay,
      Map<String, Long> requiredPointerVersions,
      Set<String> requiredAbsentPointers,
      Map<String, Long> parentMarkerVersions) {
    return createAttachedWithMetaAndCompanions(
        overlay, requiredPointerVersions, requiredAbsentPointers, parentMarkerVersions, null);
  }

  /**
   * Creates an overlay while both referenced parents are at the observed versions, publishing the
   * Integration and Catalog dependency pointers and advancing both dependency markers atomically.
   * The optional companion is an idempotency receipt, so it cannot lag the visible resource.
   */
  public Optional<ResourceWithMeta<CatalogOverlay>> createAttachedWithMetaAndCompanions(
      CatalogOverlay overlay,
      Map<String, Long> requiredPointerVersions,
      Set<String> requiredAbsentPointers,
      Map<String, Long> parentMarkerVersions,
      Function<ResourceWithMeta<CatalogOverlay>, List<PointerStore.CasOp>> companions) {
    return repo.createWithMeta(
        overlay,
        new PointerConditions(
            requiredPointerVersions, requiredAbsentPointers, parentMarkerVersions),
        companions);
  }

  public boolean beginDeletion(ResourceId overlayId, long expectedPointerVersion) {
    if (expectedPointerVersion <= 0L) return false;
    String canonical = Keys.catalogOverlayPointerById(overlayId.getAccountId(), overlayId.getId());
    String fence = Keys.catalogOverlayDeletionMarker(overlayId.getAccountId(), overlayId.getId());
    if (pointerStore.get(fence).isPresent()) {
      return pointerStore
          .get(canonical)
          .map(pointer -> pointer.getVersion() == expectedPointerVersion)
          .orElse(false);
    }
    return pointerStore.compareAndSetBatch(
        List.of(
            new PointerStore.CasCheck(canonical, expectedPointerVersion),
            AccountDeletionFence.checkForAccountWrite(overlayId.getAccountId(), fence),
            new PointerStore.CasUpsert(
                fence, 0L, PointerReferences.opaqueMarkerPointer(fence, fence, 1L))));
  }

  public long deletionFenceVersion(ResourceId overlayId) {
    return pointerStore
        .get(Keys.catalogOverlayDeletionMarker(overlayId.getAccountId(), overlayId.getId()))
        .map(ai.floedb.floecat.common.rpc.Pointer::getVersion)
        .orElse(0L);
  }

  /** Removes the Overlay, both dependency pointers, and its deletion fence atomically. */
  public boolean deleteWithFence(
      ResourceId overlayId, long expectedPointerVersion, long expectedFenceVersion) {
    String fence = Keys.catalogOverlayDeletionMarker(overlayId.getAccountId(), overlayId.getId());
    return repo.deleteWithPreconditionWhilePointersMatchAndDeletePointers(
        key(overlayId),
        expectedPointerVersion,
        PointerConditions.none(),
        Map.of(fence, expectedFenceVersion));
  }

  public boolean update(CatalogOverlay overlay, long expectedPointerVersion) {
    return repo.update(overlay, expectedPointerVersion);
  }

  public Optional<MutationMeta> updateWithMetaUnlessIntegrationDeleting(
      CatalogOverlay overlay, long expectedPointerVersion) {
    String integrationFence =
        Keys.catalogIntegrationDeletionMarker(
            overlay.getResourceId().getAccountId(), overlay.getIntegrationId().getId());
    String overlayFence =
        Keys.catalogOverlayDeletionMarker(
            overlay.getResourceId().getAccountId(), overlay.getResourceId().getId());
    return repo.updateWithMetaWhilePointersMatchAndBumpMarkers(
        overlay,
        expectedPointerVersion,
        new PointerConditions(Map.of(), Set.of(integrationFence, overlayFence), Map.of()));
  }

  public Optional<ResourceWithMeta<CatalogOverlay>> replaceIdentityAttachedWithMeta(
      CatalogOverlay current,
      long expectedPointerVersion,
      CatalogOverlay replacement,
      Map<String, Long> requiredPointerVersions,
      Set<String> requiredAbsentPointers,
      Map<String, Long> parentMarkerVersions) {
    String deletionFence =
        Keys.catalogOverlayDeletionMarker(
            current.getResourceId().getAccountId(), current.getResourceId().getId());
    long deletionFenceVersion = deletionFenceVersion(current.getResourceId());
    return repo.replaceIdentityWithMeta(
        current,
        expectedPointerVersion,
        replacement,
        new PointerConditions(
            requiredPointerVersions, requiredAbsentPointers, parentMarkerVersions),
        deletionFenceVersion == 0L ? Map.of() : Map.of(deletionFence, deletionFenceVersion));
  }

  public boolean deleteWithPrecondition(ResourceId overlayId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(key(overlayId), expectedPointerVersion);
  }

  public boolean delete(ResourceId overlayId) {
    return repo.delete(key(overlayId));
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

  public List<ResourceWithMeta<CatalogOverlay>> listByCatalogWithMetaConsistent(
      String accountId, String catalogId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefixWithMetaConsistent(
        Keys.catalogOverlayPointerByCatalogPrefix(accountId, catalogId), limit, pageToken, nextOut);
  }

  public int count(String accountId) {
    return repo.countByPrefix(Keys.catalogOverlayPointerByNamePrefix(accountId));
  }

  public int countByIntegration(String accountId, String integrationId) {
    return repo.countByPrefixConsistent(
        Keys.catalogOverlayPointerByIntegrationPrefix(accountId, integrationId));
  }

  public int countByCatalog(String accountId, String catalogId) {
    return repo.countByPrefixConsistent(
        Keys.catalogOverlayPointerByCatalogPrefix(accountId, catalogId));
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
