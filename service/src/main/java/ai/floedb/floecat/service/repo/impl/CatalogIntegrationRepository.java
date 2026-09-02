/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.service.repo.cache.AuthoritativePointerStore;
import ai.floedb.floecat.service.repo.model.CatalogIntegrationKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.AccountDeletionFence;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.service.repo.util.RepositoryReads;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.CachedPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@ApplicationScoped
public class CatalogIntegrationRepository {

  private final GenericResourceRepository<CatalogIntegration, CatalogIntegrationKey> repo;
  private final PointerStore pointerStore;

  public CatalogIntegrationRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(pointerStore, pointerStore, blobStore);
  }

  @Inject
  public CatalogIntegrationRepository(
      PointerStore pointerStore,
      @CachedPointerStore PointerStore pointerReads,
      BlobStore blobStore) {
    this.pointerStore = AuthoritativePointerStore.of(pointerStore);
    repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.CATALOG_INTEGRATION,
            CatalogIntegration::parseFrom,
            CatalogIntegration::toByteArray,
            "application/x-protobuf",
            null,
            RepositoryReads.direct(pointerReads, blobStore));
  }

  public void create(CatalogIntegration integration) {
    repo.create(integration);
  }

  public ResourceWithMeta<CatalogIntegration> createWithMeta(CatalogIntegration integration) {
    return repo.createWithMeta(integration);
  }

  public ResourceWithMeta<CatalogIntegration> createWithMetaAndCompletion(
      CatalogIntegration integration,
      Function<ResourceWithMeta<CatalogIntegration>, PointerStore.CasUpsert> completionFactory) {
    return repo.createWithMeta(
        integration, committed -> List.of(completionFactory.apply(committed)));
  }

  public boolean update(CatalogIntegration integration, long expectedPointerVersion) {
    return repo.update(integration, expectedPointerVersion);
  }

  public Optional<MutationMeta> updateWithMetaUnlessDeleting(
      CatalogIntegration integration, long expectedPointerVersion) {
    String fence =
        Keys.catalogIntegrationDeletionMarker(
            integration.getResourceId().getAccountId(), integration.getResourceId().getId());
    return repo.updateWithMetaWhilePointersMatchAndBumpMarkers(
        integration,
        expectedPointerVersion,
        new PointerConditions(Map.of(), Set.of(fence), Map.of()));
  }

  public Optional<ResourceWithMeta<CatalogIntegration>> replaceIdentityWithMeta(
      CatalogIntegration current,
      long expectedPointerVersion,
      CatalogIntegration replacement,
      long expectedOverlayMarkerVersion) {
    String accountId = current.getResourceId().getAccountId();
    String oldMarker =
        Keys.catalogIntegrationOverlaysMarker(accountId, current.getResourceId().getId());
    String newMarker =
        Keys.catalogIntegrationOverlaysMarker(accountId, replacement.getResourceId().getId());
    String oldDeletionFence =
        Keys.catalogIntegrationDeletionMarker(accountId, current.getResourceId().getId());
    String newDeletionFence =
        Keys.catalogIntegrationDeletionMarker(accountId, replacement.getResourceId().getId());
    Set<String> requiredAbsent = new java.util.HashSet<>();
    requiredAbsent.add(newMarker);
    requiredAbsent.add(oldDeletionFence);
    requiredAbsent.add(newDeletionFence);
    if (expectedOverlayMarkerVersion == 0L) requiredAbsent.add(oldMarker);
    return repo.replaceIdentityWithMeta(
        current,
        expectedPointerVersion,
        replacement,
        new PointerConditions(Map.of(), Set.copyOf(requiredAbsent), Map.of()),
        expectedOverlayMarkerVersion == 0L
            ? Map.of()
            : Map.of(oldMarker, expectedOverlayMarkerVersion));
  }

  public boolean deleteWithPreconditionAndOverlayMarker(
      ResourceId integrationId, long expectedPointerVersion, long expectedMarkerVersion) {
    String marker =
        Keys.catalogIntegrationOverlaysMarker(integrationId.getAccountId(), integrationId.getId());
    String fence =
        Keys.catalogIntegrationDeletionMarker(integrationId.getAccountId(), integrationId.getId());
    return repo.deleteWithPreconditionWhilePointersMatchAndDeletePointers(
        key(integrationId),
        expectedPointerVersion,
        new PointerConditions(
            Map.of(),
            expectedMarkerVersion == 0L ? Set.of(marker, fence) : Set.of(fence),
            Map.of()),
        expectedMarkerVersion == 0L ? Map.of() : Map.of(marker, expectedMarkerVersion));
  }

  public boolean deleteWithPreconditionForAccountDeletion(
      ResourceId integrationId,
      long expectedPointerVersion,
      long expectedMarkerVersion,
      long expectedAccountDeletionFenceVersion) {
    if (expectedAccountDeletionFenceVersion <= 0L) return false;
    String marker =
        Keys.catalogIntegrationOverlaysMarker(integrationId.getAccountId(), integrationId.getId());
    String fence =
        Keys.catalogIntegrationDeletionMarker(integrationId.getAccountId(), integrationId.getId());
    String accountFence = Keys.accountDeletionMarker(integrationId.getAccountId());
    return repo.deleteWithPreconditionWhilePointersMatchAndDeletePointers(
        key(integrationId),
        expectedPointerVersion,
        new PointerConditions(
            Map.of(accountFence, expectedAccountDeletionFenceVersion),
            expectedMarkerVersion == 0L ? Set.of(marker, fence) : Set.of(fence),
            Map.of()),
        expectedMarkerVersion == 0L ? Map.of() : Map.of(marker, expectedMarkerVersion));
  }

  public boolean beginCascadeDeletion(ResourceId integrationId, long expectedPointerVersion) {
    if (expectedPointerVersion <= 0L) return false;
    String canonical =
        Keys.catalogIntegrationPointerById(integrationId.getAccountId(), integrationId.getId());
    String fence =
        Keys.catalogIntegrationDeletionMarker(integrationId.getAccountId(), integrationId.getId());
    if (pointerStore.get(fence).isPresent()) {
      return pointerStore
          .get(canonical)
          .map(pointer -> pointer.getVersion() == expectedPointerVersion)
          .orElse(false);
    }
    return pointerStore.compareAndSetBatch(
        List.of(
            new PointerStore.CasCheck(canonical, expectedPointerVersion),
            AccountDeletionFence.checkForAccountWrite(integrationId.getAccountId(), fence),
            new PointerStore.CasUpsert(
                fence, 0L, PointerReferences.opaqueMarkerPointer(fence, fence, 1L))));
  }

  public long cascadeDeletionFenceVersion(ResourceId integrationId) {
    return pointerStore
        .get(
            Keys.catalogIntegrationDeletionMarker(
                integrationId.getAccountId(), integrationId.getId()))
        .map(ai.floedb.floecat.common.rpc.Pointer::getVersion)
        .orElse(0L);
  }

  public boolean deleteWithPreconditionForCascadeDeletion(
      ResourceId integrationId,
      long expectedPointerVersion,
      long expectedMarkerVersion,
      long expectedFenceVersion) {
    String marker =
        Keys.catalogIntegrationOverlaysMarker(integrationId.getAccountId(), integrationId.getId());
    String fence =
        Keys.catalogIntegrationDeletionMarker(integrationId.getAccountId(), integrationId.getId());
    Map<String, Long> deletes = new java.util.HashMap<>();
    if (expectedMarkerVersion > 0L) deletes.put(marker, expectedMarkerVersion);
    deletes.put(fence, expectedFenceVersion);
    return repo.deleteWithPreconditionWhilePointersMatchAndDeletePointers(
        key(integrationId),
        expectedPointerVersion,
        new PointerConditions(
            Map.of(), expectedMarkerVersion == 0L ? Set.of(marker) : Set.of(), Map.of()),
        Map.copyOf(deletes));
  }

  public Optional<CatalogIntegration> getById(ResourceId integrationId) {
    return repo.getByKey(key(integrationId));
  }

  public Optional<CatalogIntegration> getByIdForMutation(ResourceId integrationId) {
    return repo.getByKeyForMutation(key(integrationId));
  }

  public Optional<ResourceWithMeta<CatalogIntegration>> getByIdWithMeta(ResourceId integrationId) {
    return repo.getByKeyWithMeta(key(integrationId));
  }

  public boolean existsById(ResourceId integrationId) {
    return repo.existsByKey(key(integrationId));
  }

  public Optional<CatalogIntegration> getByName(String accountId, String displayName) {
    return repo.get(Keys.catalogIntegrationPointerByName(accountId, displayName));
  }

  public Optional<ResourceWithMeta<CatalogIntegration>> getByNameWithMeta(
      String accountId, String displayName) {
    return repo.getWithMeta(Keys.catalogIntegrationPointerByName(accountId, displayName));
  }

  public List<CatalogIntegration> list(
      String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(
        Keys.catalogIntegrationPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public List<CatalogIntegration> listConsistent(
      String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefixForMutation(
        Keys.catalogIntegrationPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public List<ResourceWithMeta<CatalogIntegration>> listWithMeta(
      String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefixWithMeta(
        Keys.catalogIntegrationPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public int count(String accountId) {
    return repo.countByPrefix(Keys.catalogIntegrationPointerByNamePrefix(accountId));
  }

  public MutationMeta metaFor(ResourceId integrationId) {
    return repo.metaFor(key(integrationId));
  }

  public MutationMeta metaForSafe(ResourceId integrationId) {
    return repo.metaForSafe(key(integrationId));
  }

  private static CatalogIntegrationKey key(ResourceId id) {
    return new CatalogIntegrationKey(id.getAccountId(), id.getId());
  }
}
