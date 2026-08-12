/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.Timestamp;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CatalogIntegrationRepositoryTest {

  @Test
  void roundTripsAndRefreshesNameIndex() {
    var repo =
        new CatalogIntegrationRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var id =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("integration")
            .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
            .build();
    var integration =
        CatalogIntegration.newBuilder()
            .setResourceId(id)
            .setDisplayName("warehouse")
            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
            .setCatalogUri("https://catalog.example")
            .build();

    repo.create(integration);
    assertEquals(integration, repo.getById(id).orElseThrow());
    assertEquals(id, repo.getByName("account", "warehouse").orElseThrow().getResourceId());
    assertEquals(1, repo.count("account"));

    var meta = repo.metaFor(id);
    assertTrue(
        repo.update(
            integration.toBuilder().setDisplayName("renamed-warehouse").build(),
            meta.getPointerVersion()));
    assertTrue(repo.getByName("account", "warehouse").isEmpty());
    var renamed = repo.getByName("account", "renamed-warehouse").orElseThrow();
    assertEquals(CatalogIntegrationType.CIT_ICEBERG_REST, renamed.getType());
    assertEquals("https://catalog.example", renamed.getCatalogUri());
  }

  @Test
  void deleteWithoutOverlayMarkerRequiresItToRemainAbsent() {
    var pointers = new InMemoryPointerStore();
    var repo = new CatalogIntegrationRepository(pointers, new InMemoryBlobStore());
    var id =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("integration")
            .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
            .build();
    repo.create(
        CatalogIntegration.newBuilder().setResourceId(id).setDisplayName("warehouse").build());
    long version = repo.metaFor(id).getPointerVersion();

    assertTrue(repo.deleteWithPreconditionAndOverlayMarker(id, version, 0L));
    assertFalse(repo.getById(id).isPresent());
    assertFalse(
        pointers.get(Keys.catalogIntegrationOverlaysMarker("account", "integration")).isPresent());
  }

  @Test
  void nonCascadeDeleteRequiresCascadeFenceToRemainAbsent() {
    var pointers = new InMemoryPointerStore();
    var repo = new CatalogIntegrationRepository(pointers, new InMemoryBlobStore());
    var id = id("integration");
    repo.create(
        CatalogIntegration.newBuilder().setResourceId(id).setDisplayName("warehouse").build());
    long version = repo.metaFor(id).getPointerVersion();

    assertTrue(repo.beginCascadeDeletion(id, version));
    assertFalse(repo.deleteWithPreconditionAndOverlayMarker(id, version, 0L));
    assertTrue(repo.getById(id).isPresent());
    assertTrue(
        pointers.get(Keys.catalogIntegrationDeletionMarker("account", "integration")).isPresent());
  }

  @Test
  void accountDeletionFenceRejectsNewResources() {
    var pointers = new InMemoryPointerStore();
    var repo = new CatalogIntegrationRepository(pointers, new InMemoryBlobStore());
    String marker = Keys.accountDeletionMarker("account");
    assertTrue(
        pointers.compareAndSet(
            marker, 0L, PointerReferences.opaqueMarkerPointer(marker, "deleting", 1L)));
    var integration =
        CatalogIntegration.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("account")
                    .setId("integration")
                    .setKind(ResourceKind.RK_CATALOG_INTEGRATION))
            .setDisplayName("warehouse")
            .build();

    assertThrows(BaseResourceRepository.NotFoundException.class, () -> repo.create(integration));
    assertTrue(repo.getById(integration.getResourceId()).isEmpty());
  }

  @Test
  void nameReadDoesNotReturnResourceRenamedAfterSecondarySelection() {
    var repoRef = new AtomicReference<CatalogIntegrationRepository>();
    var renamed = new AtomicBoolean();
    var pointers =
        new InMemoryPointerStore() {
          @Override
          public Optional<Pointer> get(String key) {
            Optional<Pointer> selected = super.get(key);
            if (key.equals(Keys.catalogIntegrationPointerByName("account", "warehouse"))
                && renamed.compareAndSet(false, true)) {
              var current = repoRef.get().getByIdWithMeta(id("integration")).orElseThrow();
              assertTrue(
                  repoRef
                      .get()
                      .update(
                          current.value().toBuilder().setDisplayName("renamed").build(),
                          current.meta().getPointerVersion()));
            }
            return selected;
          }
        };
    var repo = new CatalogIntegrationRepository(pointers, new InMemoryBlobStore());
    repoRef.set(repo);
    var integration =
        CatalogIntegration.newBuilder()
            .setResourceId(id("integration"))
            .setDisplayName("warehouse")
            .build();
    repo.create(integration);

    assertTrue(repo.getByNameWithMeta("account", "warehouse").isEmpty());
    assertEquals("renamed", repo.getByName("account", "renamed").orElseThrow().getDisplayName());
  }

  @Test
  void replacementAtomicallySwapsToNewIdentity() {
    var pointers = new InMemoryPointerStore();
    var repo = new CatalogIntegrationRepository(pointers, new InMemoryBlobStore());
    var original =
        CatalogIntegration.newBuilder()
            .setResourceId(id("old"))
            .setDisplayName("warehouse")
            .setCatalogUri("https://old.example")
            .build();
    repo.create(original);
    var current = repo.getByIdWithMeta(original.getResourceId()).orElseThrow();
    var replacement =
        original.toBuilder().setResourceId(id("new")).setCatalogUri("https://new.example").build();

    var replaced =
        repo.replaceIdentityWithMeta(
                current.value(), current.meta().getPointerVersion(), replacement, 0L)
            .orElseThrow();

    assertTrue(repo.getById(original.getResourceId()).isEmpty());
    assertEquals(replacement, replaced.value());
    assertEquals(
        replacement.getResourceId(),
        repo.getByName("account", "warehouse").orElseThrow().getResourceId());
  }

  @Test
  void idempotencyReceiptAndIntegrationPointersBothCommitWhenAcknowledgementIsLost()
      throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var commitThenFail = new RepoTestPointerStores.CommitThenFailBatchPointerStore(pointers);
    var repo = new CatalogIntegrationRepository(commitThenFail, blobs);
    var idempotency = new IdempotencyRepositoryImpl(commitThenFail, blobs);
    var integration =
        CatalogIntegration.newBuilder()
            .setResourceId(id("integration"))
            .setDisplayName("warehouse")
            .build();
    String operation = "CreateCatalogIntegration";
    String key = Keys.idempotencyKey("account", operation, "request-1");
    Timestamp createdAt = timestamp(1L);
    Timestamp expiresAt = timestamp(2L);
    var committedMeta = new AtomicReference<ai.floedb.floecat.common.rpc.MutationMeta>();
    assertTrue(
        idempotency.createPending(
            "account",
            key,
            operation,
            "request-hash",
            integration.getResourceId(),
            createdAt,
            expiresAt));

    assertThrows(
        RepoTestPointerStores.CommitThenFailBatchPointerStore.InjectedAcknowledgementFailure.class,
        () ->
            repo.createWithMetaAndCompletion(
                integration,
                row -> {
                  committedMeta.set(row.meta());
                  return idempotency.prepareSuccess(
                      "account",
                      key,
                      operation,
                      "request-hash",
                      row.value().getResourceId(),
                      row.meta(),
                      row.value().toByteArray(),
                      createdAt,
                      expiresAt);
                }));

    var receipt = idempotency.get(key).orElseThrow();
    assertEquals(IdempotencyRecord.Status.SUCCEEDED, receipt.getStatus());
    assertEquals(committedMeta.get(), receipt.getMeta());
    assertEquals(integration, CatalogIntegration.parseFrom(receipt.getPayload()));
    assertEquals(integration, repo.getById(integration.getResourceId()).orElseThrow());
    assertEquals(integration, repo.getByName("account", "warehouse").orElseThrow());
  }

  @Test
  void idempotencyReceiptAndIntegrationPointersBothRemainUncommittedWhenBatchFails() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var failing = new RepoTestPointerStores.FailingBatchPointerStore(pointers);
    var repo = new CatalogIntegrationRepository(failing, blobs);
    var idempotency = new IdempotencyRepositoryImpl(failing, blobs);
    var integration =
        CatalogIntegration.newBuilder()
            .setResourceId(id("integration"))
            .setDisplayName("warehouse")
            .build();
    String operation = "CreateCatalogIntegration";
    String key = Keys.idempotencyKey("account", operation, "request-1");
    Timestamp createdAt = timestamp(1L);
    Timestamp expiresAt = timestamp(2L);
    assertTrue(
        idempotency.createPending(
            "account",
            key,
            operation,
            "request-hash",
            integration.getResourceId(),
            createdAt,
            expiresAt));

    assertThrows(
        RepoTestPointerStores.FailingBatchPointerStore.InjectedBatchFailure.class,
        () ->
            repo.createWithMetaAndCompletion(
                integration,
                row ->
                    idempotency.prepareSuccess(
                        "account",
                        key,
                        operation,
                        "request-hash",
                        row.value().getResourceId(),
                        row.meta(),
                        row.value().toByteArray(),
                        createdAt,
                        expiresAt)));

    assertTrue(repo.getById(integration.getResourceId()).isEmpty());
    assertTrue(repo.getByName("account", "warehouse").isEmpty());
    assertEquals(IdempotencyRecord.Status.PENDING, idempotency.get(key).orElseThrow().getStatus());
  }

  private static Timestamp timestamp(long seconds) {
    return Timestamp.newBuilder().setSeconds(seconds).build();
  }

  private static ResourceId id(String value) {
    return ResourceId.newBuilder()
        .setAccountId("account")
        .setId(value)
        .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
        .build();
  }
}
