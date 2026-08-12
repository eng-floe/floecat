/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.BearerAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.integration.rpc.SecretValue;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogIntegrationCredentialCleanupTest {
  private InMemoryPointerStore pointers;
  private CatalogIntegrationRepository integrations;
  private CatalogIntegrationCredentialStore credentials;
  private CatalogIntegrationCredentialCleanup cleanup;

  @BeforeEach
  void setUp() {
    pointers = new InMemoryPointerStore();
    integrations = mock(CatalogIntegrationRepository.class);
    credentials = mock(CatalogIntegrationCredentialStore.class);
    cleanup = new CatalogIntegrationCredentialCleanup();
    cleanup.pointerStore = pointers;
    cleanup.integrations = integrations;
    cleanup.credentials = credentials;
  }

  @Test
  void retainsLiveGenerationAndRetriesFailedDeletionAfterResourceDisappears() {
    ResourceId id = id("integration");
    CatalogIntegration integration = storedIntegration(id, 7L);
    String marker = Keys.catalogIntegrationCredentialCleanupPointer("acct", "integration", 7L);
    when(integrations.getById(id))
        .thenReturn(Optional.of(integration), Optional.empty(), Optional.empty());
    doThrow(new IllegalStateException("secrets unavailable"))
        .doNothing()
        .when(credentials)
        .deleteImmediately(id, 7L);

    cleanup.schedule(integration);
    cleanup.drain(System.currentTimeMillis() + 1_000L, 10);
    assertTrue(pointers.get(marker).isPresent());

    cleanup.drain(System.currentTimeMillis() + 1_000L, 10);
    assertTrue(pointers.get(marker).isPresent());

    CatalogIntegrationCredentialCleanup.Result result =
        cleanup.drain(System.currentTimeMillis() + 1_000L, 10);
    assertEquals(1, result.deleted());
    assertTrue(pointers.get(marker).isEmpty());
    verify(credentials, org.mockito.Mockito.times(2)).deleteImmediately(id, 7L);
  }

  @Test
  void preparedCredentialCleanupIsDurable() {
    ResourceId id = id("prepared");
    when(integrations.getById(id)).thenReturn(Optional.empty());
    CatalogIntegrationCredentials prepared =
        CatalogIntegrationCredentials.newBuilder()
            .setBearerToken(SecretValue.newBuilder().setValue("token"))
            .build();

    cleanup.cleanPrepared(id, 3L, prepared);

    verify(credentials).deleteImmediately(id, 3L);
    assertTrue(
        pointers
            .get(Keys.catalogIntegrationCredentialCleanupPointer("acct", "prepared", 3L))
            .isEmpty());
  }

  private static ResourceId id(String value) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setId(value)
        .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
        .build();
  }

  private static CatalogIntegration storedIntegration(ResourceId id, long generation) {
    return CatalogIntegration.newBuilder()
        .setResourceId(id)
        .setAuthentication(
            CatalogAuthentication.newBuilder()
                .setBearer(BearerAuthentication.getDefaultInstance())
                .setCredentialsConfigured(true)
                .setCredentialGeneration(generation))
        .build();
  }
}
