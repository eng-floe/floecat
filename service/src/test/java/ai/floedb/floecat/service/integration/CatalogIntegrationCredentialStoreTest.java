/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.BearerAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.integration.rpc.SecretValue;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CatalogIntegrationCredentialStoreTest {
  @Test
  void existingGenerationIsImmutable() {
    var secretsManager = mock(SecretsManager.class);
    var store = new CatalogIntegrationCredentialStore();
    store.secretsManager = secretsManager;
    var integrationId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("integration")
            .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
            .build();
    var original =
        CatalogIntegrationCredentials.newBuilder()
            .setBearerToken(SecretValue.newBuilder().setValue("original"))
            .build();
    when(secretsManager.get(
            "acct",
            CatalogIntegrationCredentialStore.SECRET_TYPE,
            CatalogIntegrationCredentialStore.reference(integrationId, 1L)))
        .thenReturn(Optional.of(original.toByteArray()));

    store.store(
        integrationId,
        1L,
        CatalogIntegrationCredentials.newBuilder()
            .setBearerToken(SecretValue.newBuilder().setValue("replacement"))
            .build());

    verify(secretsManager, never()).putIfAbsent(any(), any(), any(), any());
    verify(secretsManager, never()).update(any(), any(), any(), any());
  }

  @Test
  void rotationSkipsOccupiedGenerationEvenForSameCredentials() {
    var secretsManager = mock(SecretsManager.class);
    var store = new CatalogIntegrationCredentialStore();
    store.secretsManager = secretsManager;
    var integrationId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("integration")
            .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
            .build();
    var original =
        CatalogIntegrationCredentials.newBuilder()
            .setBearerToken(SecretValue.newBuilder().setValue("original"))
            .build();
    when(secretsManager.get(
            "acct",
            CatalogIntegrationCredentialStore.SECRET_TYPE,
            CatalogIntegrationCredentialStore.reference(integrationId, 2L)))
        .thenReturn(Optional.of(original.toByteArray()));
    when(secretsManager.get(
            "acct",
            CatalogIntegrationCredentialStore.SECRET_TYPE,
            CatalogIntegrationCredentialStore.reference(integrationId, 3L)))
        .thenReturn(Optional.empty());
    when(secretsManager.putIfAbsent(any(), any(), any(), any())).thenReturn(true);

    assertEquals(3L, store.storeRotation(integrationId, 2L, original));
    verify(secretsManager)
        .putIfAbsent(
            eq("acct"),
            eq(CatalogIntegrationCredentialStore.SECRET_TYPE),
            eq(CatalogIntegrationCredentialStore.reference(integrationId, 3L)),
            eq(original.toByteArray()));
  }

  @Test
  void rotationAdvancesWhenReservationReportsGenerationOccupied() {
    var secretsManager = mock(SecretsManager.class);
    var store = new CatalogIntegrationCredentialStore();
    store.secretsManager = secretsManager;
    var integrationId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("integration")
            .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
            .build();
    var credentials =
        CatalogIntegrationCredentials.newBuilder()
            .setBearerToken(SecretValue.newBuilder().setValue("replacement"))
            .build();
    when(secretsManager.get(any(), any(), any())).thenReturn(Optional.empty());
    when(secretsManager.putIfAbsent(any(), any(), any(), any())).thenReturn(false, true);

    assertEquals(3L, store.storeRotation(integrationId, 2L, credentials));
    verify(secretsManager)
        .putIfAbsent(
            "acct",
            CatalogIntegrationCredentialStore.SECRET_TYPE,
            CatalogIntegrationCredentialStore.reference(integrationId, 3L),
            credentials.toByteArray());
  }

  @Test
  void resolvesTypedCredentialsFromResourceState() {
    var secretsManager = mock(SecretsManager.class);
    var store = new CatalogIntegrationCredentialStore();
    store.secretsManager = secretsManager;
    var integrationId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("integration")
            .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
            .build();
    var credentials =
        CatalogIntegrationCredentials.newBuilder()
            .setBearerToken(SecretValue.newBuilder().setValue("token"))
            .build();
    var integration =
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setAuthentication(
                CatalogAuthentication.newBuilder()
                    .setBearer(BearerAuthentication.getDefaultInstance())
                    .setCredentialsConfigured(true)
                    .setCredentialGeneration(3L))
            .build();
    when(secretsManager.get(
            "acct",
            CatalogIntegrationCredentialStore.SECRET_TYPE,
            CatalogIntegrationCredentialStore.reference(integrationId, 3L)))
        .thenReturn(Optional.of(credentials.toByteArray()));

    assertEquals(credentials, store.resolve(integration).orElseThrow());
  }
}
