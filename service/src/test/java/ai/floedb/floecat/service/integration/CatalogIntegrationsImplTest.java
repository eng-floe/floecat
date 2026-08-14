/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.CreateMode;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.AwsAssumeRoleAuthentication;
import ai.floedb.floecat.integration.rpc.AwsSigV4Authentication;
import ai.floedb.floecat.integration.rpc.BearerAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationSpec;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.integration.rpc.CreateCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.DeleteCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.GetCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.OAuthClientCredentialsAuthentication;
import ai.floedb.floecat.integration.rpc.SecretValue;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationAuthenticationRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationRequest;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CatalogIntegrationsImplTest {
  private CatalogIntegrationsImpl service;
  private SecretsManager secretsManager;

  @BeforeEach
  void setUp() {
    service = new CatalogIntegrationsImpl();
    service.integrations = mock(CatalogIntegrationRepository.class);
    service.overlays = mock(CatalogOverlayRepository.class);
    service.markerStore = mock(MarkerStore.class);
    service.principal = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.idempotencyStore = mock(IdempotencyRepository.class);
    secretsManager = mock(SecretsManager.class);
    service.credentialStore = new CatalogIntegrationCredentialStore();
    service.credentialStore.secretsManager = secretsManager;
    service.credentialCleanup = new CatalogIntegrationCredentialCleanup();
    service.credentialCleanup.pointerStore = new InMemoryPointerStore();
    service.credentialCleanup.integrations = service.integrations;
    service.credentialCleanup.credentials = service.credentialStore;
    service.discovery = mock(CatalogIntegrationDiscovery.class);
    when(secretsManager.putIfAbsent(any(), any(), any(), any())).thenReturn(true);
    installBasePrincipal(service, service.principal);
    when(service.principal.get()).thenReturn(principal());
    when(service.integrations.createWithMeta(any()))
        .thenAnswer(
            invocation ->
                new ResourceWithMeta<>(
                    invocation.getArgument(0),
                    MutationMeta.newBuilder().setPointerVersion(1L).build()));
  }

  @Test
  void createPreservesSqlNameAndPersistsStrictHttpEndpoint() {
    var response =
        service
            .createCatalogIntegration(
                CreateCatalogIntegrationRequest.newBuilder()
                    .setSpec(
                        CatalogIntegrationSpec.newBuilder()
                            .setDisplayName("Warehouse")
                            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                            .setCatalogUri("https://catalog.example/v1")
                            .putProperties("warehouse", "analytics")
                            .putProperties("s3.region", "us-east-1")
                            .setAuthentication(oauthAuthentication()))
                    .setCredentials(oauthCredentials())
                    .build())
            .await()
            .indefinitely();

    var persisted = ArgumentCaptor.forClass(CatalogIntegration.class);
    verify(service.integrations).createWithMeta(persisted.capture());
    assertEquals("Warehouse", persisted.getValue().getDisplayName());
    assertEquals("https://catalog.example/v1", response.getIntegration().getCatalogUri());
    assertEquals("analytics", response.getIntegration().getPropertiesMap().get("warehouse"));
    assertEquals("us-east-1", response.getIntegration().getPropertiesMap().get("s3.region"));
  }

  @Test
  void createRejectsSecretBearingConnectionProperties() {
    for (String key : List.of("token", "s3.secret-access-key", "header.Authorization")) {
      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  service
                      .createCatalogIntegration(
                          CreateCatalogIntegrationRequest.newBuilder()
                              .setSpec(
                                  CatalogIntegrationSpec.newBuilder()
                                      .setDisplayName("Warehouse")
                                      .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                                      .setCatalogUri("https://catalog.example/v1")
                                      .putProperties(key, "redacted"))
                              .build())
                      .await()
                      .indefinitely(),
              key);

      assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode(), key);
    }
    verify(service.integrations, never()).createWithMeta(any());
  }

  @Test
  void createRejectsMalformedAndSecretBearingCatalogUris() {
    for (String uri :
        List.of(
            "catalog.example",
            "https://catalog.example/%ZZ",
            "https://user:password@catalog.example",
            "https://catalog.example?access_token=redacted",
            "https://catalog.example?client%5Fsecret=redacted",
            "https://catalog.example?X-Amz-Signature=redacted",
            "https://catalog.example?sig=redacted",
            "https://catalog.example#access_token=redacted",
            "https://catalog.example#client_secret=redacted",
            "https://catalog.example#")) {
      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  service
                      .createCatalogIntegration(
                          CreateCatalogIntegrationRequest.newBuilder()
                              .setSpec(
                                  CatalogIntegrationSpec.newBuilder()
                                      .setDisplayName("Warehouse")
                                      .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                                      .setCatalogUri(uri))
                              .build())
                      .await()
                      .indefinitely(),
              uri);

      assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode(), uri);
    }
    verify(service.integrations, never()).createWithMeta(any());
  }

  @Test
  void createAllowsBenignQueryParameters() {
    var response =
        service
            .createCatalogIntegration(
                CreateCatalogIntegrationRequest.newBuilder()
                    .setSpec(
                        CatalogIntegrationSpec.newBuilder()
                            .setDisplayName("Warehouse")
                            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                            .setCatalogUri("https://catalog.example/v1?warehouse=analytics")
                            .setAuthentication(oauthAuthentication()))
                    .setCredentials(oauthCredentials())
                    .build())
            .await()
            .indefinitely();
    assertEquals(
        "https://catalog.example/v1?warehouse=analytics",
        response.getIntegration().getCatalogUri());
  }

  @Test
  void createStoresCredentialsSeparatelyUsingDeterministicReference() throws Exception {
    var response =
        service
            .createCatalogIntegration(
                CreateCatalogIntegrationRequest.newBuilder()
                    .setSpec(
                        CatalogIntegrationSpec.newBuilder()
                            .setDisplayName("Warehouse")
                            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                            .setCatalogUri("https://catalog.example")
                            .setAuthentication(
                                CatalogAuthentication.newBuilder()
                                    .setOauthClientCredentials(
                                        OAuthClientCredentialsAuthentication.newBuilder()
                                            .setClientId("client-id")
                                            .addScopes("catalog"))))
                    .setCredentials(
                        CatalogIntegrationCredentials.newBuilder()
                            .setOauthClientSecret(
                                SecretValue.newBuilder().setValue("client-secret")))
                    .build())
            .await()
            .indefinitely();

    var persisted = ArgumentCaptor.forClass(CatalogIntegration.class);
    verify(service.integrations).createWithMeta(persisted.capture());
    var storedAuthentication = persisted.getValue().getAuthentication();
    assertTrue(storedAuthentication.getCredentialsConfigured());
    assertEquals(1L, storedAuthentication.getCredentialGeneration());
    assertEquals(storedAuthentication, response.getIntegration().getAuthentication());

    var payload = ArgumentCaptor.forClass(byte[].class);
    verify(secretsManager)
        .putIfAbsent(
            eq("acct"),
            eq(CatalogIntegrationCredentialStore.SECRET_TYPE),
            eq(
                CatalogIntegrationCredentialStore.reference(
                    persisted.getValue().getResourceId(), 1L)),
            payload.capture());
    assertEquals(
        "client-secret",
        CatalogIntegrationCredentials.parseFrom(payload.getValue())
            .getOauthClientSecret()
            .getValue());
  }

  @Test
  void updateAuthenticationRotatesCredentialsAndAdvancesGeneration() {
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var current =
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setDisplayName("Warehouse")
            .setType(CatalogIntegrationType.CIT_UNITY)
            .setCatalogUri("https://catalog.example")
            .setAuthentication(
                CatalogAuthentication.newBuilder()
                    .setOauthClientCredentials(
                        OAuthClientCredentialsAuthentication.newBuilder().setClientId("client-id"))
                    .setCredentialsConfigured(true)
                    .setCredentialGeneration(1L))
            .build();
    when(service.integrations.getByIdWithMeta(integrationId))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    current, MutationMeta.newBuilder().setPointerVersion(7L).build())));
    when(service.integrations.updateWithMetaUnlessDeleting(any(), eq(7L)))
        .thenReturn(Optional.of(MutationMeta.newBuilder().setPointerVersion(8L).build()));

    var response =
        service
            .updateCatalogIntegrationAuthentication(
                UpdateCatalogIntegrationAuthenticationRequest.newBuilder()
                    .setIntegrationId(integrationId)
                    .setAuthentication(
                        CatalogAuthentication.newBuilder()
                            .setBearer(BearerAuthentication.getDefaultInstance()))
                    .setCredentials(
                        CatalogIntegrationCredentials.newBuilder()
                            .setBearerToken(SecretValue.newBuilder().setValue("token")))
                    .build())
            .await()
            .indefinitely();

    var desired = ArgumentCaptor.forClass(CatalogIntegration.class);
    verify(service.integrations).updateWithMetaUnlessDeleting(desired.capture(), eq(7L));
    assertEquals(2L, response.getIntegration().getAuthentication().getCredentialGeneration());
    assertEquals(desired.getValue(), response.getIntegration());
    verify(secretsManager)
        .putIfAbsent(
            eq("acct"),
            eq(CatalogIntegrationCredentialStore.SECRET_TYPE),
            eq(CatalogIntegrationCredentialStore.reference(integrationId, 2L)),
            any(byte[].class));
    verify(secretsManager)
        .deleteImmediately(
            eq("acct"),
            eq(CatalogIntegrationCredentialStore.SECRET_TYPE),
            eq(CatalogIntegrationCredentialStore.reference(integrationId, 1L)));
  }

  @Test
  void failedAuthenticationCasDeletesOnlyPreparedCredentialGeneration() {
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var current =
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setType(CatalogIntegrationType.CIT_UNITY)
            .setAuthentication(
                CatalogAuthentication.newBuilder()
                    .setBearer(BearerAuthentication.getDefaultInstance())
                    .setCredentialsConfigured(true)
                    .setCredentialGeneration(1L))
            .build();
    when(service.integrations.getByIdWithMeta(integrationId))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    current, MutationMeta.newBuilder().setPointerVersion(7L).build())));
    when(service.integrations.updateWithMetaUnlessDeleting(any(), eq(7L)))
        .thenReturn(Optional.empty());

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .updateCatalogIntegrationAuthentication(
                        UpdateCatalogIntegrationAuthenticationRequest.newBuilder()
                            .setIntegrationId(integrationId)
                            .setAuthentication(
                                CatalogAuthentication.newBuilder()
                                    .setBearer(BearerAuthentication.getDefaultInstance()))
                            .setCredentials(
                                CatalogIntegrationCredentials.newBuilder()
                                    .setBearerToken(SecretValue.newBuilder().setValue("new-token")))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    verify(secretsManager)
        .deleteImmediately(
            "acct",
            CatalogIntegrationCredentialStore.SECRET_TYPE,
            CatalogIntegrationCredentialStore.reference(integrationId, 2L));
    verify(secretsManager, never())
        .deleteImmediately(
            "acct",
            CatalogIntegrationCredentialStore.SECRET_TYPE,
            CatalogIntegrationCredentialStore.reference(integrationId, 1L));
  }

  @Test
  void losingAuthenticationCasRetainsGenerationPublishedByConcurrentWinner() {
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var current =
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setType(CatalogIntegrationType.CIT_UNITY)
            .setAuthentication(
                CatalogAuthentication.newBuilder()
                    .setBearer(BearerAuthentication.getDefaultInstance())
                    .setCredentialsConfigured(true)
                    .setCredentialGeneration(1L))
            .build();
    var winner =
        current.toBuilder()
            .setAuthentication(
                CatalogAuthentication.newBuilder()
                    .setBearer(BearerAuthentication.getDefaultInstance())
                    .setCredentialsConfigured(true)
                    .setCredentialGeneration(2L))
            .build();
    when(service.integrations.getByIdWithMeta(integrationId))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    current, MutationMeta.newBuilder().setPointerVersion(7L).build())));
    when(service.integrations.updateWithMetaUnlessDeleting(any(), eq(7L)))
        .thenReturn(Optional.empty());
    when(service.integrations.getByIdForMutation(integrationId)).thenReturn(Optional.of(winner));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            service
                .updateCatalogIntegrationAuthentication(
                    UpdateCatalogIntegrationAuthenticationRequest.newBuilder()
                        .setIntegrationId(integrationId)
                        .setAuthentication(
                            CatalogAuthentication.newBuilder()
                                .setBearer(BearerAuthentication.getDefaultInstance()))
                        .setCredentials(
                            CatalogIntegrationCredentials.newBuilder()
                                .setBearerToken(SecretValue.newBuilder().setValue("new-token")))
                        .build())
                .await()
                .indefinitely());

    verify(secretsManager, never())
        .deleteImmediately(
            "acct",
            CatalogIntegrationCredentialStore.SECRET_TYPE,
            CatalogIntegrationCredentialStore.reference(integrationId, 2L));
  }

  @Test
  void unityRejectsAwsAuthentication() {
    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogIntegration(
                        CreateCatalogIntegrationRequest.newBuilder()
                            .setSpec(
                                CatalogIntegrationSpec.newBuilder()
                                    .setDisplayName("Warehouse")
                                    .setType(CatalogIntegrationType.CIT_UNITY)
                                    .setCatalogUri("https://catalog.example")
                                    .setAuthentication(
                                        CatalogAuthentication.newBuilder()
                                            .setAwsAssumeRole(
                                                ai.floedb.floecat.integration.rpc
                                                    .AwsAssumeRoleAuthentication.newBuilder()
                                                    .setRoleArn(
                                                        "arn:aws:iam::123456789012:role/test"))))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(service.integrations, never()).createWithMeta(any());
  }

  @Test
  void createRejectsBlankTopLevelAssumeRoleExternalId() {
    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogIntegration(
                        createRequestWithAuthentication(
                            CatalogAuthentication.newBuilder()
                                .setAwsAssumeRole(
                                    AwsAssumeRoleAuthentication.newBuilder()
                                        .setRoleArn("arn:aws:iam::123456789012:role/test")
                                        .setExternalId(" "))
                                .build()))
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(service.integrations, never()).createWithMeta(any());
  }

  @Test
  void createRejectsBlankSigV4AssumeRoleExternalId() {
    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogIntegration(
                        createRequestWithAuthentication(
                            CatalogAuthentication.newBuilder()
                                .setAwsSigv4(
                                    AwsSigV4Authentication.newBuilder()
                                        .setRegion("us-east-1")
                                        .setAwsAssumeRole(
                                            AwsAssumeRoleAuthentication.newBuilder()
                                                .setRoleArn("arn:aws:iam::123456789012:role/test")
                                                .setExternalId(" ")))
                                .build()))
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(service.integrations, never()).createWithMeta(any());
  }

  @Test
  void createRejectsMissingRequiredCredentials() {
    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogIntegration(
                        CreateCatalogIntegrationRequest.newBuilder()
                            .setSpec(
                                CatalogIntegrationSpec.newBuilder()
                                    .setDisplayName("Warehouse")
                                    .setType(CatalogIntegrationType.CIT_UNITY)
                                    .setCatalogUri("https://catalog.example")
                                    .setAuthentication(
                                        CatalogAuthentication.newBuilder()
                                            .setBearer(BearerAuthentication.getDefaultInstance())))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(secretsManager, never()).putIfAbsent(any(), any(), any(), any());
  }

  @Test
  void definiteCreateConflictDeletesPreparedCredentials() {
    var existing =
        CatalogIntegration.newBuilder()
            .setResourceId(id("existing", ResourceKind.RK_CATALOG_INTEGRATION))
            .setDisplayName("Warehouse")
            .build();
    when(service.integrations.getByNameWithMeta("acct", "Warehouse"))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    existing, MutationMeta.newBuilder().setPointerVersion(1L).build())));

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.createCatalogIntegration(validCreateRequest()).await().indefinitely());

    assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
    var reference = ArgumentCaptor.forClass(String.class);
    verify(secretsManager)
        .putIfAbsent(
            eq("acct"),
            eq(CatalogIntegrationCredentialStore.SECRET_TYPE),
            reference.capture(),
            any(byte[].class));
    verify(secretsManager)
        .deleteImmediately(
            "acct", CatalogIntegrationCredentialStore.SECRET_TYPE, reference.getValue());
  }

  @Test
  void accountDeletionFenceFailureDeletesPreparedCredentials() {
    when(service.integrations.createWithMeta(any()))
        .thenThrow(new BaseResourceRepository.NotFoundException("account deletion is fenced"));

    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.createCatalogIntegration(validCreateRequest()).await().indefinitely());
    assertEquals(Status.Code.NOT_FOUND, failure.getStatus().getCode());

    var reference = ArgumentCaptor.forClass(String.class);
    verify(secretsManager)
        .putIfAbsent(
            eq("acct"),
            eq(CatalogIntegrationCredentialStore.SECRET_TYPE),
            reference.capture(),
            any(byte[].class));
    verify(secretsManager)
        .deleteImmediately(
            "acct", CatalogIntegrationCredentialStore.SECRET_TYPE, reference.getValue());
  }

  @Test
  void createRejectsUnknownConflictMode() {
    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogIntegration(
                        CreateCatalogIntegrationRequest.newBuilder()
                            .setCreateModeValue(99)
                            .setSpec(
                                CatalogIntegrationSpec.newBuilder()
                                    .setDisplayName("Warehouse")
                                    .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                                    .setCatalogUri("https://catalog.example"))
                            .build())
                    .await()
                    .indefinitely());
    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
  }

  @Test
  void ifNotExistsReturnsExistingBeforeValidatingUnusedConnectionFields() {
    var existing =
        CatalogIntegration.newBuilder()
            .setResourceId(id("existing", ResourceKind.RK_CATALOG_INTEGRATION))
            .setDisplayName("Warehouse")
            .setType(CatalogIntegrationType.CIT_UNITY)
            .setCatalogUri("https://existing.example")
            .build();
    var meta = MutationMeta.newBuilder().setPointerVersion(9L).build();
    when(service.integrations.getByNameWithMeta("acct", "Warehouse"))
        .thenReturn(Optional.of(new ResourceWithMeta<>(existing, meta)));

    var response =
        service
            .createCatalogIntegration(
                CreateCatalogIntegrationRequest.newBuilder()
                    .setCreateMode(CreateMode.CM_RETURN_EXISTING)
                    .setSpec(
                        CatalogIntegrationSpec.newBuilder()
                            .setDisplayName("Warehouse")
                            .setType(CatalogIntegrationType.CIT_UNSPECIFIED)
                            .setCatalogUri("not a URI"))
                    .build())
            .await()
            .indefinitely();

    assertEquals(existing, response.getIntegration());
    assertEquals(meta, response.getMeta());
    verify(service.integrations, never()).createWithMeta(any());
  }

  @Test
  void ifNotExistsRaceDeletesCredentialsPreparedForUnpublishedIdentity() {
    var existing =
        CatalogIntegration.newBuilder()
            .setResourceId(id("existing", ResourceKind.RK_CATALOG_INTEGRATION))
            .setDisplayName("Warehouse")
            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
            .setCatalogUri("https://existing.example")
            .build();
    var meta = MutationMeta.newBuilder().setPointerVersion(9L).build();
    when(service.integrations.getByNameWithMeta("acct", "Warehouse"))
        .thenReturn(Optional.empty(), Optional.of(new ResourceWithMeta<>(existing, meta)));

    var response =
        service
            .createCatalogIntegration(
                validCreateRequest().toBuilder()
                    .setCreateMode(CreateMode.CM_RETURN_EXISTING)
                    .build())
            .await()
            .indefinitely();

    assertEquals(existing, response.getIntegration());
    var reference = ArgumentCaptor.forClass(String.class);
    verify(secretsManager)
        .putIfAbsent(
            eq("acct"),
            eq(CatalogIntegrationCredentialStore.SECRET_TYPE),
            reference.capture(),
            any(byte[].class));
    verify(secretsManager)
        .deleteImmediately(
            "acct", CatalogIntegrationCredentialStore.SECRET_TYPE, reference.getValue());
    verify(service.integrations, never()).createWithMeta(any());
  }

  @Test
  void createOrReplaceAtomicallySwapsToNewIdentity() {
    ResourceId existingId = id("existing", ResourceKind.RK_CATALOG_INTEGRATION);
    var existing =
        CatalogIntegration.newBuilder()
            .setResourceId(existingId)
            .setDisplayName("Warehouse")
            .setType(CatalogIntegrationType.CIT_UNITY)
            .setCatalogUri("https://old.example")
            .build();
    when(service.integrations.getByNameWithMeta("acct", "Warehouse"))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    existing, MutationMeta.newBuilder().setPointerVersion(7L).build())));
    when(service.markerStore.catalogIntegrationOverlaysMarkerVersion(existingId)).thenReturn(4L);
    when(service.overlays.countByIntegration("acct", "existing")).thenReturn(0);
    when(service.integrations.replaceIdentityWithMeta(any(), eq(7L), any(), eq(4L)))
        .thenAnswer(
            invocation ->
                Optional.of(
                    new ResourceWithMeta<>(
                        invocation.getArgument(2),
                        MutationMeta.newBuilder().setPointerVersion(1L).build())));

    var response =
        service
            .createCatalogIntegration(
                CreateCatalogIntegrationRequest.newBuilder()
                    .setCreateMode(CreateMode.CM_REPLACE)
                    .setSpec(
                        CatalogIntegrationSpec.newBuilder()
                            .setDisplayName("Warehouse")
                            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                            .setCatalogUri("https://new.example")
                            .setAuthentication(oauthAuthentication()))
                    .setCredentials(oauthCredentials())
                    .build())
            .await()
            .indefinitely();

    assertNotEquals(existingId, response.getIntegration().getResourceId());
    assertEquals(1L, response.getMeta().getPointerVersion());
    assertEquals("https://new.example", response.getIntegration().getCatalogUri());
  }

  @Test
  void createOrReplaceRejectsDependentOverlays() {
    ResourceId existingId = id("existing", ResourceKind.RK_CATALOG_INTEGRATION);
    var existing =
        CatalogIntegration.newBuilder()
            .setResourceId(existingId)
            .setDisplayName("Warehouse")
            .setType(CatalogIntegrationType.CIT_UNITY)
            .setCatalogUri("https://old.example")
            .build();
    when(service.integrations.getByNameWithMeta("acct", "Warehouse"))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    existing, MutationMeta.newBuilder().setPointerVersion(7L).build())));
    when(service.markerStore.catalogIntegrationOverlaysMarkerVersion(existingId)).thenReturn(4L);
    when(service.overlays.countByIntegration("acct", "existing")).thenReturn(2);

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogIntegration(
                        CreateCatalogIntegrationRequest.newBuilder()
                            .setCreateMode(CreateMode.CM_REPLACE)
                            .setSpec(
                                CatalogIntegrationSpec.newBuilder()
                                    .setDisplayName("Warehouse")
                                    .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                                    .setCatalogUri("https://new.example")
                                    .setAuthentication(oauthAuthentication()))
                            .setCredentials(oauthCredentials())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.ABORTED, error.getStatus().getCode());
    verify(service.integrations, never())
        .replaceIdentityWithMeta(any(), anyLong(), any(), anyLong());
  }

  @Test
  void idempotentCreateRequiresAtomicReceiptWithResourcePublication() {
    var receipt = new AtomicReference<IdempotencyRecord>();
    when(service.idempotencyStore.get(any()))
        .thenAnswer(invocation -> Optional.ofNullable(receipt.get()));
    when(service.idempotencyStore.createPending(
            any(), any(), any(), any(), any(ResourceId.class), any(), any()))
        .thenReturn(true);
    when(service.idempotencyStore.prepareSuccess(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              String opName = invocation.getArgument(2, String.class);
              String requestHash = invocation.getArgument(3, String.class);
              ResourceId resourceId = invocation.getArgument(4, ResourceId.class);
              MutationMeta meta = invocation.getArgument(5, MutationMeta.class);
              byte[] payload = invocation.getArgument(6, byte[].class);
              com.google.protobuf.Timestamp createdAt =
                  invocation.getArgument(7, com.google.protobuf.Timestamp.class);
              com.google.protobuf.Timestamp expiresAt =
                  invocation.getArgument(8, com.google.protobuf.Timestamp.class);
              receipt.set(
                  IdempotencyRecord.newBuilder()
                      .setOpName(opName)
                      .setRequestHash(requestHash)
                      .setStatus(IdempotencyRecord.Status.SUCCEEDED)
                      .setResourceId(resourceId)
                      .setMeta(meta)
                      .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                      .setCreatedAt(createdAt)
                      .setExpiresAt(expiresAt)
                      .build());
              return new PointerStore.CasUpsert(
                  "receipt", 1L, Pointer.newBuilder().setKey("receipt").build());
            });
    when(service.integrations.createWithMetaAndCompletion(any(), any()))
        .thenAnswer(
            invocation -> {
              CatalogIntegration value = invocation.getArgument(0);
              var row =
                  new ResourceWithMeta<>(
                      value, MutationMeta.newBuilder().setPointerVersion(1L).build());
              Function<ResourceWithMeta<CatalogIntegration>, PointerStore.CasUpsert> completion =
                  invocation.getArgument(1);
              completion.apply(row);
              return row;
            });

    var response =
        service
            .createCatalogIntegration(
                CreateCatalogIntegrationRequest.newBuilder()
                    .setIdempotency(
                        ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder().setKey("key"))
                    .setSpec(
                        CatalogIntegrationSpec.newBuilder()
                            .setDisplayName("Warehouse")
                            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                            .setCatalogUri("https://catalog.example")
                            .setAuthentication(oauthAuthentication()))
                    .setCredentials(oauthCredentials())
                    .build())
            .await()
            .indefinitely();

    assertEquals(receipt.get().getResourceId(), response.getIntegration().getResourceId());
    verify(service.idempotencyStore)
        .prepareSuccess(any(), any(), any(), any(), any(), any(), any(), any(), any());
    verify(service.idempotencyStore, never())
        .finalizeSuccess(any(), any(), any(), any(), any(), any(), any(), any(), any());
    verify(service.idempotencyStore, never()).delete(any());
    verify(secretsManager)
        .putIfAbsent(
            "acct",
            CatalogIntegrationCredentialStore.SECRET_TYPE,
            CatalogIntegrationCredentialStore.reference(
                response.getIntegration().getResourceId(), 1L),
            oauthCredentials().toByteArray());
  }

  @Test
  void idempotentCreateAdoptsReservedIdentityPublishedByConcurrentRetry() {
    var reservedId = new AtomicReference<ResourceId>();
    var requestHash = new AtomicReference<String>();
    var createdAt = new AtomicReference<com.google.protobuf.Timestamp>();
    var expiresAt = new AtomicReference<com.google.protobuf.Timestamp>();
    var meta = MutationMeta.newBuilder().setPointerVersion(9L).build();
    when(service.idempotencyStore.get(any()))
        .thenAnswer(
            invocation -> {
              if (reservedId.get() == null) return Optional.empty();
              var integration =
                  CatalogIntegration.newBuilder()
                      .setResourceId(reservedId.get())
                      .setDisplayName("Warehouse")
                      .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                      .setCatalogUri("https://catalog.example")
                      .build();
              return Optional.of(
                  IdempotencyRecord.newBuilder()
                      .setOpName("CreateCatalogIntegration")
                      .setRequestHash(requestHash.get())
                      .setStatus(IdempotencyRecord.Status.SUCCEEDED)
                      .setResourceId(reservedId.get())
                      .setMeta(meta)
                      .setPayload(integration.toByteString())
                      .setCreatedAt(createdAt.get())
                      .setExpiresAt(expiresAt.get())
                      .build());
            });
    when(service.idempotencyStore.createPending(
            any(), any(), any(), any(), any(ResourceId.class), any(), any()))
        .thenAnswer(
            invocation -> {
              requestHash.set(invocation.getArgument(3));
              reservedId.set(invocation.getArgument(4));
              createdAt.set(invocation.getArgument(5));
              expiresAt.set(invocation.getArgument(6));
              return true;
            });
    when(service.integrations.getByNameWithMeta("acct", "Warehouse"))
        .thenAnswer(
            invocation ->
                Optional.of(
                    new ResourceWithMeta<>(
                        CatalogIntegration.newBuilder()
                            .setResourceId(reservedId.get())
                            .setDisplayName("Warehouse")
                            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                            .setCatalogUri("https://catalog.example")
                            .build(),
                        meta)));

    var response =
        service
            .createCatalogIntegration(
                CreateCatalogIntegrationRequest.newBuilder()
                    .setIdempotency(
                        ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder().setKey("key"))
                    .setSpec(
                        CatalogIntegrationSpec.newBuilder()
                            .setDisplayName("Warehouse")
                            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                            .setCatalogUri("https://catalog.example")
                            .setAuthentication(oauthAuthentication()))
                    .setCredentials(oauthCredentials())
                    .build())
            .await()
            .indefinitely();

    assertEquals(reservedId.get(), response.getIntegration().getResourceId());
    assertEquals(meta, response.getMeta());
    verify(service.integrations, never()).createWithMetaAndCompletion(any(), any());
    verify(service.idempotencyStore, never())
        .prepareSuccess(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void getResolvesSqlName() {
    var integration =
        CatalogIntegration.newBuilder()
            .setResourceId(id("existing", ResourceKind.RK_CATALOG_INTEGRATION))
            .setDisplayName("Warehouse")
            .setAuthentication(
                CatalogAuthentication.newBuilder()
                    .setBearer(BearerAuthentication.getDefaultInstance())
                    .setCredentialsConfigured(true)
                    .setCredentialGeneration(3L))
            .build();
    when(service.integrations.getByNameWithMeta("acct", "Warehouse"))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    integration, MutationMeta.newBuilder().setPointerVersion(3L).build())));
    var response =
        service
            .getCatalogIntegration(
                GetCatalogIntegrationRequest.newBuilder().setDisplayName("Warehouse").build())
            .await()
            .indefinitely();
    assertEquals("Warehouse", response.getIntegration().getDisplayName());
    assertTrue(response.getIntegration().getAuthentication().getCredentialsConfigured());
    assertEquals(3L, response.getIntegration().getAuthentication().getCredentialGeneration());
    assertEquals(3L, response.getMeta().getPointerVersion());
  }

  @Test
  void updateCatalogUriPreservesIdentityAndAdvancesGeneration() {
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var current =
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setDisplayName("Warehouse")
            .setCatalogUri("https://catalog.example")
            .build();
    var currentMeta = MutationMeta.newBuilder().setPointerVersion(4L).build();
    var updatedMeta = MutationMeta.newBuilder().setPointerVersion(5L).build();
    when(service.integrations.getByIdWithMeta(integrationId))
        .thenReturn(Optional.of(new ResourceWithMeta<>(current, currentMeta)));
    when(service.integrations.updateWithMetaUnlessDeleting(any(), eq(4L)))
        .thenReturn(Optional.of(updatedMeta));

    var response =
        service
            .updateCatalogIntegration(
                UpdateCatalogIntegrationRequest.newBuilder()
                    .setIntegrationId(integrationId)
                    .setSpec(
                        CatalogIntegrationSpec.newBuilder().setCatalogUri("https://other.example"))
                    .setUpdateMask(FieldMask.newBuilder().addPaths("catalog_uri"))
                    .build())
            .await()
            .indefinitely();

    assertEquals(integrationId, response.getIntegration().getResourceId());
    assertEquals("Warehouse", response.getIntegration().getDisplayName());
    assertEquals("https://other.example", response.getIntegration().getCatalogUri());
    assertEquals(5L, response.getMeta().getPointerVersion());
  }

  @Test
  void updateConnectionPropertiesReplacesThePersistedMap() {
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var current =
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setDisplayName("Warehouse")
            .setCatalogUri("https://catalog.example")
            .putProperties("warehouse", "old-catalog")
            .putProperties("removed", "value")
            .build();
    var currentMeta = MutationMeta.newBuilder().setPointerVersion(4L).build();
    var updatedMeta = MutationMeta.newBuilder().setPointerVersion(5L).build();
    when(service.integrations.getByIdWithMeta(integrationId))
        .thenReturn(Optional.of(new ResourceWithMeta<>(current, currentMeta)));
    when(service.integrations.updateWithMetaUnlessDeleting(any(), eq(4L)))
        .thenReturn(Optional.of(updatedMeta));

    var response =
        service
            .updateCatalogIntegration(
                UpdateCatalogIntegrationRequest.newBuilder()
                    .setIntegrationId(integrationId)
                    .setSpec(
                        CatalogIntegrationSpec.newBuilder()
                            .putProperties("warehouse", "new-catalog"))
                    .setUpdateMask(FieldMask.newBuilder().addPaths("properties"))
                    .build())
            .await()
            .indefinitely();

    assertEquals(Map.of("warehouse", "new-catalog"), response.getIntegration().getPropertiesMap());
    assertEquals("https://catalog.example", response.getIntegration().getCatalogUri());
    assertEquals(5L, response.getMeta().getPointerVersion());
  }

  @Test
  void legacyConnectorPermissionDoesNotAuthorizeIntegrationCreate() {
    service.authz = new Authorizer();
    when(service.principal.get()).thenReturn(principal("connector.manage"));

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogIntegration(CreateCatalogIntegrationRequest.newBuilder().build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    verify(service.integrations, never()).create(any());
  }

  @Test
  void updateRejectsConnectivityFields() {
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .updateCatalogIntegration(
                        UpdateCatalogIntegrationRequest.newBuilder()
                            .setIntegrationId(integrationId)
                            .setUpdateMask(FieldMask.newBuilder().addPaths("auth"))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(service.integrations, never()).update(any(), anyLong());
  }

  @Test
  void deleteConsumesObservedDependencyMarkerInSameTransaction() {
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var current =
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setAuthentication(
                CatalogAuthentication.newBuilder()
                    .setBearer(BearerAuthentication.getDefaultInstance())
                    .setCredentialsConfigured(true)
                    .setCredentialGeneration(1L))
            .build();
    var meta = MutationMeta.newBuilder().setPointerVersion(7).build();
    when(service.integrations.getByIdWithMeta(integrationId))
        .thenReturn(
            Optional.of(
                new ai.floedb.floecat.service.repo.util.GenericResourceRepository
                    .ResourceWithMeta<>(current, meta)));
    when(service.markerStore.catalogIntegrationOverlaysMarkerVersion(integrationId)).thenReturn(4L);
    when(service.overlays.countByIntegration("acct", "integration")).thenReturn(0);
    when(service.integrations.deleteWithPreconditionAndOverlayMarker(integrationId, 7L, 4L))
        .thenReturn(true);

    service
        .deleteCatalogIntegration(
            DeleteCatalogIntegrationRequest.newBuilder().setIntegrationId(integrationId).build())
        .await()
        .indefinitely();

    verify(service.markerStore).catalogIntegrationOverlaysMarkerVersion(integrationId);
    verify(service.integrations).deleteWithPreconditionAndOverlayMarker(integrationId, 7L, 4L);
    verify(secretsManager)
        .deleteImmediately(
            "acct",
            CatalogIntegrationCredentialStore.SECRET_TYPE,
            CatalogIntegrationCredentialStore.reference(integrationId, 1L));
  }

  @Test
  void deleteRejectsDependentOverlaysBeforeSchedulingCredentialCleanup() {
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var current = CatalogIntegration.newBuilder().setResourceId(integrationId).build();
    var meta = MutationMeta.newBuilder().setPointerVersion(7).build();
    when(service.integrations.getByIdWithMeta(integrationId))
        .thenReturn(Optional.of(new ResourceWithMeta<>(current, meta)));
    when(service.markerStore.catalogIntegrationOverlaysMarkerVersion(integrationId)).thenReturn(4L);
    when(service.overlays.countByIntegration("acct", "integration")).thenReturn(2);

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .deleteCatalogIntegration(
                        DeleteCatalogIntegrationRequest.newBuilder()
                            .setIntegrationId(integrationId)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.ABORTED, error.getStatus().getCode());
    verify(service.integrations, never())
        .deleteWithPreconditionAndOverlayMarker(any(), anyLong(), anyLong());
  }

  @Test
  void cascadeDeleteRequiresOverlayWriteBeforeReadingIntegration() {
    service.authz = new Authorizer();
    when(service.principal.get()).thenReturn(principal("catalog-integration.write"));
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .deleteCatalogIntegration(
                        DeleteCatalogIntegrationRequest.newBuilder()
                            .setIntegrationId(integrationId)
                            .setCascade(true)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    verify(service.integrations, never()).getByIdWithMeta(any());
  }

  @Test
  void cascadeFencesDeletesDependentsAndAtomicallyCompletes() {
    service.authz = new Authorizer();
    when(service.principal.get())
        .thenReturn(principal("catalog-integration.write", "catalog-overlay.write"));
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var integration = CatalogIntegration.newBuilder().setResourceId(integrationId).build();
    var integrationMeta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var catalogId = id("catalog", ResourceKind.RK_CATALOG);
    var overlay =
        CatalogOverlay.newBuilder()
            .setResourceId(overlayId)
            .setCatalogId(catalogId)
            .setIntegrationId(integrationId)
            .build();
    var overlayMeta = MutationMeta.newBuilder().setPointerVersion(3L).build();
    when(service.integrations.getByIdWithMeta(integrationId))
        .thenReturn(Optional.of(new ResourceWithMeta<>(integration, integrationMeta)));
    when(service.integrations.beginCascadeDeletion(integrationId, 7L)).thenReturn(true);
    when(service.overlays.listByIntegrationWithMetaConsistent(
            eq("acct"), eq("integration"), eq(100), eq(""), any()))
        .thenReturn(List.of(new ResourceWithMeta<>(overlay, overlayMeta)), List.of());
    when(service.overlays.beginDeletion(overlayId, 3L)).thenReturn(true);
    when(service.overlays.deletionFenceVersion(overlayId)).thenReturn(1L);
    when(service.overlays.deleteWithFence(overlayId, 3L, 1L)).thenReturn(true);
    when(service.overlays.countByIntegration("acct", "integration")).thenReturn(0);
    when(service.markerStore.catalogIntegrationOverlaysMarkerVersion(integrationId)).thenReturn(4L);
    when(service.integrations.cascadeDeletionFenceVersion(integrationId)).thenReturn(1L);
    when(service.integrations.deleteWithPreconditionForCascadeDeletion(integrationId, 7L, 4L, 1L))
        .thenReturn(true);

    service
        .deleteCatalogIntegration(
            DeleteCatalogIntegrationRequest.newBuilder()
                .setIntegrationId(integrationId)
                .setCascade(true)
                .build())
        .await()
        .indefinitely();

    verify(service.integrations).beginCascadeDeletion(integrationId, 7L);
    verify(service.overlays).beginDeletion(overlayId, 3L);
    verify(service.overlays).deleteWithFence(overlayId, 3L, 1L);
    verify(service.integrations)
        .deleteWithPreconditionForCascadeDeletion(integrationId, 7L, 4L, 1L);
    verify(service.integrations, never())
        .deleteWithPreconditionAndOverlayMarker(any(), anyLong(), anyLong());
  }

  @Test
  void validatesPersistedIntegrationAndReturnsCapabilitiesAndGeneration() {
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var integration =
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setAuthentication(
                CatalogAuthentication.newBuilder()
                    .setBearer(BearerAuthentication.getDefaultInstance())
                    .setCredentialGeneration(3L))
            .build();
    var meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    when(service.integrations.getByIdWithMeta(integrationId))
        .thenReturn(Optional.of(new ResourceWithMeta<>(integration, meta)));
    var check =
        ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationCheck.newBuilder()
            .setType(
                ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationCheckType
                    .CIVCT_CATALOG_CONNECTION)
            .setStatus(
                ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationStatus.CIVS_PASSED)
            .build();
    when(service.discovery.validate(integration))
        .thenReturn(
            new CatalogIntegrationDiscovery.ValidationResult(
                true,
                List.of(check),
                ai.floedb.floecat.catalog.access.CatalogCapabilities.of(
                    ai.floedb.floecat.catalog.access.CatalogCapability.VALIDATE,
                    ai.floedb.floecat.catalog.access.CatalogCapability.LIST_NAMESPACES)));

    var response =
        service
            .validateCatalogIntegration(
                ai.floedb.floecat.integration.rpc.ValidateCatalogIntegrationRequest.newBuilder()
                    .setIntegrationId(integrationId)
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getValid());
    assertEquals(meta, response.getIntegrationMeta());
    assertEquals(2, response.getCapabilitiesCount());
    verify(service.authz).require(any(), eq("catalog-integration.use"));
  }

  @Test
  void pagesUpstreamNamespacesAndListsLightweightObjects() {
    var integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var integration =
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setAuthentication(CatalogAuthentication.newBuilder().setCredentialGeneration(3L))
            .build();
    var meta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    when(service.integrations.getByIdWithMeta(integrationId))
        .thenReturn(Optional.of(new ResourceWithMeta<>(integration, meta)));
    var parent = ai.floedb.floecat.catalog.access.NamespacePath.of("Production");
    var finance = ai.floedb.floecat.catalog.access.NamespacePath.of("Production", "Finance");
    var sales = ai.floedb.floecat.catalog.access.NamespacePath.of("Production", "Sales");
    when(service.discovery.listNamespaces(integration, parent)).thenReturn(List.of(finance, sales));

    var first =
        service
            .listUpstreamNamespaces(
                ai.floedb.floecat.integration.rpc.ListUpstreamNamespacesRequest.newBuilder()
                    .setIntegrationId(integrationId)
                    .setParent(
                        ai.floedb.floecat.integration.rpc.NamespacePath.newBuilder()
                            .addSegments("Production"))
                    .setPage(ai.floedb.floecat.common.rpc.PageRequest.newBuilder().setPageSize(1))
                    .build())
            .await()
            .indefinitely();
    assertEquals(
        List.of("Production", "Finance"), first.getNamespaces(0).getPath().getSegmentsList());
    assertFalse(first.getPage().getNextPageToken().isBlank());

    when(service.discovery.listObjects(integration, sales, Set.of()))
        .thenReturn(
            List.of(
                new CatalogIntegrationDiscovery.DiscoveredObject(
                    new ai.floedb.floecat.catalog.access.CatalogObjectName(sales, "orders"),
                    CatalogIntegrationDiscovery.ObjectKind.TABLE)));
    var objects =
        service
            .listUpstreamObjects(
                ai.floedb.floecat.integration.rpc.ListUpstreamObjectsRequest.newBuilder()
                    .setIntegrationId(integrationId)
                    .setNamespace(
                        ai.floedb.floecat.integration.rpc.NamespacePath.newBuilder()
                            .addSegments("Production")
                            .addSegments("Sales"))
                    .build())
            .await()
            .indefinitely();
    assertEquals("orders", objects.getObjects(0).getName());
    assertEquals(
        ai.floedb.floecat.integration.rpc.UpstreamObjectKind.UOK_TABLE,
        objects.getObjects(0).getKind());
    assertEquals(meta, objects.getIntegrationMeta());
  }

  private static PrincipalContext principal() {
    return principal(
        "catalog-integration.read", "catalog-integration.write", "catalog-integration.use");
  }

  private static CatalogAuthentication oauthAuthentication() {
    return CatalogAuthentication.newBuilder()
        .setOauthClientCredentials(
            OAuthClientCredentialsAuthentication.newBuilder().setClientId("client-id"))
        .build();
  }

  private static CatalogIntegrationCredentials oauthCredentials() {
    return CatalogIntegrationCredentials.newBuilder()
        .setOauthClientSecret(SecretValue.newBuilder().setValue("client-secret"))
        .build();
  }

  private static CreateCatalogIntegrationRequest validCreateRequest() {
    return CreateCatalogIntegrationRequest.newBuilder()
        .setSpec(
            CatalogIntegrationSpec.newBuilder()
                .setDisplayName("Warehouse")
                .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                .setCatalogUri("https://catalog.example")
                .setAuthentication(oauthAuthentication()))
        .setCredentials(oauthCredentials())
        .build();
  }

  private static CreateCatalogIntegrationRequest createRequestWithAuthentication(
      CatalogAuthentication authentication) {
    return CreateCatalogIntegrationRequest.newBuilder()
        .setSpec(
            CatalogIntegrationSpec.newBuilder()
                .setDisplayName("Warehouse")
                .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
                .setCatalogUri("https://catalog.example")
                .setAuthentication(authentication))
        .build();
  }

  private static PrincipalContext principal(String... permissions) {
    return PrincipalContext.newBuilder()
        .setAccountId("acct")
        .setCorrelationId("corr")
        .addAllPermissions(List.of(permissions))
        .build();
  }

  private static ResourceId id(String value, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("acct").setId(value).setKind(kind).build();
  }

  private static void installBasePrincipal(
      CatalogIntegrationsImpl service, PrincipalProvider principalProvider) {
    try {
      Field field =
          ai.floedb.floecat.service.common.BaseServiceImpl.class.getDeclaredField("principal");
      field.setAccessible(true);
      field.set(service, principalProvider);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Failed to inject BaseServiceImpl principal provider", e);
    }
  }
}
