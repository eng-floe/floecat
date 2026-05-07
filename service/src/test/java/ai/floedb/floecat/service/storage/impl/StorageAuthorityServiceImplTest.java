/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.rpc.DeleteStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.GetStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityForLocationRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.rpc.StorageAuthoritySpec;
import ai.floedb.floecat.storage.rpc.UpdateStorageAuthorityRequest;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StorageAuthorityServiceImplTest {
  private static final ResourceId AUTHORITY_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
          .setId("sa-1")
          .build();
  private static final ResourceId FOREIGN_AUTHORITY_ID =
      AUTHORITY_ID.toBuilder().setAccountId("foreign-acct").build();
  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("tbl-1")
          .build();
  private static final ResourceId FOREIGN_TABLE_ID =
      TABLE_ID.toBuilder().setAccountId("foreign").build();

  private StorageAuthorityServiceImpl service;
  private StorageAuthorityRepository repo;
  private PrincipalProvider principalProvider;
  private Authorizer authz;
  private RecordingSecretsManager secretsManager;
  private TableRepository tableRepo;
  private AtomicReference<StorageAuthority> state;
  private AtomicLong version;

  @BeforeEach
  void setUp() {
    service = new StorageAuthorityServiceImpl();
    repo = mock(StorageAuthorityRepository.class);
    principalProvider = mock(PrincipalProvider.class);
    authz = mock(Authorizer.class);
    secretsManager = new RecordingSecretsManager();
    tableRepo = mock(TableRepository.class);
    state = new AtomicReference<>(currentAuthority());
    version = new AtomicLong(1L);

    service.repo = repo;
    service.principalProvider = principalProvider;
    service.authz = authz;
    service.secretsManager = secretsManager;
    service.resolver = new StorageAuthorityResolver();
    service.resolver.secretsManager = secretsManager;
    service.tableRepo = tableRepo;
    installBasePrincipal(service, principalProvider);

    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setCorrelationId("corr")
            .addPermissions("connector.manage")
            .addPermissions("connector.read")
            .addPermissions("table.read")
            .addPermissions("catalog.read")
            .addPermissions(RolePermissions.STORAGE_AUTHORITY_RESOLVE_INTERNAL)
            .build();
    when(principalProvider.get()).thenReturn(principal);

    when(repo.getById(AUTHORITY_ID)).thenAnswer(_ -> Optional.ofNullable(state.get()));
    when(repo.metaFor(AUTHORITY_ID))
        .thenAnswer(_ -> MutationMeta.newBuilder().setPointerVersion(version.get()).build());
    when(repo.metaForSafe(AUTHORITY_ID))
        .thenAnswer(_ -> MutationMeta.newBuilder().setPointerVersion(version.get()).build());
    when(repo.update(any(StorageAuthority.class), anyLong()))
        .thenAnswer(
            invocation -> {
              state.set(invocation.getArgument(0, StorageAuthority.class));
              version.incrementAndGet();
              return true;
            });
    when(repo.deleteWithPrecondition(eq(AUTHORITY_ID), anyLong())).thenReturn(true);
    when(repo.list(eq("acct"), anyInt(), any(), any()))
        .thenReturn(java.util.List.of(currentAuthority()));
    when(tableRepo.getById(TABLE_ID)).thenReturn(Optional.of(currentTable()));
  }

  @Test
  void updateClearingCredentialsDeletesStoredSecret() {
    UpdateStorageAuthorityRequest request =
        UpdateStorageAuthorityRequest.newBuilder()
            .setAuthorityId(AUTHORITY_ID)
            .setSpec(StorageAuthoritySpec.newBuilder().build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("credentials").build())
            .build();

    service.updateStorageAuthority(request).await().indefinitely();

    assertTrue(secretsManager.deleteCalled);
    assertEquals("sa-1", secretsManager.lastSecretId);
  }

  @Test
  void getScopesAuthorityIdToPrincipalAccount() {
    var response =
        service
            .getStorageAuthority(
                GetStorageAuthorityRequest.newBuilder()
                    .setAuthorityId(FOREIGN_AUTHORITY_ID)
                    .build())
            .await()
            .indefinitely();

    assertEquals(AUTHORITY_ID, response.getAuthority().getResourceId());
    verify(repo).getById(AUTHORITY_ID);
  }

  @Test
  void updateScopesAuthorityIdToPrincipalAccount() {
    UpdateStorageAuthorityRequest request =
        UpdateStorageAuthorityRequest.newBuilder()
            .setAuthorityId(FOREIGN_AUTHORITY_ID)
            .setSpec(StorageAuthoritySpec.newBuilder().setRegion("us-west-2").build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("region").build())
            .build();

    service.updateStorageAuthority(request).await().indefinitely();

    verify(repo, times(2)).getById(AUTHORITY_ID);
    verify(repo, times(2)).metaFor(AUTHORITY_ID);
  }

  @Test
  void deleteScopesAuthorityIdToPrincipalAccount() {
    service
        .deleteStorageAuthority(
            DeleteStorageAuthorityRequest.newBuilder().setAuthorityId(FOREIGN_AUTHORITY_ID).build())
        .await()
        .indefinitely();

    verify(repo).deleteWithPrecondition(eq(AUTHORITY_ID), anyLong());
    assertEquals("sa-1", secretsManager.lastSecretId);
  }

  @Test
  void resolveScopesTableIdToPrincipalAccount() {
    ResolveStorageAuthorityResponse response =
        service
            .resolveStorageAuthority(
                ResolveStorageAuthorityRequest.newBuilder()
                    .setTableId(FOREIGN_TABLE_ID)
                    .setLocationPrefix("s3://warehouse/orders")
                    .build())
            .await()
            .indefinitely();

    verify(repo).list(eq("acct"), anyInt(), any(), any());
    verify(tableRepo).getById(TABLE_ID);
    assertEquals(AUTHORITY_ID, response.getAuthorityId());
  }

  @Test
  void resolveRejectsCallerLocationOutsideTableLocation() {
    var ex =
        org.junit.jupiter.api.Assertions.assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .resolveStorageAuthority(
                        ResolveStorageAuthorityRequest.newBuilder()
                            .setTableId(TABLE_ID)
                            .setLocationPrefix("s3://warehouse/other")
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void resolveForLocationAllowsInternalLookupWithoutTableLoad() {
    ResolveStorageAuthorityResponse response =
        service
            .resolveStorageAuthorityForLocation(
                ResolveStorageAuthorityForLocationRequest.newBuilder()
                    .setLocationPrefix("s3://warehouse/orders/metadata/v1.json")
                    .build())
            .await()
            .indefinitely();

    verify(repo).list(eq("acct"), anyInt(), any(), any());
    verify(tableRepo, org.mockito.Mockito.never()).getById(any());
    verify(authz)
        .require(
            any(PrincipalContext.class), eq(RolePermissions.STORAGE_AUTHORITY_RESOLVE_INTERNAL));
    assertEquals(AUTHORITY_ID, response.getAuthorityId());
  }

  @Test
  void resolveForLocationRejectsLookupWithoutInternalPermission() {
    doThrow(
            io.grpc.Status.PERMISSION_DENIED
                .withDescription("missing permission")
                .asRuntimeException())
        .when(authz)
        .require(
            any(PrincipalContext.class), eq(RolePermissions.STORAGE_AUTHORITY_RESOLVE_INTERNAL));

    var ex =
        org.junit.jupiter.api.Assertions.assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .resolveStorageAuthorityForLocation(
                        ResolveStorageAuthorityForLocationRequest.newBuilder()
                            .setLocationPrefix("s3://warehouse/orders/metadata/v1.json")
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verify(repo, org.mockito.Mockito.never()).list(eq("acct"), anyInt(), any(), any());
    verify(tableRepo, org.mockito.Mockito.never()).getById(any());
  }

  @Test
  void clientSideCredentialVendingRejectsUnscopedTemporaryAuthorityCredentials() {
    StorageAuthorityResolver resolver = new StorageAuthorityResolver();
    var authority = currentAuthority().toBuilder().clearAssumeRoleArn().build();
    var temporaryCredentials =
        AuthCredentials.newBuilder()
            .setAws(
                AuthCredentials.AwsCredentials.newBuilder()
                    .setAccessKeyId("akid")
                    .setSecretAccessKey("secret")
                    .setSessionToken("session"))
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                resolver.mintTemporaryCredentials(
                    authority, temporaryCredentials, "s3://warehouse/orders"));

    assertTrue(ex.getMessage().contains("scoped temporary storage credentials"));
  }

  @Test
  void updateNonCredentialFieldRetainsStoredSecret() {
    UpdateStorageAuthorityRequest request =
        UpdateStorageAuthorityRequest.newBuilder()
            .setAuthorityId(AUTHORITY_ID)
            .setSpec(StorageAuthoritySpec.newBuilder().setRegion("us-west-2").build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("region").build())
            .build();

    service.updateStorageAuthority(request).await().indefinitely();

    assertFalse(secretsManager.deleteCalled);
    assertFalse(secretsManager.putCalled);
    assertFalse(secretsManager.updateCalled);
    assertEquals("us-west-2", state.get().getRegion());
  }

  @Test
  void fullReplaceWithoutCredentialsDeletesStoredSecret() {
    StorageAuthoritySpec replacement =
        StorageAuthoritySpec.newBuilder()
            .setDisplayName("renamed")
            .setEnabled(true)
            .setType("s3")
            .setLocationPrefix("s3://warehouse/renamed")
            .setRegion("us-east-2")
            .build();

    UpdateStorageAuthorityRequest request =
        UpdateStorageAuthorityRequest.newBuilder()
            .setAuthorityId(AUTHORITY_ID)
            .setSpec(replacement)
            .build();

    var response = service.updateStorageAuthority(request).await().indefinitely();

    assertTrue(secretsManager.deleteCalled);
    assertEquals("renamed", response.getAuthority().getDisplayName());
    assertEquals("s3://warehouse/renamed", response.getAuthority().getLocationPrefix());
    assertEquals("us-east-2", response.getAuthority().getRegion());
  }

  private static StorageAuthority currentAuthority() {
    return StorageAuthority.newBuilder()
        .setResourceId(AUTHORITY_ID)
        .setDisplayName("orders")
        .setEnabled(true)
        .setType("s3")
        .setLocationPrefix("s3://warehouse/orders")
        .setRegion("us-east-1")
        .setCreatedAt(Timestamps.fromSeconds(1))
        .setUpdatedAt(Timestamps.fromSeconds(1))
        .build();
  }

  private static ai.floedb.floecat.catalog.rpc.Table currentTable() {
    return ai.floedb.floecat.catalog.rpc.Table.newBuilder()
        .setResourceId(TABLE_ID)
        .putProperties("location", "s3://warehouse/orders")
        .build();
  }

  private static void installBasePrincipal(
      StorageAuthorityServiceImpl service, PrincipalProvider principalProvider) {
    try {
      Field field =
          ai.floedb.floecat.service.common.BaseServiceImpl.class.getDeclaredField("principal");
      field.setAccessible(true);
      field.set(service, principalProvider);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Failed to inject BaseServiceImpl principal provider", e);
    }
  }

  private static final class RecordingSecretsManager implements SecretsManager {
    boolean putCalled;
    boolean updateCalled;
    boolean deleteCalled;
    String lastSecretId;

    @Override
    public void put(String accountId, String secretType, String secretId, byte[] payload) {
      putCalled = true;
      lastSecretId = secretId;
    }

    @Override
    public Optional<byte[]> get(String accountId, String secretType, String secretId) {
      lastSecretId = secretId;
      return Optional.of(
          AuthCredentials.newBuilder()
              .setAws(
                  AuthCredentials.AwsCredentials.newBuilder()
                      .setAccessKeyId("akid")
                      .setSecretAccessKey("secret"))
              .build()
              .toByteArray());
    }

    @Override
    public void update(String accountId, String secretType, String secretId, byte[] payload) {
      updateCalled = true;
      lastSecretId = secretId;
    }

    @Override
    public void delete(String accountId, String secretType, String secretId) {
      deleteCalled = true;
      lastSecretId = secretId;
    }
  }
}
