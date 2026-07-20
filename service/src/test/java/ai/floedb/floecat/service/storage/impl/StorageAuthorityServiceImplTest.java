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
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.rpc.DeleteStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.GetStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.ResolveSnapshotCompatStorageRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.rpc.StorageAuthoritySpec;
import ai.floedb.floecat.storage.rpc.StorageCredentialUsage;
import ai.floedb.floecat.storage.rpc.UpdateStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.VendStorageCredentialsRequest;
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
  private static final ResourceId CONNECTOR_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_CONNECTOR)
          .setId("conn-1")
          .build();
  private static final ResourceId FOREIGN_TABLE_ID =
      TABLE_ID.toBuilder().setAccountId("foreign").build();
  private static final ResourceId DATARBRICKS_AUTHORITY_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
          .setId("sa-db")
          .build();

  private StorageAuthorityServiceImpl service;
  private StorageAuthorityRepository repo;
  private PrincipalProvider principalProvider;
  private Authorizer authz;
  private RecordingSecretsManager secretsManager;
  private TableRepository tableRepo;
  private ConnectorRepository connectorRepo;
  private SnapshotRepository snapshotRepo;
  private ReconcileJobStore reconcileJobs;
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
    connectorRepo = mock(ConnectorRepository.class);
    snapshotRepo = mock(SnapshotRepository.class);
    reconcileJobs = mock(ReconcileJobStore.class);
    state = new AtomicReference<>(currentAuthority());
    version = new AtomicLong(1L);

    service.repo = repo;
    service.principalProvider = principalProvider;
    service.authz = authz;
    service.secretsManager = secretsManager;
    service.resolver = new StorageAuthorityResolver();
    service.resolver.secretsManager = secretsManager;
    service.tableRepo = tableRepo;
    service.connectorRepo = connectorRepo;
    service.snapshotRepo = snapshotRepo;
    service.reconcileJobs = reconcileJobs;
    service.blobStoreType = "s3";
    service.blobBucket = "floecat-dev";
    service.storageAwsRegion = "us-east-1";
    service.storageAwsS3Endpoint = Optional.of("http://localstack:4566");
    service.storageAwsPathStyleAccess = true;
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
    when(connectorRepo.getById(CONNECTOR_ID)).thenReturn(Optional.of(discoveryConnector()));
    when(snapshotRepo.getById(TABLE_ID, 77L))
        .thenReturn(Optional.of(currentSnapshot(TABLE_ID, 77L)));
    when(reconcileJobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(reconcileJobs.getLeaseView("job-1")).thenReturn(Optional.of(activeLeaseView()));
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
  void resolveServerSideScopesTableIdToPrincipalAccount() {
    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setTableId(FOREIGN_TABLE_ID)
                    .setLocationPrefix("s3://warehouse/orders")
                    .setUsage(StorageCredentialUsage.SCU_SERVER)
                    .build())
            .await()
            .indefinitely();

    verify(repo).list(eq("acct"), anyInt(), any(), any());
    verify(tableRepo).getById(TABLE_ID);
    assertEquals(AUTHORITY_ID, response.getAuthorityId());
  }

  @Test
  void resolveServerSideUsesRequestedLocationPrefixWhenItDiffersFromTableLocation() {
    StorageAuthority databricksAuthority =
        StorageAuthority.newBuilder()
            .setResourceId(DATARBRICKS_AUTHORITY_ID)
            .setDisplayName("databricks")
            .setEnabled(true)
            .setType("s3")
            .setLocationPrefix("s3://floedb-databricks-metastore-367509577365")
            .setRegion("us-east-1")
            .setCreatedAt(Timestamps.fromSeconds(1))
            .setUpdatedAt(Timestamps.fromSeconds(1))
            .build();
    when(repo.list(eq("acct"), anyInt(), any(), any()))
        .thenReturn(java.util.List.of(currentAuthority(), databricksAuthority));
    when(tableRepo.getById(TABLE_ID))
        .thenReturn(Optional.of(tableWithRequestedDatabricksSubprefix()));

    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setTableId(TABLE_ID)
                    .setLocationPrefix(
                        "s3://floedb-databricks-metastore-367509577365/metastore/table/metadata")
                    .setUsage(StorageCredentialUsage.SCU_SERVER)
                    .build())
            .await()
            .indefinitely();

    assertEquals(DATARBRICKS_AUTHORITY_ID, response.getAuthorityId());
  }

  @Test
  void resolveServerSidePrefersStorageLocationOverSourceMetadataLocation() {
    when(tableRepo.getById(TABLE_ID))
        .thenReturn(Optional.of(tableWithStorageAndMetadataLocation()));

    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setTableId(TABLE_ID)
                    .setUsage(StorageCredentialUsage.SCU_SERVER)
                    .build())
            .await()
            .indefinitely();

    assertEquals(AUTHORITY_ID, response.getAuthorityId());
  }

  @Test
  void resolveRejectsCallerLocationOutsideTableLocation() {
    var ex =
        org.junit.jupiter.api.Assertions.assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setTableId(TABLE_ID)
                            .setLocationPrefix("s3://warehouse/other")
                            .setUsage(StorageCredentialUsage.SCU_CLIENT)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void resolveForLocationAllowsInternalLookupWithoutTableLoad() {
    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setLocationPrefix("s3://warehouse/orders/data/part-000.parquet")
                    .setUsage(StorageCredentialUsage.SCU_SERVER)
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
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://warehouse/orders/metadata/v1.json")
                            .setUsage(StorageCredentialUsage.SCU_SERVER)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verify(repo, org.mockito.Mockito.never()).list(eq("acct"), anyInt(), any(), any());
    verify(tableRepo, org.mockito.Mockito.never()).getById(any());
  }

  @Test
  void resolveForAccountLocationRequiresValidMatchingLeaseWhenProvided() {
    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setLocationPrefix("s3://warehouse/orders/data/part-000.parquet")
                    .setUsage(StorageCredentialUsage.SCU_SERVER)
                    .setExecutionBinding(
                        ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
                            .setReconcileLease(
                                ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding.newBuilder()
                                    .setJobId("job-1")
                                    .setLeaseEpoch("lease-1"))
                            .build())
                    .build())
            .await()
            .indefinitely();

    verify(reconcileJobs).renewLease("job-1", "lease-1");
    verify(reconcileJobs).getLeaseView("job-1");
    assertEquals(AUTHORITY_ID, response.getAuthorityId());
  }

  @Test
  void resolveForDiscoveryPlannerUsesConnectorBootstrapScopeBeforeTableExists() {
    when(reconcileJobs.getLeaseView("job-1")).thenReturn(Optional.of(discoveryTableLeaseView()));

    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setLocationPrefix("s3://warehouse/orders/metadata/v1.json")
                    .setUsage(StorageCredentialUsage.SCU_SERVER)
                    .setExecutionBinding(
                        ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
                            .setReconcileLease(
                                ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding.newBuilder()
                                    .setJobId("job-1")
                                    .setLeaseEpoch("lease-1")))
                    .build())
            .await()
            .indefinitely();

    assertEquals(AUTHORITY_ID, response.getAuthorityId());
    verify(connectorRepo).getById(CONNECTOR_ID);
    verify(tableRepo, org.mockito.Mockito.never()).getById(any());
  }

  @Test
  void resolveForDiscoveryPlannerRejectsLocationOutsideConnectorBootstrapScope() {
    when(reconcileJobs.getLeaseView("job-1")).thenReturn(Optional.of(discoveryTableLeaseView()));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://warehouse/other/metadata/v1.json")
                            .setUsage(StorageCredentialUsage.SCU_SERVER)
                            .setExecutionBinding(
                                ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
                                    .setReconcileLease(
                                        ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding
                                            .newBuilder()
                                            .setJobId("job-1")
                                            .setLeaseEpoch("lease-1")))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
  }

  @Test
  void resolveForViewPlannerUsesConnectorBootstrapScope() {
    when(reconcileJobs.getLeaseView("job-1")).thenReturn(Optional.of(viewLeaseView()));

    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setLocationPrefix("s3://warehouse/orders/metadata/v1.json")
                    .setUsage(StorageCredentialUsage.SCU_SERVER)
                    .setExecutionBinding(
                        ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
                            .setReconcileLease(
                                ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding.newBuilder()
                                    .setJobId("job-1")
                                    .setLeaseEpoch("lease-1")))
                    .build())
            .await()
            .indefinitely();

    assertEquals(AUTHORITY_ID, response.getAuthorityId());
  }

  @Test
  void resolveForDiscoveryTableUsesRequestTableBoundToLeasedSource() {
    when(reconcileJobs.getLeaseView("job-1")).thenReturn(Optional.of(discoveryTableLeaseView()));

    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setTableId(TABLE_ID)
                    .setLocationPrefix("s3://warehouse/orders/metadata/v1.json")
                    .setUsage(StorageCredentialUsage.SCU_SERVER)
                    .setExecutionBinding(
                        ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
                            .setReconcileLease(
                                ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding.newBuilder()
                                    .setJobId("job-1")
                                    .setLeaseEpoch("lease-1")))
                    .build())
            .await()
            .indefinitely();

    assertEquals(AUTHORITY_ID, response.getAuthorityId());
  }

  @Test
  void resolveForDiscoveryTableRejectsTableFromDifferentSource() {
    when(reconcileJobs.getLeaseView("job-1")).thenReturn(Optional.of(discoveryTableLeaseView()));
    when(tableRepo.getById(TABLE_ID))
        .thenReturn(
            Optional.of(
                currentTable().toBuilder()
                    .setUpstream(
                        currentTable().getUpstream().toBuilder().setTableDisplayName("other"))
                    .build()));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setTableId(TABLE_ID)
                            .setLocationPrefix("s3://warehouse/orders/metadata/v1.json")
                            .setUsage(StorageCredentialUsage.SCU_SERVER)
                            .setExecutionBinding(
                                ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
                                    .setReconcileLease(
                                        ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding
                                            .newBuilder()
                                            .setJobId("job-1")
                                            .setLeaseEpoch("lease-1")))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
  }

  @Test
  void
      resolveForAccountLocationAllowsStaticServerCredentialsForLeaseBoundExecutionWhenNotRequired() {
    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setLocationPrefix("s3://warehouse/orders/data/part-000.parquet")
                    .setUsage(StorageCredentialUsage.SCU_SERVER)
                    .setExecutionBinding(
                        ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
                            .setReconcileLease(
                                ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding.newBuilder()
                                    .setJobId("job-1")
                                    .setLeaseEpoch("lease-1"))
                            .build())
                    .build())
            .await()
            .indefinitely();

    assertEquals(AUTHORITY_ID, response.getAuthorityId());
    assertEquals("akid", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
    assertEquals(
        "secret", response.getStorageCredentials(0).getConfigMap().get("s3.secret-access-key"));
  }

  @Test
  void resolveForAccountLocationFailsForNoAuthority() {
    when(repo.list(eq("acct"), anyInt(), any(), any())).thenReturn(java.util.List.of());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://warehouse/orders/data/part-000.parquet")
                            .setUsage(StorageCredentialUsage.SCU_SERVER)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void resolveForAccountLocationRejectsLocationOutsideLeasedTableScope() {
    var ex =
        org.junit.jupiter.api.Assertions.assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://warehouse/other/metadata/v1.json")
                            .setUsage(StorageCredentialUsage.SCU_SERVER)
                            .setExecutionBinding(
                                ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
                                    .setReconcileLease(
                                        ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding
                                            .newBuilder()
                                            .setJobId("job-1")
                                            .setLeaseEpoch("lease-1"))
                                    .build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
  }

  @Test
  void resolveForAccountLocationAllowsSiblingFileWithinLeasedTableScope() {
    when(reconcileJobs.getLeaseView("job-1"))
        .thenReturn(
            Optional.of(
                activeLeaseView(
                    "job-1",
                    "acct",
                    "JS_RUNNING",
                    java.util.List.of(
                        "s3://warehouse/orders/data/part-000.parquet",
                        "s3://warehouse/orders/metadata/delete-000.parquet"))));

    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setLocationPrefix("s3://warehouse/orders/data/part-999.parquet")
                    .setUsage(StorageCredentialUsage.SCU_SERVER)
                    .setExecutionBinding(
                        ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
                            .setReconcileLease(
                                ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding.newBuilder()
                                    .setJobId("job-1")
                                    .setLeaseEpoch("lease-1"))
                            .build())
                    .build())
            .await()
            .indefinitely();

    assertEquals(AUTHORITY_ID, response.getAuthorityId());
  }

  @Test
  void resolveForAccountLocationRejectsMismatchedLeaseAccount() {
    when(reconcileJobs.getLeaseView("job-2"))
        .thenReturn(Optional.of(activeLeaseView("job-2", "other", "JS_RUNNING")));
    when(reconcileJobs.renewLease("job-2", "lease-2")).thenReturn(true);

    var ex =
        org.junit.jupiter.api.Assertions.assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://warehouse/orders/metadata/v1.json")
                            .setUsage(StorageCredentialUsage.SCU_SERVER)
                            .setExecutionBinding(
                                ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
                                    .setReconcileLease(
                                        ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding
                                            .newBuilder()
                                            .setJobId("job-2")
                                            .setLeaseEpoch("lease-2"))
                                    .build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
  }

  @Test
  void resolveSnapshotCompatStorageUsesConfigBackedSettings() {
    var response =
        service
            .resolveSnapshotCompatStorage(
                ResolveSnapshotCompatStorageRequest.newBuilder()
                    .setTableId(TABLE_ID)
                    .setSnapshotId(77L)
                    .build())
            .await()
            .indefinitely();

    assertEquals(
        "s3://floecat-dev"
            + ai.floedb.floecat.service.repo.model.Keys.snapshotCompatIcebergRestPrefix(
                "acct", "tbl-1", 77L),
        response.getLocationPrefix());
    assertEquals(
        "http://localstack:4566",
        response.getStorage().getClientSafeConfigMap().get("s3.endpoint"));
    assertEquals(
        "true", response.getStorage().getClientSafeConfigMap().get("s3.path-style-access"));
    assertEquals("us-east-1", response.getStorage().getClientSafeConfigMap().get("s3.region"));
    assertEquals(0, response.getStorage().getStorageCredentialsCount());
    verify(repo, org.mockito.Mockito.never()).list(eq("acct"), anyInt(), any(), any());
  }

  @Test
  void resolveSnapshotCompatStorageReturnsEmptyStorageConfigForMemoryBlobStore() {
    service.blobStoreType = "memory";

    var response =
        service
            .resolveSnapshotCompatStorage(
                ResolveSnapshotCompatStorageRequest.newBuilder()
                    .setTableId(TABLE_ID)
                    .setSnapshotId(77L)
                    .build())
            .await()
            .indefinitely();

    assertEquals(0, response.getStorage().getClientSafeConfigCount());
    assertEquals(0, response.getStorage().getStorageCredentialsCount());
    verify(repo, org.mockito.Mockito.never()).list(eq("acct"), anyInt(), any(), any());
  }

  @Test
  void clientSideCredentialVendingRejectsCredentialsWithoutKnownExpiry() {
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
                    authority, temporaryCredentials, java.util.List.of("s3://warehouse/orders")));

    assertTrue(ex.getMessage().contains("known expiry"));
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
        .setUpstream(
            ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                .setConnectorId(
                    ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setKind(ResourceKind.RK_CONNECTOR)
                        .setId("conn-1"))
                .addNamespacePath("src")
                .setTableDisplayName("orders"))
        .build();
  }

  private static Connector discoveryConnector() {
    return Connector.newBuilder()
        .setResourceId(CONNECTOR_ID)
        .setDisplayName("discovery")
        .setKind(ConnectorKind.CK_DELTA)
        .setState(ConnectorState.CS_ACTIVE)
        .putProperties("delta.table-root", "s3://warehouse/orders")
        .build();
  }

  private static ai.floedb.floecat.catalog.rpc.Table reconciledTable() {
    return ai.floedb.floecat.catalog.rpc.Table.newBuilder()
        .setResourceId(TABLE_ID)
        .putProperties("location", "s3://floecat-dev/obs/floe_prod_otel_spans")
        .putProperties(
            "source_metadata_location",
            "s3://floedb-databricks-metastore-367509577365/metastore/table/metadata/00001.metadata.json")
        .build();
  }

  private static ai.floedb.floecat.catalog.rpc.Table tableWithRequestedDatabricksSubprefix() {
    return ai.floedb.floecat.catalog.rpc.Table.newBuilder()
        .setResourceId(TABLE_ID)
        .putProperties(
            "storage_location", "s3://floedb-databricks-metastore-367509577365/metastore/table")
        .putProperties("location", "s3://floecat-dev/obs/floe_prod_otel_spans")
        .putProperties(
            "source_metadata_location",
            "s3://floedb-databricks-metastore-367509577365/metastore/table/metadata/00001.metadata.json")
        .build();
  }

  private static ai.floedb.floecat.catalog.rpc.Table tableWithStorageAndMetadataLocation() {
    return ai.floedb.floecat.catalog.rpc.Table.newBuilder()
        .setResourceId(TABLE_ID)
        .putProperties("storage_location", "s3://warehouse/orders")
        .putProperties("location", "s3://warehouse/orders")
        .putProperties(
            "source_metadata_location",
            "s3://floedb-databricks-metastore-367509577365/metastore/table/metadata/00001.metadata.json")
        .build();
  }

  private static ai.floedb.floecat.catalog.rpc.Snapshot currentSnapshot(
      ResourceId tableId, long snapshotId) {
    return ai.floedb.floecat.catalog.rpc.Snapshot.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
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

  private static ReconcileJobStore.ReconcileJob activeLeaseView() {
    return activeLeaseView("job-1", "acct", "JS_RUNNING");
  }

  private static ReconcileJobStore.ReconcileJob discoveryTableLeaseView() {
    return new ReconcileJobStore.ReconcileJob(
        "job-1",
        "acct",
        "conn-1",
        "JS_RUNNING",
        "",
        1L,
        1L,
        1L,
        1L,
        0L,
        0L,
        0L,
        false,
        ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "",
        ReconcileJobKind.PLAN_TABLE,
        ReconcileTableTask.discovery("src", "orders", "namespace-1", "orders"),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        "");
  }

  private static ReconcileJobStore.ReconcileJob viewLeaseView() {
    return new ReconcileJobStore.ReconcileJob(
        "job-1",
        "acct",
        "conn-1",
        "JS_RUNNING",
        "",
        1L,
        1L,
        1L,
        1L,
        0L,
        0L,
        0L,
        false,
        ReconcilerService.CaptureMode.METADATA_ONLY,
        0L,
        0L,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "",
        ReconcileJobKind.PLAN_VIEW,
        ReconcileTableTask.empty(),
        ReconcileViewTask.discovery("src", "orders_view", "namespace-1", "orders_view"),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        "");
  }

  private static ReconcileJobStore.ReconcileJob activeLeaseView(
      String jobId, String accountId, String state) {
    return activeLeaseView(
        jobId, accountId, state, java.util.List.of("s3://warehouse/orders/data/part-000.parquet"));
  }

  private static ReconcileJobStore.ReconcileJob activeLeaseView(
      String jobId, String accountId, String state, java.util.List<String> filePaths) {
    return new ReconcileJobStore.ReconcileJob(
        jobId,
        accountId,
        "conn-1",
        state,
        "",
        1L,
        1L,
        1L,
        1L,
        0L,
        0L,
        0L,
        false,
        ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "",
        ReconcileJobKind.EXEC_FILE_GROUP,
        ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        ReconcileSnapshotTask.of("tbl-1", 77L, "src", "orders"),
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "tbl-1",
            77L,
            filePaths == null ? 0 : filePaths.size(),
            filePaths == null ? java.util.List.of() : filePaths),
        "");
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
