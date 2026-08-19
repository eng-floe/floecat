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
import static org.mockito.Mockito.never;
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
import ai.floedb.floecat.reconciler.impl.ReconcileLeaseGrpcStatus;
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
import java.util.concurrent.atomic.AtomicInteger;
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
    service.sourceCatalogVendor = new SourceCatalogCredentialVendor();
    service.sourceCatalogVendor.connectorRepo = connectorRepo;
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
  void updateRejectsNonConcreteS3AuthorityLocation() {
    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .updateStorageAuthority(
                        UpdateStorageAuthorityRequest.newBuilder()
                            .setAuthorityId(AUTHORITY_ID)
                            .setSpec(StorageAuthoritySpec.newBuilder().setLocationPrefix("s3://"))
                            .setUpdateMask(
                                FieldMask.newBuilder().addPaths("location_prefix").build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(repo, never()).update(any(StorageAuthority.class), anyLong());
  }

  @Test
  void updateRejectsUnsupportedAuthorityType() {
    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .updateStorageAuthority(
                        UpdateStorageAuthorityRequest.newBuilder()
                            .setAuthorityId(AUTHORITY_ID)
                            .setSpec(StorageAuthoritySpec.newBuilder().setType("gcs"))
                            .setUpdateMask(FieldMask.newBuilder().addPaths("type").build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(repo, never()).update(any(StorageAuthority.class), anyLong());
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
  void deleteRetriesAnUnconditionalConcurrentMutation() {
    AtomicInteger attempts = new AtomicInteger();
    when(repo.deleteWithPrecondition(eq(AUTHORITY_ID), anyLong()))
        .thenAnswer(
            _ -> {
              if (attempts.getAndIncrement() == 0) {
                version.incrementAndGet();
                return false;
              }
              return true;
            });

    service
        .deleteStorageAuthority(
            DeleteStorageAuthorityRequest.newBuilder().setAuthorityId(AUTHORITY_ID).build())
        .await()
        .indefinitely();

    verify(repo, times(2)).deleteWithPrecondition(eq(AUTHORITY_ID), anyLong());
    assertTrue(secretsManager.deleteCalled);
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
    assertEquals(
        "s3://floedb-databricks-metastore-367509577365",
        response.getStorageCredentials(0).getPrefix());
  }

  @Test
  void twoTablesCoveredByOneAuthorityUseOneAuthorityScopedCredential() {
    ResourceId customersTableId = TABLE_ID.toBuilder().setId("tbl-2").build();
    StorageAuthority bucketAuthority =
        currentAuthority().toBuilder()
            .setLocationPrefix("s3://warehouse/")
            .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
            .build();
    when(repo.list(eq("acct"), anyInt(), any(), any()))
        .thenReturn(java.util.List.of(bucketAuthority));
    when(tableRepo.getById(customersTableId))
        .thenReturn(
            Optional.of(
                ai.floedb.floecat.catalog.rpc.Table.newBuilder()
                    .setResourceId(customersTableId)
                    .putProperties("location", "s3://warehouse/customers")
                    .build()));
    java.util.List<String> policies = new java.util.ArrayList<>();
    service.resolver =
        new StorageAuthorityResolver() {
          @Override
          ai.floedb.floecat.connector.common.auth.ResolvedStorageCredentials
              assumeRoleFromStaticSource(
                  StorageAuthority authority, AuthCredentials.AwsCredentials source) {
            policies.add(
                StorageAuthorityResolver.scopedSessionPolicy(authority.getLocationPrefix()));
            return new ai.floedb.floecat.connector.common.auth.ResolvedStorageCredentials(
                "temp-akid",
                "temp-secret",
                "temp-token",
                java.time.Instant.now().plusSeconds(3600));
          }
        };
    service.resolver.secretsManager = secretsManager;

    ResolveStorageAuthorityResponse orders = vendServerCredentialsForTable(TABLE_ID);
    ResolveStorageAuthorityResponse customers = vendServerCredentialsForTable(customersTableId);

    assertEquals("s3://warehouse/", orders.getStorageCredentials(0).getPrefix());
    assertEquals("s3://warehouse/", customers.getStorageCredentials(0).getPrefix());
    assertEquals(1, policies.size(), "the authority-scoped cache must be shared across tables");
    assertTrue(policies.getFirst().contains("arn:aws:s3:::warehouse/*"));
    assertFalse(policies.getFirst().contains("orders"));
    assertFalse(policies.getFirst().contains("customers"));
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
  void executionBoundRequestSelectsLongestAuthorityMatchingRequestedLocation() {
    StorageAuthority dataAuthority =
        currentAuthority().toBuilder()
            .setResourceId(DATARBRICKS_AUTHORITY_ID)
            .setLocationPrefix("s3://warehouse/orders/data/")
            .build();
    when(repo.list(eq("acct"), anyInt(), any(), any()))
        .thenReturn(java.util.List.of(currentAuthority(), dataAuthority));

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
                                    .setLeaseEpoch("lease-1")))
                    .build())
            .await()
            .indefinitely();

    assertEquals(DATARBRICKS_AUTHORITY_ID, response.getAuthorityId());
    assertEquals("s3://warehouse/orders/data/", response.getStorageCredentials(0).getPrefix());
  }

  @Test
  void executionBoundParentRequestRemainsPinnedToLeasedTableLocation() {
    StorageAuthority bucketAuthority =
        currentAuthority().toBuilder()
            .setResourceId(DATARBRICKS_AUTHORITY_ID)
            .setLocationPrefix("s3://warehouse/")
            .build();
    when(repo.list(eq("acct"), anyInt(), any(), any()))
        .thenReturn(java.util.List.of(bucketAuthority, currentAuthority()));

    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setLocationPrefix("s3://warehouse")
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
    assertEquals("s3://warehouse/orders", response.getStorageCredentials(0).getPrefix());
  }

  @Test
  void resolveForAccountLocationMarksRejectedLeaseAsLeasePreconditionFailure() {
    when(reconcileJobs.renewLease("job-1", "lease-1")).thenReturn(false);

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://warehouse/orders/data/part-000.parquet")
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

    assertTrue(ReconcileLeaseGrpcStatus.isLeasePreconditionFailure(error));
  }

  /**
   * The reconcile worker vends by location and sends no {@code table_id}, so the leased job is the
   * only table identity available. Before this resolution existed the source-catalog fallback was
   * unreachable from the worker -- the one caller that needs it -- and every delegating catalog
   * failed with "no storage credential authority is configured".
   *
   * <p>Vending is driven to the missing-connector bail-out on purpose: it proves the leased table
   * was resolved and the fallback entered without needing a live catalog in a unit test.
   */
  @Test
  void executionBoundVendWithoutTableIdResolvesTheLeasedTable() {
    when(repo.list(eq("acct"), anyInt(), any(), any())).thenReturn(java.util.List.of());
    when(connectorRepo.getById(CONNECTOR_ID)).thenReturn(Optional.empty());

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://warehouse/orders/data/part-000.parquet")
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

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(connectorRepo).getById(CONNECTOR_ID);
  }

  /**
   * A lease with no table bound to it (a discovery planner job, before any table exists) must not
   * attempt source-catalog vending. Loading no table is the observable proof: the bootstrap scope
   * path never touches the table repository.
   */
  @Test
  void executionBoundVendWithoutALeasedTableSkipsSourceCatalog() {
    when(repo.list(eq("acct"), anyInt(), any(), any())).thenReturn(java.util.List.of());
    when(reconcileJobs.getLeaseView("job-1")).thenReturn(Optional.of(discoveryTableLeaseView()));

    assertThrows(
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
                                    ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding.newBuilder()
                                        .setJobId("job-1")
                                        .setLeaseEpoch("lease-1")))
                        .build())
                .await()
                .indefinitely());

    verify(tableRepo, org.mockito.Mockito.never()).getById(any());
  }

  /**
   * A valid lease for one table must not vend through another table's catalog.
   *
   * <p>The lease authorizes the location; preferring an explicit table_id let a caller keep that
   * location while naming a different table, so the fallback asked the wrong upstream connector and
   * returned credentials scoped to data the lease never covered.
   */
  @Test
  void explicitTableIdMayNotOverrideTheLeasedTable() {
    when(repo.list(eq("acct"), anyInt(), any(), any())).thenReturn(java.util.List.of());

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://warehouse/orders/data/part-000.parquet")
                            .setUsage(StorageCredentialUsage.SCU_SERVER)
                            // lease binds tbl-1; ask for a different table
                            .setTableId(
                                ResourceId.newBuilder()
                                    .setAccountId("acct")
                                    .setKind(ResourceKind.RK_TABLE)
                                    .setId("tbl-somebody-elses"))
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

  /**
   * The normal capture path sends both a lease and a matching table_id --
   * JavaConnectorCaptureEngine passes request.tableId() alongside the execution job, and both
   * derive from the same fileGroupTask. The guard above must not fire there, or it blocks capture
   * outright.
   */
  @Test
  void matchingExplicitTableIdIsAccepted() {
    when(repo.list(eq("acct"), anyInt(), any(), any())).thenReturn(java.util.List.of());
    when(connectorRepo.getById(CONNECTOR_ID)).thenReturn(Optional.empty());

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://warehouse/orders/data/part-000.parquet")
                            .setUsage(StorageCredentialUsage.SCU_SERVER)
                            // same table the lease binds
                            .setTableId(TABLE_ID)
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

    // Reached source-catalog vending, i.e. was NOT rejected as a table mismatch.
    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(connectorRepo).getById(CONNECTOR_ID);
  }

  /**
   * Credentials with no expiry are refused rather than installed.
   *
   * <p>The reconcile worker only registers a refresh provider when it can see an expiry; without
   * one it embeds the credentials statically and never re-vends, so they expire mid-read with no
   * recovery. Accepting them here would trade a clear failure at vend time for an opaque 403
   * partway through a file group.
   */
  private static ai.floedb.floecat.connector.spi.FloecatConnector.VendedStorageCredentials
      vendedTuple(String accessKey, String secret, String sessionToken, java.time.Instant expiry) {
    var props = new java.util.LinkedHashMap<String, String>();
    if (accessKey != null) {
      props.put("s3.access-key-id", accessKey);
    }
    if (secret != null) {
      props.put("s3.secret-access-key", secret);
    }
    if (sessionToken != null) {
      props.put("s3.session-token", sessionToken);
    }
    return new ai.floedb.floecat.connector.spi.FloecatConnector.VendedStorageCredentials(
        props, expiry);
  }

  private static final java.time.Instant EXPIRY = java.time.Instant.ofEpochMilli(1786000000000L);

  /**
   * Every field of the session tuple is required, not just the expiry. Access key plus secret plus
   * expiry but no session token satisfies isExecutionBoundStorageCredential yet fails
   * isRefreshableExecutionCredential, so the reconciler embeds it statically and never renews --
   * recreating the defect the expiry check exists to close.
   */
  @Test
  void incompleteVendedCredentialsAreRefused() {
    record Case(String name, String ak, String sk, String token, java.time.Instant expiry) {}
    var cases =
        java.util.List.of(
            new Case("no session token", "ASIA", "secret", null, EXPIRY),
            new Case("no secret", "ASIA", null, "token", EXPIRY),
            new Case("no access key", null, "secret", "token", EXPIRY),
            new Case("no expiry", "ASIA", "secret", "token", null),
            new Case("blank session token", "ASIA", "secret", "  ", EXPIRY));

    for (Case c : cases) {
      StatusRuntimeException error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  SourceCatalogCredentialVendor.requireUsableCredentials(
                      vendedTuple(c.ak(), c.sk(), c.token(), c.expiry()),
                      "tpch_10",
                      "customer",
                      SourceCatalogCredentialVendor.CredentialUse.RECONCILE),
              c.name());
      assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, error.getStatus().getCode(), c.name());
      // Terminal by structured reason, so the reconciler stops instead of retrying forever.
      assertTrue(
          ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus
              .isVendedCredentialsNotRefreshable(error),
          c.name());
    }
  }

  @Test
  void completeVendedCredentialsAreAccepted() {
    SourceCatalogCredentialVendor.requireUsableCredentials(
        vendedTuple("ASIA", "secret", "token", EXPIRY),
        "tpch_10",
        "customer",
        SourceCatalogCredentialVendor.CredentialUse.RECONCILE);
  }

  @Test
  void queryUseAcceptsCredentialsThatCannotBeRenewed() {
    // The renewal requirement is the reconcile worker's: it registers a refresh provider only when
    // it can see an expiry, and otherwise embeds credentials statically and never re-vends. The
    // query path hands them to the scan engine for reads that happen now and registers no provider,
    // so a missing session token or expiry says nothing about whether the read will work. Enforcing
    // it there would reject perfectly readable credentials -- with a terminal classification, on a
    // path where nothing retries and there is no job to fail.
    SourceCatalogCredentialVendor.requireUsableCredentials(
        vendedTuple("AKIA", "secret", null, null),
        "tpch_10",
        "customer",
        SourceCatalogCredentialVendor.CredentialUse.QUERY);
  }

  @Test
  void queryUseStillRequiresACompleteKeyPair() {
    // Relaxing renewal does not relax usability: without both halves of the key pair there is
    // nothing to read with, and the failure would otherwise surface as an opaque 403 mid-scan.
    record Missing(String name, String ak, String sk) {}
    for (var c :
        java.util.List.of(
            new Missing("no secret", "AKIA", null),
            new Missing("no access key", null, "secret"),
            new Missing("blank secret", "AKIA", "   "))) {
      assertThrows(
          StatusRuntimeException.class,
          () ->
              SourceCatalogCredentialVendor.requireUsableCredentials(
                  vendedTuple(c.ak(), c.sk(), null, null),
                  "tpch_10",
                  "customer",
                  SourceCatalogCredentialVendor.CredentialUse.QUERY),
          c.name());
    }
  }

  /**
   * Source-catalog vending is entered only for a connector that actually asked its catalog to
   * delegate. Presence of s3.access-key-id on a FileIO is not that signal -- a Glue or S3 Tables
   * connector using static aws credentials carries it too -- so the gate keys on the declared
   * delegation intent.
   */
  @Test
  void connectorDeclaresVendedDelegationOnlyForVendedCredentialsHeader() {
    assertTrue(
        SourceCatalogCredentialVendor.connectorDeclaresVendedDelegation(
            delegationConnector("vended-credentials")));
    assertFalse(
        SourceCatalogCredentialVendor.connectorDeclaresVendedDelegation(
            delegationConnector("remote-signing")));
    assertFalse(
        SourceCatalogCredentialVendor.connectorDeclaresVendedDelegation(discoveryConnector()));
  }

  private static Connector delegationConnector(String delegationValue) {
    return Connector.newBuilder()
        .setResourceId(CONNECTOR_ID)
        .setKind(ConnectorKind.CK_ICEBERG)
        .setState(ConnectorState.CS_ACTIVE)
        .putProperties("iceberg.source", "rest")
        .putProperties("header.X-Iceberg-Access-Delegation", delegationValue)
        .build();
  }

  /** Only the non-secret routing keys reach client_safe_config; credentials never do. */
  @Test
  void clientSafeRoutingPropertiesKeepsOnlyNonSecretRoutingKeys() {
    var routing =
        SourceCatalogCredentialVendor.clientSafeRoutingProperties(
            java.util.Map.of(
                "s3.access-key-id", "ASIA",
                "s3.secret-access-key", "secret",
                "s3.session-token", "token",
                "s3.region", "eu-west-1",
                "s3.endpoint", "https://s3.eu-west-1.example",
                "s3.path-style-access", "true"));

    assertEquals("eu-west-1", routing.get("s3.region"));
    assertEquals("https://s3.eu-west-1.example", routing.get("s3.endpoint"));
    assertEquals("true", routing.get("s3.path-style-access"));
    assertFalse(routing.containsKey("s3.access-key-id"));
    assertFalse(routing.containsKey("s3.secret-access-key"));
    assertFalse(routing.containsKey("s3.session-token"));
  }

  @Test
  void vendedRoutingCarriesARegionUnderEveryAliasTheAuthorityPathEmits() {
    // Polaris vends the session triple and no region at all. The reconcile worker has a default
    // and survived; the query scan engine has none and failed the whole scan with "region is
    // missing" after planning had already succeeded. The aliases matter as much as the value --
    // an authority-backed response has always carried all three, and consumers read different ones.
    var vendor = new SourceCatalogCredentialVendor();
    vendor.defaultRegion = "us-east-1";

    var routing =
        vendor.routingProperties(
            java.util.Map.of(
                "s3.access-key-id", "ASIA",
                "s3.secret-access-key", "secret",
                "s3.session-token", "token"),
            java.util.Map.of());

    assertEquals("us-east-1", routing.get("s3.region"));
    assertEquals("us-east-1", routing.get("region"));
    assertEquals("us-east-1", routing.get("client.region"));
    assertFalse(routing.containsKey("s3.access-key-id"));
  }

  @Test
  void vendedRegionWinsOverConnectorAndDeploymentDefault() {
    var vendor = new SourceCatalogCredentialVendor();
    vendor.defaultRegion = "us-east-1";

    var routing =
        vendor.routingProperties(
            java.util.Map.of("s3.region", "eu-west-1"),
            java.util.Map.of("s3.region", "ap-south-1"));

    assertEquals("eu-west-1", routing.get("s3.region"));
    assertEquals("eu-west-1", routing.get("client.region"));
  }

  @Test
  void connectorRegionIsUsedWhenTheCatalogVendsNone() {
    var vendor = new SourceCatalogCredentialVendor();
    vendor.defaultRegion = "us-east-1";

    var routing =
        vendor.routingProperties(
            java.util.Map.of(), java.util.Map.of("client.region", "eu-west-2"));

    assertEquals("eu-west-2", routing.get("s3.region"));
    assertEquals("eu-west-2", routing.get("region"));
  }

  @Test
  void endpointIsNeverSynthesizedFromDeploymentStorageSettings() {
    // Region has no safe absent value, so it is defaulted. An endpoint does: absent means standard
    // AWS S3. Defaulting it from floecat's own storage config would point reads of a real S3
    // warehouse at floecat's blob store -- LocalStack in dev.
    var vendor = new SourceCatalogCredentialVendor();
    vendor.defaultRegion = "us-east-1";

    var routing = vendor.routingProperties(java.util.Map.of(), java.util.Map.of());

    assertFalse(routing.containsKey("s3.endpoint"));
    assertFalse(routing.containsKey("s3.path-style-access"));
  }

  /**
   * A PLAN_VIEW bootstrap lease scopes credentials by location, never validating request.table_id
   * against the lease. Source-catalog vending must therefore not be derived from a caller-supplied
   * table on that path, or a view-lease holder could steer which catalog vends -- the confused
   * deputy the leased-table path was built to prevent. Proof: the caller's table is never loaded.
   */
  @Test
  void planViewLeaseCannotVendFromCallerSuppliedTable() {
    when(repo.list(eq("acct"), anyInt(), any(), any())).thenReturn(java.util.List.of());
    when(reconcileJobs.getLeaseView("job-1")).thenReturn(Optional.of(viewLeaseView()));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://warehouse/orders/metadata/v1.json")
                            .setUsage(StorageCredentialUsage.SCU_SERVER)
                            .setTableId(
                                ResourceId.newBuilder()
                                    .setAccountId("acct")
                                    .setKind(ResourceKind.RK_TABLE)
                                    .setId("tbl-somebody-elses"))
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

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    verify(tableRepo, org.mockito.Mockito.never()).getById(any());
  }

  /**
   * Registering files in place (add_files) leaves an Iceberg table's data outside its own location,
   * so scoping a lease to the table location alone rejects the very files the lease planned. A
   * leased file must be readable even from an unrelated prefix.
   */
  @Test
  void leasedFileOutsideTheTableLocationIsInScope() {
    when(repo.list(eq("acct"), anyInt(), any(), any())).thenReturn(java.util.List.of());
    when(connectorRepo.getById(CONNECTOR_ID)).thenReturn(Optional.empty());
    when(reconcileJobs.getLeaseView("job-1"))
        .thenReturn(
            Optional.of(
                activeLeaseView(
                    "job-1",
                    "acct",
                    "JS_RUNNING",
                    java.util.List.of("s3://elsewhere/registered/part-000.parquet"))));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://elsewhere/registered/part-000.parquet")
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

    // Admitted: it reached authority resolution. Had the location been out of scope this would be
    // PERMISSION_DENIED instead.
    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    assertTrue(
        ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus
            .isNoMatchingStorageAuthority(error));
    // Source-catalog vending is deliberately not consulted. Exact leased-file membership
    // authorizes the request, but an upstream catalog chooses its own credential scope and Floecat
    // must not reinterpret table-scoped credentials as a single-file grant.
    verify(connectorRepo, never()).getById(CONNECTOR_ID);
  }

  @Test
  void icebergExternalLeasedFileUsesMatchingAuthorityPrefix() {
    String externalFile = "s3://elsewhere/registered/part-000.parquet";
    StorageAuthority externalAuthority =
        currentAuthority().toBuilder().setLocationPrefix("s3://elsewhere/").build();
    when(repo.list(eq("acct"), anyInt(), any(), any()))
        .thenReturn(java.util.List.of(externalAuthority));
    when(reconcileJobs.getLeaseView("job-1"))
        .thenReturn(
            Optional.of(
                activeLeaseView("job-1", "acct", "JS_RUNNING", java.util.List.of(externalFile))));

    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setLocationPrefix(externalFile)
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
    assertEquals("s3://elsewhere/", response.getStorageCredentials(0).getPrefix());
  }

  @Test
  void deltaDeletionVectorSiblingUsesBucketAuthorityPrefix() {
    String deletionVector =
        "s3://floedb-databricks-metastore-367509577365/metastore/metastore-id/tables/deletion_vector_uuid.bin";
    StorageAuthority databricksAuthority =
        currentAuthority().toBuilder()
            .setResourceId(DATARBRICKS_AUTHORITY_ID)
            .setLocationPrefix("s3://floedb-databricks-metastore-367509577365/")
            .build();
    when(repo.list(eq("acct"), anyInt(), any(), any()))
        .thenReturn(java.util.List.of(databricksAuthority));
    when(reconcileJobs.getLeaseView("job-1"))
        .thenReturn(
            Optional.of(
                activeLeaseView("job-1", "acct", "JS_RUNNING", java.util.List.of(deletionVector))));

    ResolveStorageAuthorityResponse response =
        service
            .vendStorageCredentials(
                VendStorageCredentialsRequest.newBuilder()
                    .setAccountId("acct")
                    .setLocationPrefix(deletionVector)
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

    assertEquals(DATARBRICKS_AUTHORITY_ID, response.getAuthorityId());
    assertEquals(
        "s3://floedb-databricks-metastore-367509577365/",
        response.getStorageCredentials(0).getPrefix());
  }

  /**
   * The leased-file allowance is exact membership. Asking for the parent prefix of a leased file
   * would hand out credentials for everything beside it, so it stays denied.
   */
  @Test
  void parentPrefixOfALeasedFileIsStillOutOfScope() {
    when(reconcileJobs.getLeaseView("job-1"))
        .thenReturn(
            Optional.of(
                activeLeaseView(
                    "job-1",
                    "acct",
                    "JS_RUNNING",
                    java.util.List.of("s3://elsewhere/registered/part-000.parquet"))));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://elsewhere/registered/")
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
  void resolveForStrictTableWithoutDestinationDoesNotUseDiscoveryScope() {
    when(reconcileJobs.getLeaseView("job-1"))
        .thenReturn(Optional.of(strictUnboundTableLeaseView()));

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

    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    assertFalse(ReconcileLeaseGrpcStatus.isLeasePreconditionFailure(error));
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
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .vendStorageCredentials(
                        VendStorageCredentialsRequest.newBuilder()
                            .setAccountId("acct")
                            .setLocationPrefix("s3://elsewhere/data/part-000.parquet")
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
            () -> resolver.mintTemporaryCredentials(authority, temporaryCredentials));

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

  private ResolveStorageAuthorityResponse vendServerCredentialsForTable(ResourceId tableId) {
    return service
        .vendStorageCredentials(
            VendStorageCredentialsRequest.newBuilder()
                .setAccountId("acct")
                .setTableId(tableId)
                .setUsage(StorageCredentialUsage.SCU_SERVER)
                .build())
        .await()
        .indefinitely();
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

  private static ReconcileJobStore.ReconcileJob strictUnboundTableLeaseView() {
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
        ReconcileTableTask.of("src", "orders"),
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
