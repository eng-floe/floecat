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

package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.SketchPayload;
import ai.floedb.floecat.catalog.rpc.SketchRole;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.StandaloneFileGroupExecutionPayload;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultPayload;
import ai.floedb.floecat.reconciler.rpc.FileGroupStatsPayload;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.stats.spi.StatsStore;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class LeasedFileGroupExecutionServiceTest {
  private static final String ACCOUNT_ID = "acct";
  private static final String CONNECTOR_ID = "conn";
  private static final String PARENT_JOB_ID = "parent-job";
  private static final String CHILD_JOB_ID = "child-job";
  private static final String LEASE_EPOCH = "lease-1";
  private static final String TABLE_ID = "table-1";
  private static final long SNAPSHOT_ID = 55L;

  private LeasedFileGroupExecutionService service;
  private ReconcileJobStore jobs;
  private TableRepository tableRepo;
  private ConnectorRepository connectorRepo;
  private SnapshotRepository snapshotRepo;
  private CredentialResolver credentialResolver;
  private IdempotencyRepository idempotencyStore;
  private StatsStore statsStore;
  private StatsOrchestrator statsOrchestrator;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedFileGroupExecutionService();
    jobs = mock(ReconcileJobStore.class);
    tableRepo = mock(TableRepository.class);
    connectorRepo = mock(ConnectorRepository.class);
    snapshotRepo = mock(SnapshotRepository.class);
    credentialResolver = mock(CredentialResolver.class);
    idempotencyStore = mock(IdempotencyRepository.class);
    statsStore = mock(StatsStore.class);
    statsOrchestrator = mock(StatsOrchestrator.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.tableRepo = tableRepo;
    service.connectorRepo = connectorRepo;
    service.snapshotRepo = snapshotRepo;
    service.credentialResolver = credentialResolver;
    service.idempotencyStore = idempotencyStore;
    service.statsStore = statsStore;
    service.statsOrchestrator = statsOrchestrator;
    service.blobStore = mock(ai.floedb.floecat.storage.spi.BlobStore.class);
    when(principal.getCorrelationId()).thenReturn("corr");
    when(principal.getAccountId()).thenReturn(ACCOUNT_ID);
    when(service.blobStore.head(resultPayloadUri()))
        .thenReturn(
            Optional.of(
                ai.floedb.floecat.common.rpc.BlobHeader.newBuilder()
                    .setContentLength(100L)
                    .build()));
    when(idempotencyStore.get(anyString())).thenReturn(Optional.empty());
    when(idempotencyStore.createPending(
            anyString(), anyString(), anyString(), anyString(), any(), any()))
        .thenReturn(true);
    when(jobs.completeFileGroupSuccess(
            anyString(),
            anyString(),
            any(ReconcileFileGroupResultDescriptor.class),
            anyLong(),
            anyString()))
        .thenReturn(true);
  }

  @Test
  void resolveUsesParentSnapshotTaskFileGroupsFromDurableJobView() {
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));

    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    CHILD_JOB_ID,
                    ReconcileJobKind.EXEC_FILE_GROUP,
                    ReconcileSnapshotTask.empty(),
                    group.asReference(),
                    PARENT_JOB_ID)));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    PARENT_JOB_ID,
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ReconcileSnapshotTask.of(
                        TABLE_ID,
                        SNAPSHOT_ID,
                        "db",
                        "events",
                        List.of(group),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(tableRepo.getById(tableId())).thenReturn(Optional.of(table()));
    when(connectorRepo.getById(connectorId())).thenReturn(Optional.of(connector()));

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    assertEquals("plan-1", payload.planId());
    assertEquals("group-1", payload.groupId());
    assertEquals(List.of("s3://bucket/data/file-1.parquet"), payload.plannedFilePaths());
  }

  @Test
  void persistSuccessReadsManifestPayloadAtCompletion() {
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileJobStore.ReconcileJob childLeaseView =
        job(
            CHILD_JOB_ID,
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileSnapshotTask.empty(),
            plannedGroup.asReference(),
            PARENT_JOB_ID);
    ReconcileJobStore.ReconcileJob parent =
        job(
            PARENT_JOB_ID,
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileSnapshotTask.of(
                TABLE_ID,
                SNAPSHOT_ID,
                "db",
                "events",
                List.of(plannedGroup),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                1),
            ReconcileFileGroupTask.empty(),
            "");
    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID)).thenReturn(Optional.of(parent));
    when(jobs.get(ACCOUNT_ID, CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));

    boolean accepted =
        service.persistSuccess(
            principal, CHILD_JOB_ID, LEASE_EPOCH, "result-1", resultDescriptor(List.of()));

    assertTrue(accepted);
    ArgumentCaptor<ReconcileFileGroupResultDescriptor> persisted =
        ArgumentCaptor.forClass(ReconcileFileGroupResultDescriptor.class);
    verify(jobs)
        .completeFileGroupSuccess(
            eq(CHILD_JOB_ID),
            eq(LEASE_EPOCH),
            persisted.capture(),
            anyLong(),
            eq("Executed file group group-1"));
    assertEquals(1, persisted.getValue().plannedFileCount());
    assertEquals(1, persisted.getValue().succeededFileCount());
    assertEquals(resultPayloadUri(), persisted.getValue().payloadUri());
  }

  @Test
  void persistSuccessFullWritesFileStatsToDraftGeneration() {
    TargetStatsRecord fileStats = fileStatsRecord("s3://bucket/data/file-1.parquet", 3L);
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileJobStore.ReconcileJob childLeaseView =
        job(
            CHILD_JOB_ID,
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileSnapshotTask.empty(),
            plannedGroup.asReference(),
            PARENT_JOB_ID,
            true);
    ReconcileJobStore.ReconcileJob parent =
        job(
            PARENT_JOB_ID,
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileSnapshotTask.of(
                TABLE_ID,
                SNAPSHOT_ID,
                "db",
                "events",
                List.of(plannedGroup),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                1),
            ReconcileFileGroupTask.empty(),
            "",
            true);
    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID)).thenReturn(Optional.of(parent));
    when(jobs.get(ACCOUNT_ID, CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    publishStatsPayload(List.of(fileStats));

    boolean accepted =
        service.persistSuccess(
            principal, CHILD_JOB_ID, LEASE_EPOCH, "result-1", resultDescriptor(List.of(fileStats)));

    assertTrue(accepted);
    verify(statsStore)
        .replaceTargetStatsInGeneration(
            eq(tableId()),
            eq(SNAPSHOT_ID),
            eq("full-rescan-" + PARENT_JOB_ID),
            eq(List.of(fileStats.getTarget())),
            eq(List.of(fileStats)));
    verify(statsStore, never()).putTargetStatsIfAbsent(any());
  }

  @Test
  void persistSuccessFullRejectsFileStatsOutsidePlannedGroup() {
    TargetStatsRecord fileStats = fileStatsRecord("s3://bucket/data/file-2.parquet", 3L);
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileJobStore.ReconcileJob childLeaseView =
        job(
            CHILD_JOB_ID,
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileSnapshotTask.empty(),
            plannedGroup.asReference(),
            PARENT_JOB_ID,
            true);
    ReconcileJobStore.ReconcileJob parent =
        job(
            PARENT_JOB_ID,
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileSnapshotTask.of(
                TABLE_ID,
                SNAPSHOT_ID,
                "db",
                "events",
                List.of(plannedGroup),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                1),
            ReconcileFileGroupTask.empty(),
            "",
            true);
    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID)).thenReturn(Optional.of(parent));
    when(jobs.get(ACCOUNT_ID, CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    publishStatsPayload(List.of(fileStats));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            service.persistSuccess(
                principal,
                CHILD_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                resultDescriptor(List.of(fileStats))));
    verify(statsStore, never())
        .replaceTargetStatsInGeneration(any(), anyLong(), anyString(), any(), any());
  }

  @Test
  void statsGenerationIdRequiresParentJobId() {
    ReconcileJobStore.LeasedJob lease =
        new ReconcileJobStore.LeasedJob(
            CHILD_JOB_ID,
            ACCOUNT_ID,
            CONNECTOR_ID,
            true,
            CaptureMode.METADATA_ONLY,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            LEASE_EPOCH,
            "",
            "executor",
            ReconcileJobKind.EXEC_FILE_GROUP,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.empty(),
            "");

    assertThrows(
        IllegalArgumentException.class,
        () -> LeasedFileGroupExecutionService.statsGenerationId(lease));
  }

  @Test
  void persistSuccessDoesNotFinalizeIdempotencyWhenLeaseOutcomeRejected() {
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileJobStore.ReconcileJob childLeaseView =
        job(
            CHILD_JOB_ID,
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileSnapshotTask.empty(),
            plannedGroup.asReference(),
            PARENT_JOB_ID);
    ReconcileJobStore.ReconcileJob parent =
        job(
            PARENT_JOB_ID,
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileSnapshotTask.of(
                TABLE_ID,
                SNAPSHOT_ID,
                "db",
                "events",
                List.of(plannedGroup),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                1),
            ReconcileFileGroupTask.empty(),
            "");
    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID)).thenReturn(Optional.of(parent));
    when(jobs.get(ACCOUNT_ID, CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    publishStatsPayload(List.of());
    when(jobs.completeFileGroupSuccess(
            anyString(),
            anyString(),
            any(ReconcileFileGroupResultDescriptor.class),
            anyLong(),
            anyString()))
        .thenReturn(false);

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service.persistSuccess(
                    principal, CHILD_JOB_ID, LEASE_EPOCH, "result-1", resultDescriptor(List.of())));

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    verify(idempotencyStore, never())
        .finalizeSuccess(
            anyString(), anyString(), anyString(), anyString(), any(), any(), any(), any(), any());
  }

  @Test
  void resolveFailsWhenParentSnapshotTaskDoesNotContainPlannedGroup() {
    ReconcileFileGroupTask childRef =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, 1, List.of(), List.of());

    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    CHILD_JOB_ID,
                    ReconcileJobKind.EXEC_FILE_GROUP,
                    ReconcileSnapshotTask.empty(),
                    childRef,
                    PARENT_JOB_ID)));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    PARENT_JOB_ID,
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ReconcileSnapshotTask.of(
                        TABLE_ID,
                        SNAPSHOT_ID,
                        "db",
                        "events",
                        List.of(),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH));

    assertEquals(
        "FAILED_PRECONDITION: planned file group could not be resolved from parent snapshot plan",
        error.getMessage());
  }

  @Test
  void resolvePreservesCapturePolicyForCaptureModeExecFileGroup() {
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileScope scopedCapture =
        ReconcileScope.of(
            List.of(),
            TABLE_ID,
            null,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(new ReconcileCapturePolicy.Column("col_a", true, false)),
                java.util.Set.of(
                    ReconcileCapturePolicy.Output.FILE_STATS,
                    ReconcileCapturePolicy.Output.COLUMN_STATS)));

    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    CHILD_JOB_ID,
                    ReconcileJobKind.EXEC_FILE_GROUP,
                    ReconcileSnapshotTask.empty(),
                    group.asReference(),
                    PARENT_JOB_ID,
                    CaptureMode.METADATA_AND_CAPTURE,
                    scopedCapture)));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    PARENT_JOB_ID,
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ReconcileSnapshotTask.of(
                        TABLE_ID,
                        SNAPSHOT_ID,
                        "db",
                        "events",
                        List.of(group),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "",
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty())));
    when(tableRepo.getById(tableId())).thenReturn(Optional.of(table()));
    when(connectorRepo.getById(connectorId())).thenReturn(Optional.of(connector()));

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    assertEquals(scopedCapture.capturePolicy(), payload.capturePolicy());
  }

  @Test
  void resolveAddsTableStorageLocationHintToDeltaConnectorPayload() {
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));

    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    CHILD_JOB_ID,
                    ReconcileJobKind.EXEC_FILE_GROUP,
                    ReconcileSnapshotTask.empty(),
                    group.asReference(),
                    PARENT_JOB_ID)));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    PARENT_JOB_ID,
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ReconcileSnapshotTask.of(
                        TABLE_ID,
                        SNAPSHOT_ID,
                        "db",
                        "events",
                        List.of(group),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(tableRepo.getById(tableId()))
        .thenReturn(
            Optional.of(
                table().toBuilder()
                    .putProperties("storage_location", "s3://bucket/table")
                    .build()));
    when(connectorRepo.getById(connectorId()))
        .thenReturn(Optional.of(connector().toBuilder().setKind(ConnectorKind.CK_DELTA).build()));

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    assertEquals(
        "s3://bucket/table", payload.sourceConnector().getPropertiesOrThrow("storage_location"));
  }

  @Test
  void resolveDerivesIcebergStorageLocationFromCurrentSnapshotMetadata() {
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));

    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    CHILD_JOB_ID,
                    ReconcileJobKind.EXEC_FILE_GROUP,
                    ReconcileSnapshotTask.empty(),
                    group.asReference(),
                    PARENT_JOB_ID)));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    PARENT_JOB_ID,
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ReconcileSnapshotTask.of(
                        TABLE_ID,
                        SNAPSHOT_ID,
                        "db",
                        "events",
                        List.of(group),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(tableRepo.getById(tableId())).thenReturn(Optional.of(table()));
    when(connectorRepo.getById(connectorId()))
        .thenReturn(Optional.of(connector().toBuilder().setKind(ConnectorKind.CK_ICEBERG).build()));
    when(snapshotRepo.latestRegisteredSnapshot(tableId()))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setMetadataLocation(
                        "s3://bucket/warehouse/orders/metadata/00001.metadata.json")
                    .build()));

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    assertEquals("s3://bucket/warehouse/orders", payload.storageLocation());
  }

  @Test
  void resolveAddsResolvedDeltaStorageOptionsToConnectorPayload() {
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));

    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    CHILD_JOB_ID,
                    ReconcileJobKind.EXEC_FILE_GROUP,
                    ReconcileSnapshotTask.empty(),
                    group.asReference(),
                    PARENT_JOB_ID)));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    PARENT_JOB_ID,
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ReconcileSnapshotTask.of(
                        TABLE_ID,
                        SNAPSHOT_ID,
                        "db",
                        "events",
                        List.of(group),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(tableRepo.getById(tableId()))
        .thenReturn(
            Optional.of(
                table().toBuilder()
                    .putProperties("storage_location", "s3://bucket/table")
                    .build()));
    when(connectorRepo.getById(connectorId()))
        .thenReturn(
            Optional.of(
                connector().toBuilder()
                    .setKind(ConnectorKind.CK_DELTA)
                    .putProperties("s3.endpoint", "http://localstack:4566")
                    .putProperties("s3.path-style-access", "true")
                    .setAuth(
                        AuthConfig.newBuilder()
                            .setScheme("none")
                            .setCredentials(
                                AuthCredentials.newBuilder()
                                    .setAws(
                                        AuthCredentials.AwsCredentials.newBuilder()
                                            .setAccessKeyId("test-access")
                                            .setSecretAccessKey("test-secret")
                                            .setSessionToken("test-token")))
                            .build())
                    .build()));

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    assertEquals(
        "http://localstack:4566", payload.sourceConnector().getPropertiesOrThrow("s3.endpoint"));
    assertEquals("true", payload.sourceConnector().getPropertiesOrThrow("s3.path-style-access"));
    assertEquals("test-access", payload.sourceConnector().getPropertiesOrThrow("s3.access-key-id"));
    assertEquals(
        "test-secret", payload.sourceConnector().getPropertiesOrThrow("s3.secret-access-key"));
    assertEquals("test-token", payload.sourceConnector().getPropertiesOrThrow("s3.session-token"));
  }

  @Test
  void prepareIndexArtifactRecordSkipsBlobWriteWhenArtifactWasAlreadyUploaded() throws Exception {
    service.blobStore = mock(ai.floedb.floecat.storage.spi.BlobStore.class);

    IndexArtifactRecord record =
        IndexArtifactRecord.newBuilder()
            .setTableId(tableId())
            .setSnapshotId(SNAPSHOT_ID)
            .setTarget(
                IndexTarget.newBuilder()
                    .setFile(IndexFileTarget.newBuilder().setFilePath("s3://bucket/file-1.parquet"))
                    .build())
            .setArtifactUri("s3://floescan-sidecars/acct/table-1/55/file-1.parquet")
            .setArtifactFormat("parquet")
            .setArtifactFormatVersion(1)
            .setState(IndexArtifactState.IAS_READY)
            .build();
    when(service.blobStore.head(record.getArtifactUri()))
        .thenReturn(
            Optional.of(
                ai.floedb.floecat.common.rpc.BlobHeader.newBuilder()
                    .setEtag("etag-1")
                    .setContentLength(128L)
                    .build()));

    IndexArtifactRecord prepared =
        invokePrepareIndexArtifactRecord(
            tableId(),
            SNAPSHOT_ID,
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/file-1.parquet")),
            record);

    verify(service.blobStore, never()).put(anyString(), any(byte[].class), anyString());
    verify(service.blobStore).head(record.getArtifactUri());
    assertEquals(record.toBuilder().setContentEtag("etag-1").build(), prepared);
  }

  private IndexArtifactRecord invokePrepareIndexArtifactRecord(
      ResourceId tableId,
      long snapshotId,
      ReconcileFileGroupTask plannedTask,
      IndexArtifactRecord record)
      throws Exception {
    Method method =
        LeasedFileGroupExecutionService.class.getDeclaredMethod(
            "prepareIndexArtifactRecord",
            ResourceId.class,
            long.class,
            ReconcileFileGroupTask.class,
            IndexArtifactRecord.class);
    method.setAccessible(true);
    return (IndexArtifactRecord) method.invoke(service, tableId, snapshotId, plannedTask, record);
  }

  private static ReconcileJobStore.ReconcileJob job(
      String jobId,
      ReconcileJobKind kind,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      String parentJobId) {
    return job(jobId, kind, snapshotTask, fileGroupTask, parentJobId, false);
  }

  private static ReconcileJobStore.ReconcileJob job(
      String jobId,
      ReconcileJobKind kind,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      String parentJobId,
      boolean fullRescan) {
    return job(
        jobId,
        kind,
        snapshotTask,
        fileGroupTask,
        parentJobId,
        fullRescan,
        CaptureMode.METADATA_ONLY,
        ReconcileScope.empty());
  }

  private static ReconcileJobStore.ReconcileJob job(
      String jobId,
      ReconcileJobKind kind,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      String parentJobId,
      CaptureMode captureMode,
      ReconcileScope scope) {
    return job(jobId, kind, snapshotTask, fileGroupTask, parentJobId, false, captureMode, scope);
  }

  private static ReconcileJobStore.ReconcileJob job(
      String jobId,
      ReconcileJobKind kind,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      String parentJobId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope) {
    return new ReconcileJobStore.ReconcileJob(
        jobId,
        ACCOUNT_ID,
        CONNECTOR_ID,
        "JS_RUNNING",
        "Running",
        1L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        fullRescan,
        captureMode,
        0L,
        0L,
        0L,
        false,
        scope,
        ReconcileExecutionPolicy.defaults(),
        "",
        "remote_file_group_worker",
        kind,
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        snapshotTask,
        fileGroupTask,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        parentJobId);
  }

  private static TargetStatsRecord fileStatsRecord(String filePath, long rowCount) {
    return TargetStatsRecord.newBuilder()
        .setTableId(tableId())
        .setSnapshotId(SNAPSHOT_ID)
        .setTarget(
            StatsTarget.newBuilder().setFile(FileStatsTarget.newBuilder().setFilePath(filePath)))
        .setFile(FileTargetStats.newBuilder().setFilePath(filePath).setRowCount(rowCount))
        .build();
  }

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId(ACCOUNT_ID)
        .setKind(ResourceKind.RK_TABLE)
        .setId(TABLE_ID)
        .build();
  }

  private static SketchPayload sketch(SketchRole role, String sketchType) {
    return SketchPayload.newBuilder()
        .setRole(role)
        .setSketchType(sketchType)
        .setData(ByteString.copyFrom(new byte[] {1, 2, 3}))
        .build();
  }

  private static String resultPayloadUri() {
    return Keys.reconcileFileGroupResultPayloadUri(
        ACCOUNT_ID, PARENT_JOB_ID, CHILD_JOB_ID, LEASE_EPOCH);
  }

  private static String statsPayloadUri() {
    return Keys.reconcileFileGroupStatsPayloadUri(
        ACCOUNT_ID, PARENT_JOB_ID, CHILD_JOB_ID, LEASE_EPOCH);
  }

  private byte[] publishStatsPayload(List<TargetStatsRecord> fileStats) {
    byte[] bytes =
        FileGroupStatsPayload.newBuilder()
            .setFormatVersion(1)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId(CONNECTOR_ID)
            .setParentJobId(PARENT_JOB_ID)
            .setFileGroupJobId(CHILD_JOB_ID)
            .setPlanId("plan-1")
            .setGroupId("group-1")
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(LEASE_EPOCH)
            .setResultId("result-1")
            .addAllFileStats(fileStats)
            .build()
            .toByteArray();
    when(service.blobStore.head(statsPayloadUri()))
        .thenReturn(
            Optional.of(
                ai.floedb.floecat.common.rpc.BlobHeader.newBuilder()
                    .setContentLength(bytes.length)
                    .build()));
    when(service.blobStore.get(statsPayloadUri())).thenReturn(bytes);
    return bytes;
  }

  private ReconcileFileGroupResultDescriptor resultDescriptor(List<TargetStatsRecord> fileStats) {
    byte[] resultBytes = publishResultPayload();
    byte[] statsBytes = publishStatsPayload(fileStats);
    return new ReconcileFileGroupResultDescriptor(
        1,
        ACCOUNT_ID,
        CONNECTOR_ID,
        PARENT_JOB_ID,
        CHILD_JOB_ID,
        "plan-1",
        "group-1",
        TABLE_ID,
        SNAPSHOT_ID,
        LEASE_EPOCH,
        "result-1",
        resultPayloadUri(),
        resultBytes.length,
        Base64.getEncoder().encodeToString(sha256(resultBytes)),
        1,
        1,
        0,
        0,
        0,
        0,
        statsPayloadUri(),
        statsBytes.length,
        Base64.getEncoder().encodeToString(sha256(statsBytes)),
        fileStats.size(),
        1L);
  }

  private byte[] publishResultPayload() {
    byte[] bytes =
        FileGroupResultPayload.newBuilder()
            .setFormatVersion(1)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId(CONNECTOR_ID)
            .setParentJobId(PARENT_JOB_ID)
            .setFileGroupJobId(CHILD_JOB_ID)
            .setPlanId("plan-1")
            .setGroupId("group-1")
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(LEASE_EPOCH)
            .setResultId("result-1")
            .addFileResults(
                ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.newBuilder()
                    .setFilePath("s3://bucket/data/file-1.parquet")
                    .setState(
                        ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_SUCCEEDED))
            .build()
            .toByteArray();
    when(service.blobStore.head(resultPayloadUri()))
        .thenReturn(
            Optional.of(
                ai.floedb.floecat.common.rpc.BlobHeader.newBuilder()
                    .setContentLength(bytes.length)
                    .build()));
    when(service.blobStore.get(resultPayloadUri())).thenReturn(bytes);
    return bytes;
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static ResourceId connectorId() {
    return ResourceId.newBuilder()
        .setAccountId(ACCOUNT_ID)
        .setKind(ResourceKind.RK_CONNECTOR)
        .setId(CONNECTOR_ID)
        .build();
  }

  private static Table table() {
    return Table.newBuilder()
        .setResourceId(tableId())
        .setUpstream(
            UpstreamRef.newBuilder()
                .setConnectorId(connectorId())
                .setTableDisplayName("events")
                .addNamespacePath("db")
                .build())
        .build();
  }

  private static Connector connector() {
    return Connector.newBuilder()
        .setResourceId(connectorId())
        .setKind(ConnectorKind.CK_ICEBERG)
        .setAuth(AuthConfig.getDefaultInstance())
        .build();
  }
}
