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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.PutIndexArtifactItem;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
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
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.LeasedFileGroupIndexArtifact;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
  private SnapshotFinalizePersistenceService snapshotFinalizePersistence;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedFileGroupExecutionService();
    jobs = mock(ReconcileJobStore.class);
    tableRepo = mock(TableRepository.class);
    connectorRepo = mock(ConnectorRepository.class);
    snapshotRepo = mock(SnapshotRepository.class);
    credentialResolver = mock(CredentialResolver.class);
    snapshotFinalizePersistence = mock(SnapshotFinalizePersistenceService.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.tableRepo = tableRepo;
    service.connectorRepo = connectorRepo;
    service.snapshotRepo = snapshotRepo;
    service.credentialResolver = credentialResolver;
    service.snapshotFinalizePersistence = snapshotFinalizePersistence;
    when(principal.getCorrelationId()).thenReturn("corr");
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
  void mergePersistedChildResultPreservesChildPartialsWhileHydratingPlannedFilePaths() {
    TargetStatsRecord partialAggregate =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId())
            .setSnapshotId(SNAPSHOT_ID)
            .setTarget(
                StatsTarget.newBuilder()
                    .setTable(ai.floedb.floecat.catalog.rpc.TableStatsTarget.getDefaultInstance())
                    .build())
            .setTable(TableValueStats.newBuilder().setRowCount(1L).build())
            .build();
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileFileGroupTask persistedChild =
        group.withPartialAggregateRecords(List.of(partialAggregate));

    ReconcileFileGroupTask merged =
        LeasedFileGroupExecutionService.mergePersistedChildResult(group, persistedChild);

    assertEquals(List.of("s3://bucket/data/file-1.parquet"), merged.filePaths());
    assertEquals(List.of(partialAggregate), merged.partialAggregateRecords());
  }

  @Test
  void mergedPartialAggregatesMergesPriorAggregateStateWithNewChunkOnly() throws Exception {
    TargetStatsRecord previousAggregate =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId())
            .setSnapshotId(SNAPSHOT_ID)
            .setTarget(
                StatsTarget.newBuilder()
                    .setTable(ai.floedb.floecat.catalog.rpc.TableStatsTarget.getDefaultInstance())
                    .build())
            .setTable(TableValueStats.newBuilder().setRowCount(10L).build())
            .build();
    TargetStatsRecord chunkFileStat =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId())
            .setSnapshotId(SNAPSHOT_ID)
            .setTarget(
                StatsTarget.newBuilder()
                    .setFile(
                        ai.floedb.floecat.catalog.rpc.FileStatsTarget.newBuilder()
                            .setFilePath("s3://bucket/data/file-1.parquet"))
                    .build())
            .setFile(
                ai.floedb.floecat.catalog.rpc.FileTargetStats.newBuilder()
                    .setFilePath("s3://bucket/data/file-1.parquet")
                    .setRowCount(5L)
                    .build())
            .build();
    TargetStatsRecord chunkAggregate =
        previousAggregate.toBuilder()
            .setTable(TableValueStats.newBuilder().setRowCount(5L).build())
            .build();
    TargetStatsRecord mergedAggregate =
        previousAggregate.toBuilder()
            .setTable(TableValueStats.newBuilder().setRowCount(15L).build())
            .build();
    ReconcileFileGroupTask plannedTask =
        ReconcileFileGroupTask.of(
                "plan-1",
                "group-1",
                TABLE_ID,
                SNAPSHOT_ID,
                List.of("s3://bucket/data/file-1.parquet"))
            .withPartialAggregateRecords(List.of(previousAggregate));
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            TABLE_ID,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(),
                java.util.Set.of(
                    ReconcileCapturePolicy.Output.TABLE_STATS,
                    ReconcileCapturePolicy.Output.FILE_STATS)));
    ReconcileJobStore.LeasedJob lease =
        new ReconcileJobStore.LeasedJob(
            CHILD_JOB_ID,
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_ONLY,
            scope,
            ReconcileExecutionPolicy.defaults(),
            LEASE_EPOCH,
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            null,
            null,
            ReconcileSnapshotTask.of(
                TABLE_ID,
                SNAPSHOT_ID,
                "db",
                "events",
                List.of(plannedTask),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                1),
            plannedTask,
            PARENT_JOB_ID);

    when(snapshotFinalizePersistence.buildAggregateStats(
            eq(tableId()), eq(SNAPSHOT_ID), any(), eq(List.of(chunkFileStat))))
        .thenReturn(List.of(chunkAggregate));
    when(snapshotFinalizePersistence.mergeAggregatePartials(
            eq(tableId()), eq(SNAPSHOT_ID), any(), eq(List.of(previousAggregate, chunkAggregate))))
        .thenReturn(List.of(mergedAggregate));

    List<TargetStatsRecord> merged =
        invokeMergedPartialAggregates(
            lease, tableId(), SNAPSHOT_ID, plannedTask, List.of(chunkFileStat));

    assertEquals(List.of(mergedAggregate), merged);
    verify(snapshotFinalizePersistence)
        .mergeAggregatePartials(
            eq(tableId()), eq(SNAPSHOT_ID), any(), eq(List.of(previousAggregate, chunkAggregate)));
  }

  @Test
  void mergedPartialAggregatesRejectsNonFileStatsInFileGroupChunks() throws Exception {
    ReconcileFileGroupTask plannedTask =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            TABLE_ID,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(), java.util.Set.of(ReconcileCapturePolicy.Output.TABLE_STATS)));
    ReconcileJobStore.LeasedJob lease =
        new ReconcileJobStore.LeasedJob(
            CHILD_JOB_ID,
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_ONLY,
            scope,
            ReconcileExecutionPolicy.defaults(),
            LEASE_EPOCH,
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            null,
            null,
            ReconcileSnapshotTask.of(
                TABLE_ID,
                SNAPSHOT_ID,
                "db",
                "events",
                List.of(plannedTask),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                1),
            plannedTask,
            PARENT_JOB_ID);
    TargetStatsRecord aggregateRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId())
            .setSnapshotId(SNAPSHOT_ID)
            .setTarget(
                StatsTarget.newBuilder()
                    .setTable(ai.floedb.floecat.catalog.rpc.TableStatsTarget.getDefaultInstance())
                    .build())
            .setTable(TableValueStats.newBuilder().setRowCount(5L).build())
            .build();

    Throwable error =
        assertThrows(
            Throwable.class,
            () ->
                invokeMergedPartialAggregates(
                    lease, tableId(), SNAPSHOT_ID, plannedTask, List.of(aggregateRecord)));

    assertTrue(error.getCause() instanceof StatusRuntimeException);
    assertEquals(
        "INVALID_ARGUMENT: file-group result stats_records must contain only file-target stats",
        error.getCause().getMessage());
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
    when(tableRepo.getById(tableId()))
        .thenReturn(
            Optional.of(
                table().toBuilder()
                    .putProperties("current-snapshot-id", Long.toString(SNAPSHOT_ID))
                    .build()));
    when(connectorRepo.getById(connectorId()))
        .thenReturn(Optional.of(connector().toBuilder().setKind(ConnectorKind.CK_ICEBERG).build()));
    when(snapshotRepo.getById(tableId(), SNAPSHOT_ID))
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
  void parseIndexArtifactsAllowsDirectUploadedArtifactWithoutInlineContent() throws Exception {
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
    LeasedFileGroupIndexArtifact artifact =
        LeasedFileGroupIndexArtifact.newBuilder().setRecord(record).build();

    List<ReconcilerBackend.StagedIndexArtifact> staged =
        invokeParseIndexArtifacts(List.of(artifact));

    assertEquals(1, staged.size());
    assertEquals(record, staged.getFirst().record());
    assertEquals(0, staged.getFirst().content().length);
    assertTrue(staged.getFirst().contentType().isEmpty());
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
    PutIndexArtifactItem item =
        PutIndexArtifactItem.newBuilder().setRecord(record).setContent(ByteString.empty()).build();

    when(service.blobStore.head(record.getArtifactUri())).thenReturn(Optional.empty());

    IndexArtifactRecord prepared = invokePrepareIndexArtifactRecord(item);

    verify(service.blobStore, never()).put(anyString(), any(byte[].class), anyString());
    verify(service.blobStore).head(record.getArtifactUri());
    assertEquals(record.toBuilder().setContentEtag("").build(), prepared);
  }

  @SuppressWarnings("unchecked")
  private List<ReconcilerBackend.StagedIndexArtifact> invokeParseIndexArtifacts(
      List<LeasedFileGroupIndexArtifact> artifacts) throws Exception {
    Method method =
        LeasedFileGroupExecutionService.class.getDeclaredMethod("parseIndexArtifacts", List.class);
    method.setAccessible(true);
    return (List<ReconcilerBackend.StagedIndexArtifact>) method.invoke(service, artifacts);
  }

  private IndexArtifactRecord invokePrepareIndexArtifactRecord(PutIndexArtifactItem item)
      throws Exception {
    Class<?> metricsClass =
        Class.forName(
            "ai.floedb.floecat.service.reconciler.impl.LeasedFileGroupExecutionService$ChunkPersistMetrics");
    var ctor = metricsClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object metrics = ctor.newInstance();
    Method method =
        LeasedFileGroupExecutionService.class.getDeclaredMethod(
            "prepareIndexArtifactRecord", PutIndexArtifactItem.class, metricsClass);
    method.setAccessible(true);
    return (IndexArtifactRecord) method.invoke(service, item, metrics);
  }

  @SuppressWarnings("unchecked")
  private List<TargetStatsRecord> invokeMergedPartialAggregates(
      ReconcileJobStore.LeasedJob lease,
      ResourceId tableId,
      long snapshotId,
      ReconcileFileGroupTask plannedTask,
      List<TargetStatsRecord> chunkStats)
      throws Exception {
    Method method =
        LeasedFileGroupExecutionService.class.getDeclaredMethod(
            "mergedPartialAggregates",
            ReconcileJobStore.LeasedJob.class,
            ResourceId.class,
            long.class,
            ReconcileFileGroupTask.class,
            List.class);
    method.setAccessible(true);
    return (List<TargetStatsRecord>)
        method.invoke(service, lease, tableId, snapshotId, plannedTask, chunkStats);
  }

  private static ReconcileJobStore.ReconcileJob job(
      String jobId,
      ReconcileJobKind kind,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      String parentJobId) {
    return job(
        jobId,
        kind,
        snapshotTask,
        fileGroupTask,
        parentJobId,
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
        false,
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

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId(ACCOUNT_ID)
        .setKind(ResourceKind.RK_TABLE)
        .setId(TABLE_ID)
        .build();
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
