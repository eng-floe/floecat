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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
import ai.floedb.floecat.reconciler.impl.FileArtifactReuse;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.StandaloneFileGroupExecutionPayload;
import ai.floedb.floecat.reconciler.jobs.ArtifactReferenceDigest;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
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
  private IndexArtifactRepository indexArtifactRepository;
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
    indexArtifactRepository = mock(IndexArtifactRepository.class);
    statsOrchestrator = mock(StatsOrchestrator.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.tableRepo = tableRepo;
    service.connectorRepo = connectorRepo;
    service.snapshotRepo = snapshotRepo;
    service.credentialResolver = credentialResolver;
    service.idempotencyStore = idempotencyStore;
    service.statsStore = statsStore;
    service.indexArtifactRepository = indexArtifactRepository;
    when(principal.getCorrelationId()).thenReturn("corr");
    when(principal.getAccountId()).thenReturn(ACCOUNT_ID);
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
            "plan-1",
            "group-1",
            TABLE_ID,
            SNAPSHOT_ID,
            1,
            "",
            0,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(),
            List.of(),
            "{\"type\":\"struct\",\"fields\":[]}",
            List.of(
                ReconcileFileExecutionPlan.of(
                    "s3://bucket/data/file-1.parquet",
                    123L,
                    "{}",
                    null,
                    "PARQUET",
                    3,
                    List.of(
                        new ReconcileFileExecutionPlan.IcebergDeleteFile(
                            "s3://bucket/data/delete-1.parquet",
                            10L,
                            ReconcileFileExecutionPlan.IcebergDeleteContent.POSITION,
                            3,
                            List.of())))));

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
    assertEquals("{\"type\":\"struct\",\"fields\":[]}", payload.executionSchemaJson());
    assertEquals(1, payload.fileExecutionPlans().size());
    assertEquals(123L, payload.fileExecutionPlans().getFirst().fileSizeInBytes());
    assertEquals("PARQUET", payload.fileExecutionPlans().getFirst().fileFormat());
    assertEquals(
        ReconcileFileExecutionPlan.IcebergDeleteContent.POSITION,
        payload.fileExecutionPlans().getFirst().icebergDeleteFiles().getFirst().content());
    assertTrue(!payload.fileExecutionPlans().getFirst().sourceFingerprint().isBlank());
  }

  @Test
  void resolvePinsCompatiblePriorStatsAndIndexIntoIncrementalFilePlan() {
    String filePath = "s3://bucket/data/file-1.parquet";
    String schema = "{\"type\":\"struct\",\"fields\":[]}";
    ReconcileFileExecutionPlan filePlan =
        ReconcileFileExecutionPlan.of(
            filePath, 123L, "{}", null, "PARQUET", 3, List.of(), "iceberg-data-v1:7:10");
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            TABLE_ID,
            SNAPSHOT_ID,
            1,
            "",
            0,
            List.of(filePath),
            List.of(),
            List.of(),
            schema,
            List.of(filePlan));
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(),
            java.util.Set.of(
                ReconcileCapturePolicy.Output.FILE_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    String sourceFingerprint = FileArtifactReuse.sourceFingerprint(filePlan, schema);
    String indexSourceFingerprint = FileArtifactReuse.indexSourceFingerprint(filePlan);
    String statsSignature = FileArtifactReuse.statsCaptureSignature(policy);
    String indexSignature = FileArtifactReuse.indexCaptureSignature(policy);
    TargetStatsRecord priorStats =
        fileStatsRecord(filePath, 10L).toBuilder()
            .setSnapshotId(SNAPSHOT_ID - 1L)
            .putProperties(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY, sourceFingerprint)
            .putProperties(FileArtifactReuse.STATS_SIGNATURE_PROPERTY, statsSignature)
            .putProperties(FileArtifactReuse.REALIZED_STATS_SELECTORS_PROPERTY, "#1")
            .build();
    IndexTarget indexTarget =
        IndexTarget.newBuilder()
            .setFile(IndexFileTarget.newBuilder().setFilePath(filePath))
            .build();
    IndexArtifactRecord priorIndex =
        IndexArtifactRecord.newBuilder()
            .setTableId(tableId())
            .setSnapshotId(SNAPSHOT_ID - 1L)
            .setTarget(indexTarget)
            .setArtifactUri("s3://sidecars/prior.parquet")
            .setState(IndexArtifactState.IAS_READY)
            .putProperties(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY, indexSourceFingerprint)
            .putProperties(FileArtifactReuse.INDEX_SIGNATURE_PROPERTY, indexSignature)
            .putProperties("indexed_columns", "#1")
            .build();
    var pinned =
        new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
            "generation-1", 7L, "/capture.pb", 9L);
    var repositoryPredecessor =
        new IndexArtifactRepository.GenerationPredecessor("generation-1", 7L, "/capture.pb", 9L);

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
                    scope)));
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
                            false,
                            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                            "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                            1)
                        .withIndexPredecessor(pinned),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(tableRepo.getById(tableId())).thenReturn(Optional.of(table()));
    when(connectorRepo.getById(connectorId())).thenReturn(Optional.of(connector()));
    when(indexArtifactRepository.loadGenerationInput(
            tableId(), SNAPSHOT_ID, repositoryPredecessor, List.of(filePath)))
        .thenReturn(new IndexArtifactRepository.GenerationInput(repositoryPredecessor, List.of()));
    when(statsStore.getReusableTargetStats(
            tableId(), StatsTargetIdentity.fileTarget(filePath), sourceFingerprint, statsSignature))
        .thenReturn(Optional.of(priorStats));
    when(indexArtifactRepository.getReusableIndexArtifact(
            tableId(), indexTarget, indexSourceFingerprint, indexSignature))
        .thenReturn(Optional.of(priorIndex));

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    ReconcileFileExecutionPlan resolved = payload.fileExecutionPlans().getFirst();
    assertEquals(sourceFingerprint, resolved.sourceFingerprint());
    assertEquals(indexSourceFingerprint, resolved.indexSourceFingerprint());
    assertEquals(SNAPSHOT_ID, resolved.reusableFileStats().getSnapshotId());
    assertEquals(SNAPSHOT_ID, resolved.reusableIndexArtifact().getSnapshotId());
    assertEquals("s3://sidecars/prior.parquet", resolved.reusableIndexArtifact().getArtifactUri());
  }

  @Test
  void resolveMigratesLegacyIcebergStatsWithoutSnapshotOrdering() {
    String filePath = "s3://bucket/data/file-1.parquet";
    String schema = "{\"type\":\"struct\",\"fields\":[]}";
    ReconcileFileExecutionPlan filePlan =
        ReconcileFileExecutionPlan.of(
            filePath, 123L, "{}", null, "PARQUET", 3, List.of(), "iceberg-data-v1:7:10");
    ReconcileFileGroupTask group = fileGroup(filePath, schema, filePlan);
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(), java.util.Set.of(ReconcileCapturePolicy.Output.FILE_STATS));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    TargetStatsRecord legacy =
        fileStatsRecord(filePath, 10L).toBuilder()
            .setSnapshotId(987L)
            .setFile(
                FileTargetStats.newBuilder()
                    .setFilePath(filePath)
                    .setRowCount(10L)
                    .setSizeBytes(123L)
                    .setSequenceNumber(7L))
            .build();

    stubFileGroupResolve(group, scope, null);
    when(statsStore.findHistoricalTargetStats(
            eq(tableId()), eq(StatsTargetIdentity.fileTarget(filePath)), any()))
        .thenReturn(Optional.of(legacy));

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    ReconcileFileExecutionPlan resolved = payload.fileExecutionPlans().getFirst();
    assertEquals(SNAPSHOT_ID, resolved.reusableFileStats().getSnapshotId());
    assertEquals(10L, resolved.reusableFileStats().getFile().getRowCount());
    assertEquals(
        resolved.sourceFingerprint(),
        resolved
            .reusableFileStats()
            .getPropertiesMap()
            .get(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY));
  }

  @Test
  void resolveMigratesLegacyDeltaStatsUsingHistoricalLogIdentity() {
    String filePath = "s3://bucket/data/file-1.parquet";
    String contentIdentity = "delta-add-v1:42::";
    ReconcileFileExecutionPlan filePlan =
        ReconcileFileExecutionPlan.of(
            filePath, 123L, "{}", null, "PARQUET", 3, List.of(), contentIdentity);
    ReconcileFileGroupTask group = fileGroup(filePath, "{}", filePlan);
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(), java.util.Set.of(ReconcileCapturePolicy.Output.FILE_STATS));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    TargetStatsRecord legacy =
        fileStatsRecord(filePath, 10L).toBuilder()
            .setSnapshotId(987L)
            .setFile(
                FileTargetStats.newBuilder()
                    .setFilePath(filePath)
                    .setRowCount(10L)
                    .setSizeBytes(123L))
            .build();

    stubFileGroupResolve(group, scope, null);
    when(connectorRepo.getById(connectorId()))
        .thenReturn(Optional.of(connector().toBuilder().setKind(ConnectorKind.CK_DELTA).build()));
    service.legacyFileIdentityResolverFactory =
        (resolvedConnector, namespace, tableName, destinationTableId, filePaths) ->
            new LeasedFileGroupExecutionService.LegacyFileIdentityResolver() {
              @Override
              public Optional<String> contentIdentity(long snapshotId, String requestedFilePath) {
                return snapshotId == 987L && filePath.equals(requestedFilePath)
                    ? Optional.of(contentIdentity)
                    : Optional.empty();
              }

              @Override
              public void close() {}
            };
    when(statsStore.findHistoricalTargetStats(
            eq(tableId()), eq(StatsTargetIdentity.fileTarget(filePath)), any()))
        .thenAnswer(
            invocation -> {
              java.util.function.Predicate<TargetStatsRecord> compatibility =
                  invocation.getArgument(2);
              return compatibility.test(legacy) ? Optional.of(legacy) : Optional.empty();
            });

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    ReconcileFileExecutionPlan resolved = payload.fileExecutionPlans().getFirst();
    assertEquals(SNAPSHOT_ID, resolved.reusableFileStats().getSnapshotId());
    assertEquals(10L, resolved.reusableFileStats().getFile().getRowCount());
    assertEquals(
        resolved.sourceFingerprint(),
        resolved
            .reusableFileStats()
            .getPropertiesOrThrow(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY));
  }

  @Test
  void resolveMigratesLegacyIndexForIndexOnlyPolicyUsingHistoricalStatsIdentity() {
    String filePath = "s3://bucket/data/file-1.parquet";
    String schema = "{\"type\":\"struct\",\"fields\":[]}";
    ReconcileFileExecutionPlan filePlan =
        ReconcileFileExecutionPlan.of(
            filePath, 123L, "{}", null, "PARQUET", 3, List.of(), "iceberg-data-v1:7:10");
    ReconcileFileGroupTask group = fileGroup(filePath, schema, filePlan);
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", false, true)),
            java.util.Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    TargetStatsRecord legacyStats =
        fileStatsRecord(filePath, 10L).toBuilder()
            .setSnapshotId(987L)
            .setFile(
                FileTargetStats.newBuilder()
                    .setFilePath(filePath)
                    .setRowCount(10L)
                    .setSizeBytes(123L)
                    .setSequenceNumber(7L))
            .build();
    IndexTarget target =
        IndexTarget.newBuilder()
            .setFile(IndexFileTarget.newBuilder().setFilePath(filePath))
            .build();
    IndexArtifactRecord legacyIndex =
        IndexArtifactRecord.newBuilder()
            .setTableId(tableId())
            .setSnapshotId(987L)
            .setTarget(target)
            .setArtifactUri("s3://sidecars/legacy.parquet")
            .setState(IndexArtifactState.IAS_READY)
            .putProperties("indexed_columns", "#1")
            .build();
    var pinned =
        new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
            "generation-1", 7L, "/capture.pb", 9L);

    stubFileGroupResolve(group, scope, pinned);
    when(statsStore.findHistoricalTargetStats(
            eq(tableId()), eq(StatsTargetIdentity.fileTarget(filePath)), any()))
        .thenReturn(Optional.of(legacyStats));
    when(indexArtifactRepository.getIndexArtifact(tableId(), 987L, target))
        .thenReturn(Optional.of(legacyIndex));

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    ReconcileFileExecutionPlan resolved = payload.fileExecutionPlans().getFirst();
    assertEquals(TargetStatsRecord.getDefaultInstance(), resolved.reusableFileStats());
    assertEquals(SNAPSHOT_ID, resolved.reusableIndexArtifact().getSnapshotId());
    assertEquals("s3://sidecars/legacy.parquet", resolved.reusableIndexArtifact().getArtifactUri());
  }

  @Test
  void persistSuccessProtectsReferencedStatsObjectsBeforeRegisteringDescriptor() {
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
            principal,
            CHILD_JOB_ID,
            LEASE_EPOCH,
            "result-1",
            resultDescriptor(List.of()),
            List.of(),
            List.of());

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
    verify(statsStore)
        .protectPrewrittenStatsObjectsInGeneration(
            eq(tableId()),
            eq(SNAPSHOT_ID),
            eq("full-rescan-" + PARENT_JOB_ID),
            eq(CHILD_JOB_ID + ":" + LEASE_EPOCH),
            eq(List.of()));
    verify(statsStore)
        .registerPrewrittenStatsReferencesInGeneration(
            eq(tableId()), eq(SNAPSHOT_ID), eq("full-rescan-" + PARENT_JOB_ID), eq(List.of()));
    verify(indexArtifactRepository)
        .registerPrewrittenIndexArtifactReferencesInGeneration(
            eq(tableId()),
            eq(SNAPSHOT_ID),
            eq("full-rescan-" + PARENT_JOB_ID),
            eq(statsObjectPrefix() + "index-artifacts/"),
            eq(List.of()));
    verify(statsStore)
        .markPreparedFileGroup(
            eq(tableId()),
            eq(SNAPSHOT_ID),
            eq("full-rescan-" + PARENT_JOB_ID),
            eq(CHILD_JOB_ID),
            eq(LEASE_EPOCH),
            eq(resultDescriptor(List.of()).artifactReferencesSha256()));
    var order = inOrder(jobs, statsStore, indexArtifactRepository);
    order
        .verify(jobs)
        .completeFileGroupSuccess(
            eq(CHILD_JOB_ID),
            eq(LEASE_EPOCH),
            any(ReconcileFileGroupResultDescriptor.class),
            anyLong(),
            eq("Executed file group group-1"));
    order
        .verify(statsStore)
        .protectPrewrittenStatsObjectsInGeneration(
            any(), anyLong(), anyString(), anyString(), any());
    order
        .verify(statsStore)
        .markPreparedFileGroup(
            any(), anyLong(), anyString(), anyString(), anyString(), anyString());
    verify(idempotencyStore, never())
        .createPending(anyString(), anyString(), anyString(), anyString(), any(), any());
    verify(idempotencyStore, never())
        .finalizeSuccess(
            anyString(), anyString(), anyString(), anyString(), any(), any(), any(), any(), any());
  }

  @Test
  void persistSuccessAllowsAttachedIcebergDeleteFileStatsTarget() {
    String dataFile = "s3://bucket/data/file-1.parquet";
    String deleteFile = "s3://bucket/data/delete-1.parquet";
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            TABLE_ID,
            SNAPSHOT_ID,
            1,
            "",
            0,
            List.of(dataFile),
            List.of(),
            List.of(),
            "",
            List.of(
                ReconcileFileExecutionPlan.of(
                    dataFile,
                    123L,
                    "{}",
                    null,
                    "PARQUET",
                    3,
                    List.of(
                        new ReconcileFileExecutionPlan.IcebergDeleteFile(
                            deleteFile,
                            10L,
                            ReconcileFileExecutionPlan.IcebergDeleteContent.POSITION,
                            3,
                            List.of())))));
    ReconcileJobStore.ReconcileJob childLeaseView =
        job(
            CHILD_JOB_ID,
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileSnapshotTask.empty(),
            plannedGroup.asReference(),
            PARENT_JOB_ID);
    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
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
                        List.of(plannedGroup),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/snapshot-plan.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(jobs.get(ACCOUNT_ID, CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    List<TargetStatsRecord> records =
        List.of(fileStatsRecord(dataFile, 10L), fileStatsRecord(deleteFile, 2L));
    List<StatsObjectDescriptor> descriptors = statsObjectDescriptors(records);

    assertTrue(
        service.persistSuccess(
            principal,
            CHILD_JOB_ID,
            LEASE_EPOCH,
            "result-1",
            resultDescriptor(records),
            descriptors,
            List.of()));

    verify(statsStore)
        .registerPrewrittenStatsReferencesInGeneration(
            eq(tableId()),
            eq(SNAPSHOT_ID),
            eq("full-rescan-" + PARENT_JOB_ID),
            org.mockito.ArgumentMatchers.argThat(references -> references.size() == 2));
  }

  @Test
  void persistSuccessAllowsAttachedDeltaDeletionVectorStatsTarget() {
    String dataFile = "s3://bucket/data/file-1.parquet";
    String deletionVectorFile = "s3://bucket/data/deletion-vector-1.bin";
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            TABLE_ID,
            SNAPSHOT_ID,
            1,
            "",
            0,
            List.of(dataFile),
            List.of(),
            List.of(),
            "",
            List.of(
                ReconcileFileExecutionPlan.of(
                    dataFile,
                    123L,
                    "{}",
                    new ReconcileFileExecutionPlan.DeltaDeletionVector(
                        "p", deletionVectorFile, 4, 16, 2),
                    "PARQUET",
                    0,
                    List.of())));
    ReconcileJobStore.ReconcileJob childLeaseView =
        job(
            CHILD_JOB_ID,
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileSnapshotTask.empty(),
            plannedGroup.asReference(),
            PARENT_JOB_ID);
    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
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
                        List.of(plannedGroup),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/snapshot-plan.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(jobs.get(ACCOUNT_ID, CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    List<TargetStatsRecord> records =
        List.of(fileStatsRecord(dataFile, 10L), fileStatsRecord(deletionVectorFile, 2L));
    List<StatsObjectDescriptor> descriptors = statsObjectDescriptors(records);

    assertTrue(
        service.persistSuccess(
            principal,
            CHILD_JOB_ID,
            LEASE_EPOCH,
            "result-1",
            resultDescriptor(records),
            descriptors,
            List.of()));

    verify(statsStore)
        .registerPrewrittenStatsReferencesInGeneration(
            eq(tableId()),
            eq(SNAPSHOT_ID),
            eq("full-rescan-" + PARENT_JOB_ID),
            org.mockito.ArgumentMatchers.argThat(references -> references.size() == 2));
  }

  @Test
  void exactTerminalReplayFinishesStagingAfterCompletionCrash() {
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileJobStore.ReconcileJob terminal = terminalFileGroupJob(plannedGroup, "JS_SUCCEEDED");
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(terminal));

    assertTrue(
        service.persistSuccess(
            principal,
            CHILD_JOB_ID,
            LEASE_EPOCH,
            "result-1",
            resultDescriptor(List.of()),
            List.of(),
            List.of()));

    var order = inOrder(jobs, statsStore);
    order
        .verify(jobs)
        .completeFileGroupSuccess(
            eq(CHILD_JOB_ID),
            eq(LEASE_EPOCH),
            any(ReconcileFileGroupResultDescriptor.class),
            anyLong(),
            eq("Executed file group"));
    order
        .verify(statsStore)
        .protectPrewrittenStatsObjectsInGeneration(
            any(), anyLong(), anyString(), anyString(), any());
    order
        .verify(statsStore)
        .markPreparedFileGroup(
            any(), anyLong(), anyString(), anyString(), anyString(), anyString());
  }

  @Test
  void rejectedTerminalReplayCannotMutateGenerationState() {
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    when(jobs.getLeaseView(CHILD_JOB_ID))
        .thenReturn(Optional.of(terminalFileGroupJob(plannedGroup, "JS_SUCCEEDED")));
    when(jobs.completeFileGroupSuccess(
            anyString(),
            anyString(),
            any(ReconcileFileGroupResultDescriptor.class),
            anyLong(),
            anyString()))
        .thenReturn(false);

    assertThrows(
        StatusRuntimeException.class,
        () ->
            service.persistSuccess(
                principal,
                CHILD_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                resultDescriptor(List.of()),
                List.of(),
                List.of()));

    verifyNoInteractions(statsStore, indexArtifactRepository);
  }

  @Test
  void persistSuccessRejectsIndexPredecessorThatDiffersFromParentPin() {
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            TABLE_ID,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(), java.util.Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
    var pinned =
        new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
            "generation-1", 7L, "/capture.pb", 9L);
    var submitted =
        new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
            "generation-2", 8L, "/capture-2.pb", 10L);
    ReconcileJobStore.ReconcileJob childLeaseView =
        job(
            CHILD_JOB_ID,
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileSnapshotTask.empty(),
            plannedGroup.asReference(),
            PARENT_JOB_ID,
            CaptureMode.METADATA_AND_CAPTURE,
            scope);
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
                    1)
                .withIndexPredecessor(pinned),
            ReconcileFileGroupTask.empty(),
            "");
    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID)).thenReturn(Optional.of(parent));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            service.persistSuccess(
                principal,
                CHILD_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                resultDescriptor(List.of(), List.of(), List.of(), submitted),
                List.of(),
                List.of()));

    verify(jobs, never())
        .completeFileGroupSuccess(
            anyString(),
            anyString(),
            any(ReconcileFileGroupResultDescriptor.class),
            anyLong(),
            anyString());
  }

  @Test
  void persistSuccessProtectsEachIndividualStatsObjectWithoutReadingIt() {
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
    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
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
                        List.of(plannedGroup),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(jobs.get(ACCOUNT_ID, CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    TargetStatsRecord record = fileStatsRecord("s3://bucket/data/file-1.parquet", 10L);

    assertTrue(
        service.persistSuccess(
            principal,
            CHILD_JOB_ID,
            LEASE_EPOCH,
            "result-1",
            resultDescriptor(List.of(record)),
            statsObjectDescriptors(List.of(record)),
            List.of()));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<StatsStore.PrewrittenStatsObject>> objects =
        ArgumentCaptor.forClass(List.class);
    verify(statsStore)
        .protectPrewrittenStatsObjectsInGeneration(
            eq(tableId()),
            eq(SNAPSHOT_ID),
            eq("full-rescan-" + PARENT_JOB_ID),
            eq(CHILD_JOB_ID + ":" + LEASE_EPOCH),
            objects.capture());
    assertEquals(1, objects.getValue().size());
    assertEquals(statsObjectPrefix() + "0.pb", objects.getValue().getFirst().blobUri());
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<StatsStore.PrewrittenTargetStatsReference>> references =
        ArgumentCaptor.forClass(List.class);
    verify(statsStore)
        .registerPrewrittenStatsReferencesInGeneration(
            eq(tableId()),
            eq(SNAPSHOT_ID),
            eq("full-rescan-" + PARENT_JOB_ID),
            references.capture());
    assertEquals(1, references.getValue().size());
  }

  @Test
  void persistSuccessAllowsStatsAndIndexPointersForTheSameTarget() {
    String filePath = "s3://bucket/data/file-1.parquet";
    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of("plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of(filePath));
    ReconcileJobStore.ReconcileJob childLeaseView =
        job(
            CHILD_JOB_ID,
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileSnapshotTask.empty(),
            plannedGroup.asReference(),
            PARENT_JOB_ID);
    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
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
                        List.of(plannedGroup),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(jobs.get(ACCOUNT_ID, CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));

    TargetStatsRecord record = fileStatsRecord(filePath, 10L);
    StatsObjectDescriptor fileStats = statsObjectDescriptors(List.of(record)).getFirst();
    byte[] indexBytes = "index".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    String indexArtifactObjectPrefix = statsObjectPrefix() + "index-artifacts/";
    StatsObjectDescriptor indexArtifact =
        StatsObjectDescriptor.newBuilder()
            .setTargetStorageId("file:" + filePath)
            .setPayloadUri(
                indexArtifactObjectPrefix
                    + Hashing.sha256Hex(fileStats.getTargetStorageId())
                    + "/"
                    + HexFormat.of().formatHex(sha256(indexBytes))
                    + ".pb")
            .setPayloadBytes(indexBytes.length)
            .setPayloadSha256(ByteString.copyFrom(sha256(indexBytes)))
            .build();

    assertTrue(
        service.persistSuccess(
            principal,
            CHILD_JOB_ID,
            LEASE_EPOCH,
            "result-1",
            resultDescriptor(List.of(record), List.of(fileStats), List.of(indexArtifact)),
            List.of(fileStats),
            List.of(indexArtifact)));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<StatsStore.PrewrittenStatsObject>> objects =
        ArgumentCaptor.forClass(List.class);
    verify(statsStore)
        .protectPrewrittenStatsObjectsInGeneration(
            eq(tableId()),
            eq(SNAPSHOT_ID),
            eq("full-rescan-" + PARENT_JOB_ID),
            eq(CHILD_JOB_ID + ":" + LEASE_EPOCH),
            objects.capture());
    assertEquals(2, objects.getValue().size());
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<IndexArtifactRepository.PrewrittenIndexArtifactReference>> references =
        ArgumentCaptor.forClass(List.class);
    verify(indexArtifactRepository)
        .registerPrewrittenIndexArtifactReferencesInGeneration(
            eq(tableId()),
            eq(SNAPSHOT_ID),
            eq("full-rescan-" + PARENT_JOB_ID),
            eq(indexArtifactObjectPrefix),
            references.capture());
    assertEquals(1, references.getValue().size());
    assertEquals("file:" + filePath, references.getValue().getFirst().targetStorageId());
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
                    principal,
                    CHILD_JOB_ID,
                    LEASE_EPOCH,
                    "result-1",
                    resultDescriptor(List.of()),
                    List.of(),
                    List.of()));

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    verify(statsStore, never())
        .protectPrewrittenStatsObjectsInGeneration(
            any(), anyLong(), anyString(), anyString(), any());
    verify(indexArtifactRepository, never())
        .registerPrewrittenIndexArtifactReferencesInGeneration(
            any(), anyLong(), anyString(), anyString(), any());
    verify(idempotencyStore, never())
        .finalizeSuccess(
            anyString(), anyString(), anyString(), anyString(), any(), any(), any(), any(), any());
  }

  @Test
  void persistSuccessRejectsDigestMismatchAndUnplannedStatsTarget() {
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
    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getLeaseView(CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
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
                        List.of(plannedGroup),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/snapshot-plan.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(jobs.get(ACCOUNT_ID, CHILD_JOB_ID)).thenReturn(Optional.of(childLeaseView));
    TargetStatsRecord record = fileStatsRecord("s3://bucket/data/file-1.parquet", 10L);
    StatsObjectDescriptor changed =
        statsObjectDescriptors(List.of(record)).getFirst().toBuilder()
            .setPayloadUri(statsObjectPrefix() + "changed.pb")
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            service.persistSuccess(
                principal,
                CHILD_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                resultDescriptor(List.of(record)),
                List.of(changed),
                List.of()));

    StatsObjectDescriptor unplannedTarget =
        changed.toBuilder()
            .setTargetStorageId(
                StatsTargetIdentity.storageId(
                    StatsTargetIdentity.fileTarget("s3://bucket/data/not-in-group.parquet")))
            .build();
    IllegalArgumentException outsideGroup =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                service.persistSuccess(
                    principal,
                    CHILD_JOB_ID,
                    LEASE_EPOCH,
                    "result-1",
                    resultDescriptor(List.of(record), List.of(unplannedTarget), List.of()),
                    List.of(unplannedTarget),
                    List.of()));
    assertTrue(outsideGroup.getMessage().contains("outside the leased file group"));

    byte[] indexBytes = "index".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    StatsObjectDescriptor unplannedIndex =
        StatsObjectDescriptor.newBuilder()
            .setTargetStorageId("file:s3://bucket/data/not-in-group.parquet")
            .setPayloadUri(
                statsObjectPrefix()
                    + "index-artifacts/unplanned/"
                    + HexFormat.of().formatHex(sha256(indexBytes))
                    + ".pb")
            .setPayloadBytes(indexBytes.length)
            .setPayloadSha256(ByteString.copyFrom(sha256(indexBytes)))
            .build();
    IllegalArgumentException outsideIndexGroup =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                service.persistSuccess(
                    principal,
                    CHILD_JOB_ID,
                    LEASE_EPOCH,
                    "result-1",
                    resultDescriptor(List.of(), List.of(), List.of(unplannedIndex)),
                    List.of(),
                    List.of(unplannedIndex)));
    assertTrue(outsideIndexGroup.getMessage().contains("outside the leased file group"));

    verify(jobs, never())
        .completeFileGroupSuccess(anyString(), anyString(), any(), anyLong(), anyString());
    verifyNoInteractions(indexArtifactRepository);
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
  void resolveLoadsIndexArtifactsFromPinnedSnapshotGeneration() {
    String filePath = "s3://bucket/data/file-1.parquet";
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of("plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of(filePath));
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            TABLE_ID,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(), java.util.Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
    var pinned =
        new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
            "generation-1", 7L, "/capture.pb", 9L);
    var repositoryPredecessor =
        new IndexArtifactRepository.GenerationPredecessor("generation-1", 7L, "/capture.pb", 9L);

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
                    scope)));
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
                            1)
                        .withIndexPredecessor(pinned),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(tableRepo.getById(tableId())).thenReturn(Optional.of(table()));
    when(connectorRepo.getById(connectorId())).thenReturn(Optional.of(connector()));
    when(indexArtifactRepository.loadGenerationInput(
            eq(tableId()), eq(SNAPSHOT_ID), eq(repositoryPredecessor), eq(List.of(filePath))))
        .thenReturn(new IndexArtifactRepository.GenerationInput(repositoryPredecessor, List.of()));

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    assertEquals("generation-1", payload.indexPredecessor().generationId());
    assertEquals(7L, payload.indexPredecessor().activePointerVersion());
    verify(indexArtifactRepository)
        .loadGenerationInput(
            eq(tableId()), eq(SNAPSHOT_ID), eq(repositoryPredecessor), eq(List.of(filePath)));
    verify(indexArtifactRepository, never()).captureGenerationInput(any(), anyLong(), any());
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

  private static ReconcileJobStore.ReconcileJob job(
      String jobId,
      ReconcileJobKind kind,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      String parentJobId) {
    return job(jobId, kind, snapshotTask, fileGroupTask, parentJobId, false);
  }

  private static ReconcileJobStore.ReconcileJob terminalFileGroupJob(
      ReconcileFileGroupTask fileGroupTask, String state) {
    return new ReconcileJobStore.ReconcileJob(
        CHILD_JOB_ID,
        ACCOUNT_ID,
        CONNECTOR_ID,
        state,
        "Executed file group",
        1L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        true,
        CaptureMode.METADATA_ONLY,
        0L,
        0L,
        0L,
        false,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "",
        "remote_file_group_worker",
        ReconcileJobKind.EXEC_FILE_GROUP,
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        fileGroupTask,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        PARENT_JOB_ID);
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

  private static ReconcileFileGroupTask fileGroup(
      String filePath, String schema, ReconcileFileExecutionPlan filePlan) {
    return ReconcileFileGroupTask.of(
        "plan-1",
        "group-1",
        TABLE_ID,
        SNAPSHOT_ID,
        1,
        "",
        0,
        List.of(filePath),
        List.of(),
        List.of(),
        schema,
        List.of(filePlan));
  }

  private void stubFileGroupResolve(
      ReconcileFileGroupTask group,
      ReconcileScope scope,
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor pinned) {
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
                    scope)));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(group),
            false,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
            1);
    if (pinned != null) {
      snapshotTask = snapshotTask.withIndexPredecessor(pinned);
      var predecessor =
          new IndexArtifactRepository.GenerationPredecessor(
              pinned.generationId(),
              pinned.activePointerVersion(),
              pinned.captureManifestUri(),
              pinned.captureManifestPointerVersion());
      when(indexArtifactRepository.loadGenerationInput(
              tableId(), SNAPSHOT_ID, predecessor, group.filePaths()))
          .thenReturn(new IndexArtifactRepository.GenerationInput(predecessor, List.of()));
    }
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    PARENT_JOB_ID,
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    snapshotTask,
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(tableRepo.getById(tableId())).thenReturn(Optional.of(table()));
    when(connectorRepo.getById(connectorId())).thenReturn(Optional.of(connector()));
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

  private static String statsObjectPrefix() {
    return Keys.reconcileFileGroupStatsObjectPrefix(
        ACCOUNT_ID, TABLE_ID, SNAPSHOT_ID, PARENT_JOB_ID, CHILD_JOB_ID, LEASE_EPOCH);
  }

  private ReconcileFileGroupResultDescriptor resultDescriptor(List<TargetStatsRecord> fileStats) {
    return resultDescriptor(fileStats, statsObjectDescriptors(fileStats), List.of());
  }

  private ReconcileFileGroupResultDescriptor resultDescriptor(
      List<TargetStatsRecord> fileStats,
      List<StatsObjectDescriptor> fileStatsDescriptors,
      List<StatsObjectDescriptor> indexArtifactDescriptors) {
    return resultDescriptor(fileStats, fileStatsDescriptors, indexArtifactDescriptors, null);
  }

  private ReconcileFileGroupResultDescriptor resultDescriptor(
      List<TargetStatsRecord> fileStats,
      List<StatsObjectDescriptor> fileStatsDescriptors,
      List<StatsObjectDescriptor> indexArtifactDescriptors,
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor indexPredecessor) {
    byte[] resultBytes = "result-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
        indexArtifactDescriptors.size(),
        statsObjectPrefix(),
        fileStats.size(),
        ArtifactReferenceDigest.sha256(fileStatsDescriptors, indexArtifactDescriptors),
        indexPredecessor,
        1L);
  }

  private List<StatsObjectDescriptor> statsObjectDescriptors(List<TargetStatsRecord> fileStats) {
    var builder = new java.util.ArrayList<StatsObjectDescriptor>();
    for (int i = 0; i < fileStats.size(); i++) {
      byte[] statsBytes = fileStats.get(i).toByteArray();
      String statsUri = statsObjectPrefix() + i + ".pb";
      builder.add(
          StatsObjectDescriptor.newBuilder()
              .setTargetStorageId(StatsTargetIdentity.storageId(fileStats.get(i).getTarget()))
              .setPayloadUri(statsUri)
              .setPayloadBytes(statsBytes.length)
              .setPayloadSha256(ByteString.copyFrom(sha256(statsBytes)))
              .build());
    }
    return List.copyOf(builder);
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
