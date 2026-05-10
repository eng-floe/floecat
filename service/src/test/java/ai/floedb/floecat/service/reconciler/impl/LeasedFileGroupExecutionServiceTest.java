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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexCoverage;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.StandaloneFileGroupExecutionPayload;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultStreamRequest;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.storage.impl.StorageAuthorityResolver;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.smallrye.mutiny.Multi;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeasedFileGroupExecutionServiceTest {
  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("table-1")
          .build();
  private static final ResourceId CONNECTOR_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_CONNECTOR)
          .setId("connector-1")
          .build();

  private LeasedFileGroupExecutionService service;
  private ReconcileJobStore jobs;
  private TableRepository tableRepo;
  private ConnectorRepository connectorRepo;
  private SnapshotRepository snapshotRepo;
  private StorageAuthorityRepository storageAuthorityRepo;
  private StorageAuthorityResolver storageAuthorityResolver;
  private StatsStore statsStore;
  private IndexArtifactRepository indexArtifactRepo;
  private BlobStore blobStore;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedFileGroupExecutionService();
    jobs = mock(ReconcileJobStore.class);
    tableRepo = mock(TableRepository.class);
    connectorRepo = mock(ConnectorRepository.class);
    snapshotRepo = mock(SnapshotRepository.class);
    storageAuthorityRepo = mock(StorageAuthorityRepository.class);
    storageAuthorityResolver = mock(StorageAuthorityResolver.class);
    statsStore = mock(StatsStore.class);
    indexArtifactRepo = mock(IndexArtifactRepository.class);
    blobStore = mock(BlobStore.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.tableRepo = tableRepo;
    service.connectorRepo = connectorRepo;
    service.snapshotRepo = snapshotRepo;
    service.storageAuthorityRepo = storageAuthorityRepo;
    service.storageAuthorityResolver = storageAuthorityResolver;
    service.statsStore = statsStore;
    service.indexArtifactRepo = indexArtifactRepo;
    service.blobStore = blobStore;
    service.idempotencyStore = new InMemoryIdempotencyRepository();
    service.credentialResolver = mock(ai.floedb.floecat.connector.spi.CredentialResolver.class);
    when(principal.getCorrelationId()).thenReturn("corr");
    when(principal.getAccountId()).thenReturn("acct");
    when(statsStore.metaForTargetStats(any(), anyLong(), any(), any()))
        .thenReturn(MutationMeta.getDefaultInstance());
    when(indexArtifactRepo.metaForIndexArtifact(any(), anyLong(), any(), any()))
        .thenReturn(MutationMeta.getDefaultInstance());
    when(blobStore.head(any()))
        .thenReturn(Optional.of(BlobHeader.newBuilder().setEtag("etag-1").build()));
  }

  @Test
  void resolveFallsBackToConnectorMetadataLocationWhenCatalogStateIsBlank() throws Exception {
    when(jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(jobs.get("job-1"))
        .thenReturn(Optional.of(fileGroupJob("job-1", "parent-1", "snapshot-plan-1", "group-1")));
    when(tableRepo.getById(TABLE_ID)).thenReturn(Optional.of(tableWithoutMetadataLocation()));
    when(snapshotRepo.getById(TABLE_ID, 55L)).thenReturn(Optional.empty());
    Connector connector =
        Connector.newBuilder()
            .setResourceId(CONNECTOR_ID)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri("http://example.test")
            .build();
    when(connectorRepo.getById(CONNECTOR_ID)).thenReturn(Optional.of(connector));
    StorageAuthority authority =
        StorageAuthority.newBuilder()
            .setDisplayName("warehouse")
            .setEnabled(true)
            .setLocationPrefix("s3://warehouse")
            .build();
    when(storageAuthorityRepo.list(eq("acct"), eq(Integer.MAX_VALUE), eq(""), any()))
        .thenReturn(List.of(authority));
    when(storageAuthorityResolver.resolveServerSideStorageConfig(
            authority, "s3://warehouse/events/metadata/v1.metadata.json", "acct"))
        .thenReturn(
            Map.of(
                "s3.endpoint", "http://localhost:9000",
                "s3.path-style-access", "true",
                "s3.access-key-id", "minio",
                "s3.secret-access-key", "minio123"));

    FloecatConnector source = mock(FloecatConnector.class);
    when(source.describe("db", "events"))
        .thenReturn(
            new FloecatConnector.TableDescriptor(
                "db",
                "events",
                "s3://warehouse/events",
                "{}",
                List.of(),
                ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm.CID_FIELD_ID,
                Map.of("metadata-location", "s3://warehouse/events/metadata/v1.metadata.json")));
    service.connectorOpener = ignored -> source;

    StandaloneFileGroupExecutionPayload payload = service.resolve(principal, "job-1", "lease-1");

    assertEquals("s3://warehouse/events/metadata/v1.metadata.json", payload.metadataLocation());
    assertEquals("db", payload.sourceNamespace());
    assertEquals("events", payload.sourceTable());
    assertEquals("http://localhost:9000", payload.sourceStorageConfig().get("s3.endpoint"));
    assertEquals("minio", payload.sourceStorageConfig().get("s3.access-key-id"));
  }

  @Test
  void persistStreamedSuccessAcceptsMultipleStatsChunks() {
    when(jobs.renewLease("job-stream", "lease-stream")).thenReturn(true);
    when(jobs.get("job-stream"))
        .thenReturn(
            Optional.of(
                fileGroupJob(
                    "job-stream",
                    "parent-1",
                    "snapshot-plan-1",
                    "group-1",
                    List.of(
                        "s3://warehouse/events/data/file-1.parquet",
                        "s3://warehouse/events/data/file-2.parquet"))));

    var response =
        service
            .persistStreamedResult(
                principal,
                Multi.createFrom()
                    .items(
                        beginSuccess("job-stream", "lease-stream", "result-stream"),
                        statsChunk(
                            fileStats("s3://warehouse/events/data/file-1.parquet"),
                            fileStats("s3://warehouse/events/data/file-1.parquet")),
                        statsChunk(fileStats("s3://warehouse/events/data/file-2.parquet")),
                        endFrame()))
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(statsStore, times(2)).putTargetStats(any());
    verify(jobs)
        .persistFileGroupResult(
            eq("job-stream"),
            argThat(
                task ->
                    task != null
                        && task.fileResults().size() == 2
                        && task.fileResults().stream()
                            .anyMatch(
                                result ->
                                    result
                                            .filePath()
                                            .equals("s3://warehouse/events/data/file-1.parquet")
                                        && result.statsProcessed() == 2L)
                        && task.fileResults().stream()
                            .anyMatch(
                                result ->
                                    result
                                            .filePath()
                                            .equals("s3://warehouse/events/data/file-2.parquet")
                                        && result.statsProcessed() == 1L)));
  }

  @Test
  void persistStreamedSuccessReassemblesSplitArtifactContent() {
    when(jobs.renewLease("job-artifact", "lease-artifact")).thenReturn(true);
    when(jobs.get("job-artifact"))
        .thenReturn(
            Optional.of(
                fileGroupJob(
                    "job-artifact",
                    "parent-1",
                    "snapshot-plan-1",
                    "group-1",
                    List.of("s3://warehouse/events/data/file-1.parquet"))));

    var response =
        service
            .persistStreamedResult(
                principal,
                Multi.createFrom()
                    .items(
                        beginSuccess("job-artifact", "lease-artifact", "result-artifact"),
                        artifactBegin(
                            "s3://warehouse/events/data/file-1.parquet",
                            "s3://bucket/index.parquet"),
                        artifactContent("abc".getBytes()),
                        artifactContent("def".getBytes()),
                        artifactEnd(),
                        endFrame()))
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(blobStore)
        .put(
            eq("s3://bucket/index.parquet"),
            argThat(bytes -> java.util.Arrays.equals(bytes, "abcdef".getBytes())),
            eq("application/x-parquet"));
    verify(indexArtifactRepo)
        .putIndexArtifact(
            argThat(
                record ->
                    record != null
                        && record.getArtifactUri().equals("s3://bucket/index.parquet")
                        && record.getContentEtag().equals("etag-1")));
  }

  @Test
  void persistStreamedFailurePersistsFailedFileResults() {
    when(jobs.renewLease("job-failure", "lease-failure")).thenReturn(true);
    when(jobs.get("job-failure"))
        .thenReturn(
            Optional.of(
                fileGroupJob(
                    "job-failure",
                    "parent-1",
                    "snapshot-plan-1",
                    "group-1",
                    List.of(
                        "s3://warehouse/events/data/file-1.parquet",
                        "s3://warehouse/events/data/file-2.parquet"))));

    var response =
        service
            .persistStreamedResult(
                principal,
                Multi.createFrom()
                    .items(
                        beginFailure("job-failure", "lease-failure", "result-failure", "boom"),
                        endFrame()))
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(statsStore, times(0)).putTargetStats(any());
    verify(jobs)
        .persistFileGroupResult(
            eq("job-failure"),
            argThat(
                task ->
                    task != null
                        && task.fileResults().size() == 2
                        && task.fileResults().stream()
                            .allMatch(
                                result ->
                                    result.state()
                                            == ai.floedb.floecat.reconciler.jobs.ReconcileFileResult
                                                .State.FAILED
                                        && result.message().equals("boom"))));
  }

  @Test
  void persistStreamedSuccessIsIdempotentForRepeatedResultId() {
    when(jobs.renewLease("job-retry", "lease-retry")).thenReturn(true);
    when(jobs.get("job-retry"))
        .thenReturn(
            Optional.of(
                fileGroupJob(
                    "job-retry",
                    "parent-1",
                    "snapshot-plan-1",
                    "group-1",
                    List.of("s3://warehouse/events/data/file-1.parquet"))));

    Multi<SubmitLeasedFileGroupExecutionResultStreamRequest> stream =
        Multi.createFrom()
            .items(
                beginSuccess("job-retry", "lease-retry", "result-retry"),
                statsChunk(fileStats("s3://warehouse/events/data/file-1.parquet")),
                artifactBegin(
                    "s3://warehouse/events/data/file-1.parquet", "s3://bucket/retry-index.parquet"),
                artifactContent("payload".getBytes()),
                artifactEnd(),
                endFrame());

    assertTrue(
        service.persistStreamedResult(principal, stream).await().indefinitely().getAccepted());
    assertTrue(
        service
            .persistStreamedResult(
                principal,
                Multi.createFrom()
                    .items(
                        beginSuccess("job-retry", "lease-retry", "result-retry"),
                        statsChunk(fileStats("s3://warehouse/events/data/file-1.parquet")),
                        artifactBegin(
                            "s3://warehouse/events/data/file-1.parquet",
                            "s3://bucket/retry-index.parquet"),
                        artifactContent("payload".getBytes()),
                        artifactEnd(),
                        endFrame()))
            .await()
            .indefinitely()
            .getAccepted());

    verify(statsStore, times(1)).putTargetStats(any());
    verify(indexArtifactRepo, times(1)).putIndexArtifact(any());
    verify(jobs, times(1)).persistFileGroupResult(eq("job-retry"), any());
  }

  private static Table tableWithoutMetadataLocation() {
    return Table.newBuilder()
        .setResourceId(TABLE_ID)
        .setUpstream(
            UpstreamRef.newBuilder()
                .setConnectorId(CONNECTOR_ID)
                .addNamespacePath("db")
                .setTableDisplayName("events")
                .setFormat(ai.floedb.floecat.catalog.rpc.TableFormat.TF_ICEBERG))
        .build();
  }

  private static ReconcileJobStore.ReconcileJob fileGroupJob(
      String jobId, String parentJobId, String planId, String groupId) {
    return fileGroupJob(
        jobId, parentJobId, planId, groupId, List.of("s3://warehouse/events/data/file.parquet"));
  }

  private static ReconcileJobStore.ReconcileJob fileGroupJob(
      String jobId, String parentJobId, String planId, String groupId, List<String> filePaths) {
    return new ReconcileJobStore.ReconcileJob(
        jobId,
        "acct",
        CONNECTOR_ID.getId(),
        "JS_RUNNING",
        "",
        1L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        ReconcileScope.of(
            List.of(),
            null,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(), java.util.Set.of(ReconcileCapturePolicy.Output.FILE_STATS))),
        ReconcileExecutionPolicy.defaults(),
        "remote-executor",
        "remote_file_group_worker",
        ReconcileJobKind.EXEC_FILE_GROUP,
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.of(planId, groupId, TABLE_ID.getId(), 55L, filePaths),
        parentJobId);
  }

  private static SubmitLeasedFileGroupExecutionResultStreamRequest beginSuccess(
      String jobId, String leaseEpoch, String resultId) {
    return SubmitLeasedFileGroupExecutionResultStreamRequest.newBuilder()
        .setBegin(
            SubmitLeasedFileGroupExecutionResultStreamRequest.Begin.newBuilder()
                .setJobId(jobId)
                .setLeaseEpoch(leaseEpoch)
                .setResultId(resultId)
                .setOutcomeKind(
                    SubmitLeasedFileGroupExecutionResultStreamRequest.OutcomeKind.OK_SUCCESS))
        .build();
  }

  private static SubmitLeasedFileGroupExecutionResultStreamRequest beginFailure(
      String jobId, String leaseEpoch, String resultId, String message) {
    return SubmitLeasedFileGroupExecutionResultStreamRequest.newBuilder()
        .setBegin(
            SubmitLeasedFileGroupExecutionResultStreamRequest.Begin.newBuilder()
                .setJobId(jobId)
                .setLeaseEpoch(leaseEpoch)
                .setResultId(resultId)
                .setOutcomeKind(
                    SubmitLeasedFileGroupExecutionResultStreamRequest.OutcomeKind.OK_FAILURE)
                .setFailureMessage(message))
        .build();
  }

  private static SubmitLeasedFileGroupExecutionResultStreamRequest statsChunk(
      TargetStatsRecord... records) {
    return SubmitLeasedFileGroupExecutionResultStreamRequest.newBuilder()
        .setStatsChunk(
            SubmitLeasedFileGroupExecutionResultStreamRequest.StatsChunk.newBuilder()
                .addAllStatsRecords(List.of(records)))
        .build();
  }

  private static SubmitLeasedFileGroupExecutionResultStreamRequest artifactBegin(
      String filePath, String artifactUri) {
    return SubmitLeasedFileGroupExecutionResultStreamRequest.newBuilder()
        .setIndexArtifactBegin(
            SubmitLeasedFileGroupExecutionResultStreamRequest.IndexArtifactBegin.newBuilder()
                .setRecord(indexArtifactRecord(filePath, artifactUri))
                .setContentType("application/x-parquet"))
        .build();
  }

  private static SubmitLeasedFileGroupExecutionResultStreamRequest artifactContent(byte[] content) {
    return SubmitLeasedFileGroupExecutionResultStreamRequest.newBuilder()
        .setIndexArtifactContent(
            SubmitLeasedFileGroupExecutionResultStreamRequest.IndexArtifactContent.newBuilder()
                .setContent(ByteString.copyFrom(content)))
        .build();
  }

  private static SubmitLeasedFileGroupExecutionResultStreamRequest artifactEnd() {
    return SubmitLeasedFileGroupExecutionResultStreamRequest.newBuilder()
        .setIndexArtifactEnd(
            SubmitLeasedFileGroupExecutionResultStreamRequest.IndexArtifactEnd.newBuilder())
        .build();
  }

  private static SubmitLeasedFileGroupExecutionResultStreamRequest endFrame() {
    return SubmitLeasedFileGroupExecutionResultStreamRequest.newBuilder()
        .setEnd(SubmitLeasedFileGroupExecutionResultStreamRequest.End.newBuilder())
        .build();
  }

  private static TargetStatsRecord fileStats(String filePath) {
    return TargetStatsRecord.newBuilder()
        .setTableId(TABLE_ID)
        .setSnapshotId(55L)
        .setTarget(
            StatsTarget.newBuilder().setFile(FileStatsTarget.newBuilder().setFilePath(filePath)))
        .setFile(
            FileTargetStats.newBuilder()
                .setTableId(TABLE_ID)
                .setSnapshotId(55L)
                .setFilePath(filePath))
        .build();
  }

  private static IndexArtifactRecord indexArtifactRecord(String filePath, String artifactUri) {
    return IndexArtifactRecord.newBuilder()
        .setTableId(TABLE_ID)
        .setSnapshotId(55L)
        .setTarget(
            IndexTarget.newBuilder().setFile(IndexFileTarget.newBuilder().setFilePath(filePath)))
        .setArtifactUri(artifactUri)
        .setArtifactFormat("parquet")
        .setArtifactFormatVersion(1)
        .setCoverage(IndexCoverage.getDefaultInstance())
        .build();
  }

  private static final class InMemoryIdempotencyRepository implements IdempotencyRepository {
    private final Map<String, IdempotencyRecord> records = new ConcurrentHashMap<>();

    @Override
    public Optional<IdempotencyRecord> get(String key) {
      return Optional.ofNullable(records.get(key));
    }

    @Override
    public boolean createPending(
        String accountId,
        String key,
        String opName,
        String requestHash,
        Timestamp createdAt,
        Timestamp expiresAt) {
      return records.putIfAbsent(
              key,
              IdempotencyRecord.newBuilder()
                  .setOpName(opName)
                  .setRequestHash(requestHash)
                  .setStatus(IdempotencyRecord.Status.PENDING)
                  .setCreatedAt(createdAt)
                  .setExpiresAt(expiresAt)
                  .build())
          == null;
    }

    @Override
    public void finalizeSuccess(
        String accountId,
        String key,
        String opName,
        String requestHash,
        ResourceId resourceId,
        MutationMeta meta,
        byte[] payloadBytes,
        Timestamp createdAt,
        Timestamp expiresAt) {
      records.put(
          key,
          IdempotencyRecord.newBuilder()
              .setOpName(opName)
              .setRequestHash(requestHash)
              .setStatus(IdempotencyRecord.Status.SUCCEEDED)
              .setResourceId(resourceId)
              .setMeta(meta)
              .setPayload(ByteString.copyFrom(payloadBytes))
              .setCreatedAt(createdAt)
              .setExpiresAt(expiresAt)
              .build());
    }

    @Override
    public boolean delete(String key) {
      return records.remove(key) != null;
    }
  }
}
