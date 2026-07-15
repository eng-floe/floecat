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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CONNECTOR;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.TABLE;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.PutIndexArtifactItem;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.reconciler.impl.FileGroupExecutionSupport;
import ai.floedb.floecat.reconciler.impl.ReconcileLeaseGrpcStatus;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.StandaloneFileGroupExecutionPayload;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileIndexArtifactResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultResponse;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import ai.floedb.floecat.storage.spi.BlobStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class LeasedFileGroupExecutionService extends BaseServiceImpl {
  private static final Logger LOG = Logger.getLogger(LeasedFileGroupExecutionService.class);
  @Inject ReconcileJobStore jobs;
  @Inject TableRepository tableRepo;
  @Inject ConnectorRepository connectorRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject CredentialResolver credentialResolver;
  @Inject StatsStore statsStore;
  @Inject IndexArtifactRepository indexArtifactRepo;
  @Inject BlobStore blobStore;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject StatsOrchestrator statsOrchestrator;

  public StandaloneFileGroupExecutionPayload resolve(
      PrincipalContext principalContext, String jobId, String leaseEpoch) {
    String corr = principalContext.getCorrelationId();
    ReconcileJobStore.LeasedJob lease = requireLeasedFileGroupJob(corr, jobId, leaseEpoch);
    ReconcileFileGroupTask plannedTask = resolvePlannedTask(lease);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(plannedTask.tableId())
            .build();
    Table table =
        tableRepo
            .getById(tableId)
            .orElseThrow(
                () -> GrpcErrors.notFound(corr, TABLE, Map.of("table_id", tableId.getId())));
    if (!table.hasUpstream() || !table.getUpstream().hasConnectorId()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("table upstream connector metadata is required for file-group execution")
          .asRuntimeException();
    }
    ResourceId connectorId = table.getUpstream().getConnectorId();
    Connector connector =
        connectorRepo
            .getById(connectorId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        corr, CONNECTOR, Map.of("connector_id", connectorId.getId())));
    Connector resolvedConnector = resolvedConnectorPayload(connector, table);
    return new StandaloneFileGroupExecutionPayload(
        lease.jobId,
        lease.leaseEpoch,
        lease.parentJobId,
        resolvedConnector,
        String.join(".", table.getUpstream().getNamespacePathList()),
        table.getUpstream().getTableDisplayName(),
        resolvePayloadStorageLocation(table),
        tableId,
        plannedTask.snapshotId(),
        plannedTask.planId(),
        plannedTask.groupId(),
        plannedTask.filePaths(),
        FileGroupExecutionSupport.effectiveCapturePolicy(lease));
  }

  private Connector withTableStorageLocation(Connector connector, Table table) {
    if (connector == null
        || table == null
        || connector.getKind() != ConnectorKind.CK_DELTA
        || !table.getPropertiesMap().containsKey("storage_location")) {
      return connector;
    }
    String storageLocation = table.getPropertiesMap().get("storage_location");
    if (storageLocation == null || storageLocation.isBlank()) {
      return connector;
    }
    return connector.toBuilder().putProperties("storage_location", storageLocation).build();
  }

  private Connector resolvedConnectorPayload(Connector connector, Table table) {
    ConnectorConfig resolved = resolveCredentials(connector);
    Connector payload =
        connector.toBuilder()
            .putAllProperties(resolved.options())
            .setAuth(toAuthConfig(resolved.auth()))
            .build();
    return withTableStorageLocation(payload, table);
  }

  private String resolvePayloadStorageLocation(Table table) {
    if (table == null) {
      return "";
    }
    String location = firstNonBlank(table.getPropertiesMap().get("storage_location"));
    if (location != null) {
      return location;
    }
    location = firstNonBlank(table.getPropertiesMap().get("location"));
    if (location != null) {
      return location;
    }
    location = firstNonBlank(table.getPropertiesMap().get("delta.table-root"));
    if (location != null) {
      return location;
    }
    location = firstNonBlank(table.getPropertiesMap().get("external.location"));
    if (location != null) {
      return location;
    }
    location = deriveTableRootLocation(table.getPropertiesMap().get("source_metadata_location"));
    if (!location.isBlank()) {
      return location;
    }
    if (snapshotRepo != null) {
      location =
          snapshotRepo
              .latestRegisteredSnapshot(table.getResourceId())
              .map(SnapshotRepository::metadataLocation)
              .map(LeasedFileGroupExecutionService::deriveTableRootLocation)
              .orElse("");
      if (!location.isBlank()) {
        return location;
      }
    }
    return firstNonBlank(table.hasUpstream() ? table.getUpstream().getUri() : null, "");
  }

  private static String deriveTableRootLocation(String location) {
    String normalized = firstNonBlank(location);
    if (normalized == null) {
      return "";
    }
    int metadataSegment = normalized.indexOf("/metadata/");
    if (metadataSegment > 0) {
      return normalized.substring(0, metadataSegment);
    }
    int deltaLogSegment = normalized.indexOf("/_delta_log/");
    if (deltaLogSegment > 0) {
      return normalized.substring(0, deltaLogSegment);
    }
    if (normalized.endsWith("/metadata")) {
      return normalized.substring(0, normalized.length() - "/metadata".length());
    }
    if (normalized.endsWith("/_delta_log")) {
      return normalized.substring(0, normalized.length() - "/_delta_log".length());
    }
    return normalized;
  }

  private static String firstNonBlank(String value) {
    return firstNonBlank(value, null);
  }

  private static String firstNonBlank(String value, String defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? defaultValue : trimmed;
  }

  public boolean persistChunk(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      int chunkIndex,
      List<TargetStatsRecord> statsRecords,
      List<ai.floedb.floecat.reconciler.rpc.LeasedFileGroupIndexArtifact> indexArtifacts) {
    long totalStartNanos = System.nanoTime();
    String corr = principalContext.getCorrelationId();
    ReconcileJobStore.LeasedJob lease = requireLeasedFileGroupJob(corr, jobId, leaseEpoch);
    ReconcileFileGroupTask plannedTask = resolvePlannedTask(lease);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(plannedTask.tableId())
            .build();
    String requiredResultId = requireResultId(resultId);
    List<TargetStatsRecord> nonNullStats = nonNullStatsRecords(statsRecords);
    List<TargetStatsRecord> fileStats = fileScopedStatsRecords(nonNullStats);
    List<TargetStatsRecord> partialAggregates = partialAggregateRecords(nonNullStats);
    List<ai.floedb.floecat.reconciler.rpc.LeasedFileGroupIndexArtifact> nonNullArtifacts =
        indexArtifacts == null
            ? List.of()
            : indexArtifacts.stream().filter(java.util.Objects::nonNull).toList();
    SubmitLeasedFileGroupExecutionResultRequest.Chunk stagedChunk =
        chunkPayload(requiredResultId, chunkIndex, nonNullStats, nonNullArtifacts);
    byte[] requestBytes = stagedChunk.toByteArray();
    ChunkPersistMetrics metrics = new ChunkPersistMetrics();
    metrics.statsRecords = nonNullStats.size();
    metrics.fileStatsRecords = fileStats.size();
    metrics.partialAggregateRecords = partialAggregates.size();
    metrics.indexArtifacts = nonNullArtifacts.size();
    boolean accepted;
    long idempotentStartNanos = System.nanoTime();
    accepted =
        runIdempotentCreate(
                    () ->
                        MutationOps.createProto(
                            principalContext.getAccountId(),
                            "SubmitLeasedFileGroupExecutionResult",
                            chunkIdempotencyKey(jobId, requiredResultId, chunkIndex),
                            () -> requestBytes,
                            () -> {
                              long creatorStartNanos = System.nanoTime();
                              long snapshotId = plannedTask.snapshotId();
                              if (lease.fullRescan) {
                                validateFileStats(tableId, snapshotId, fileStats);
                              } else {
                                persistTargetStats(
                                    principalContext,
                                    tableId,
                                    snapshotId,
                                    requiredResultId,
                                    fileStats,
                                    metrics);
                              }
                              persistIndexArtifacts(
                                  principalContext,
                                  tableId,
                                  snapshotId,
                                  requiredResultId,
                                  parseIndexArtifacts(nonNullArtifacts),
                                  metrics);
                              metrics.creatorNanos += System.nanoTime() - creatorStartNanos;
                              return new IdempotencyGuard.CreateResult<>(stagedChunk, tableId);
                            },
                            ignored -> MutationMeta.getDefaultInstance(),
                            idempotencyStore,
                            nowTs(),
                            idempotencyTtlSeconds(),
                            principalContext::getCorrelationId,
                            SubmitLeasedFileGroupExecutionResultRequest.Chunk::parseFrom))
                .body
            != null;
    metrics.idempotentNanos = System.nanoTime() - idempotentStartNanos;
    metrics.totalNanos = System.nanoTime() - totalStartNanos;
    LOG.infof(
        "submit_leased_file_group_chunk_timing jobId=%s resultId=%s chunkIndex=%d accepted=%s "
            + "statsRecords=%d fileStats=%d partialAggregates=%d indexArtifacts=%d "
            + "requestBytes=%d totalMs=%.3f "
            + "idempotentMs=%.3f creatorMs=%.3f mergeMs=%.3f statsMs=%.3f statsItemCount=%d "
            + "statsItemAvgMs=%.3f statsItemMaxMs=%.3f statsStorePutMs=%.3f "
            + "statsIdempotencyOverheadMs=%.3f indexMs=%.3f indexItemCount=%d "
            + "indexItemAvgMs=%.3f indexItemMaxMs=%.3f indexBlobPutMs=%.3f indexBlobHeadMs=%.3f "
            + "indexRepoPutMs=%.3f persistResultMs=%.3f",
        jobId,
        requiredResultId,
        chunkIndex,
        accepted,
        metrics.statsRecords,
        metrics.fileStatsRecords,
        metrics.partialAggregateRecords,
        metrics.indexArtifacts,
        requestBytes.length,
        millis(metrics.totalNanos),
        millis(metrics.idempotentNanos),
        millis(metrics.creatorNanos),
        millis(metrics.mergeNanos),
        millis(metrics.statsNanos),
        metrics.statsItemCount,
        averageMillis(metrics.statsNanos, metrics.statsItemCount),
        millis(metrics.statsItemMaxNanos),
        millis(metrics.statsStorePutNanos),
        millis(Math.max(0L, metrics.statsNanos - metrics.statsStorePutNanos)),
        millis(metrics.indexNanos),
        metrics.indexItemCount,
        averageMillis(metrics.indexNanos, metrics.indexItemCount),
        millis(metrics.indexItemMaxNanos),
        millis(metrics.indexBlobPutNanos),
        millis(metrics.indexBlobHeadNanos),
        millis(metrics.indexRepoPutNanos),
        millis(metrics.persistResultNanos));
    return accepted;
  }

  public boolean persistSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      int chunkCount,
      List<ReconcileFileResult> fileResults) {
    String corr = principalContext.getCorrelationId();
    ReconcileJobStore.LeasedJob lease = requireLeasedFileGroupJob(corr, jobId, leaseEpoch);
    ReconcileFileGroupTask plannedTask = resolvePlannedTask(lease);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(plannedTask.tableId())
            .build();
    String requiredResultId = requireResultId(resultId);
    List<ReconcileFileResult> validatedFileResults = validateFileResults(plannedTask, fileResults);
    int expectedChunkCount = Math.max(0, chunkCount);
    byte[] requestBytes =
        successPayload(requiredResultId, validatedFileResults, expectedChunkCount).toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedFileGroupExecutionResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () -> {
                      ReconcileFileGroupTask latestTask =
                          latestPersistedChildResult(lease, plannedTask);
                      List<SubmitLeasedFileGroupExecutionResultRequest.Chunk> stagedChunks =
                          loadStagedChunks(
                              principalContext, jobId, requiredResultId, expectedChunkCount);
                      List<TargetStatsRecord> partialAggregates =
                          stagedChunks.stream()
                              .flatMap(
                                  chunk ->
                                      partialAggregateRecords(chunk.getStatsRecordsList()).stream())
                              .toList();
                      List<TargetStatsRecord> fileStats =
                          stagedChunks.stream()
                              .flatMap(
                                  chunk ->
                                      fileScopedStatsRecords(chunk.getStatsRecordsList()).stream())
                              .toList();
                      List<TargetStatsRecord> mergedPartialAggregates =
                          mergedPartialAggregates(
                              tableId, latestTask.snapshotId(), latestTask, partialAggregates);
                      if (lease.fullRescan) {
                        persistDraftFileGroupStats(lease, tableId, latestTask, fileStats);
                      }
                      ReconcileFileGroupTask persistedTask =
                          latestTask
                              .withFileResults(validatedFileResults)
                              .withPartialAggregateRecords(mergedPartialAggregates);
                      jobs.persistFileGroupResult(lease.jobId, lease.leaseEpoch, persistedTask);
                      boolean accepted =
                          jobs.applyLeaseOutcome(
                              lease.jobId,
                              lease.leaseEpoch,
                              ReconcileJobStore.CompletionKind.SUCCEEDED,
                              System.currentTimeMillis(),
                              "Executed file group " + latestTask.groupId(),
                              0L,
                              0L,
                              0L,
                              0L,
                              0L,
                              0L,
                              fileGroupStatsProcessed(persistedTask));
                      requireAcceptedLeaseOutcome(accepted, lease.jobId);
                      return new IdempotencyGuard.CreateResult<>(
                          SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
                              .setAccepted(true)
                              .build(),
                          tableId);
                    },
                    ignored -> MutationMeta.getDefaultInstance(),
                    idempotencyStore,
                    nowTs(),
                    idempotencyTtlSeconds(),
                    principalContext::getCorrelationId,
                    SubmitLeasedFileGroupExecutionResultResponse::parseFrom))
        .body
        .getAccepted();
  }

  private static void requireAcceptedLeaseOutcome(boolean accepted, String jobId) {
    if (!accepted) {
      throw ReconcileLeaseGrpcStatus.leasePreconditionFailed(
          "reconcile lease is no longer valid for job " + jobId);
    }
  }

  private static long fileGroupStatsProcessed(ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null || fileGroupTask.fileResults() == null) {
      return 0L;
    }
    return fileGroupTask.fileResults().stream()
        .filter(java.util.Objects::nonNull)
        .mapToLong(ReconcileFileResult::statsProcessed)
        .sum();
  }

  public boolean persistFailure(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      String message) {
    String corr = principalContext.getCorrelationId();
    ReconcileJobStore.LeasedJob lease = requireLeasedFileGroupJob(corr, jobId, leaseEpoch);
    ReconcileFileGroupTask plannedTask = resolvePlannedTask(lease);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(plannedTask.tableId())
            .build();
    String requiredResultId = requireResultId(resultId);
    String effectiveMessage = message == null ? "" : message;
    byte[] requestBytes = failurePayload(requiredResultId, effectiveMessage).toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedFileGroupExecutionResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () -> {
                      jobs.persistFileGroupResult(
                          lease.jobId,
                          lease.leaseEpoch,
                          plannedTask
                              .withFileResults(
                                  FileGroupExecutionSupport.fileResultsForFailure(
                                      plannedTask, effectiveMessage))
                              .withPartialAggregateRecords(plannedTask.partialAggregateRecords()));
                      return new IdempotencyGuard.CreateResult<>(
                          SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
                              .setAccepted(true)
                              .build(),
                          tableId);
                    },
                    ignored -> MutationMeta.getDefaultInstance(),
                    idempotencyStore,
                    nowTs(),
                    idempotencyTtlSeconds(),
                    principalContext::getCorrelationId,
                    SubmitLeasedFileGroupExecutionResultResponse::parseFrom))
        .body
        .getAccepted();
  }

  private void persistTargetStats(
      PrincipalContext principalContext,
      ResourceId tableId,
      long snapshotId,
      String resultId,
      List<TargetStatsRecord> statsRecords,
      ChunkPersistMetrics metrics) {
    List<TargetStatsRecord> nonNullStats =
        statsRecords == null
            ? List.of()
            : statsRecords.stream().filter(java.util.Objects::nonNull).toList();
    if (nonNullStats.isEmpty()) {
      return;
    }
    long batchStartNanos = System.nanoTime();
    List<TargetStatsRecord> created =
        statsStore.putTargetStatsBatchIfAbsent(tableId, snapshotId, nonNullStats);
    if (!created.isEmpty()) {
      statsOrchestrator.invalidateStatsCache(tableId, snapshotId, created);
    }
    metrics.statsItemCount += created.size();
    long batchNanos = System.nanoTime() - batchStartNanos;
    metrics.statsNanos += batchNanos;
    metrics.statsStorePutNanos += batchNanos;
  }

  private void persistDraftFileGroupStats(
      ReconcileJobStore.LeasedJob lease,
      ResourceId tableId,
      ReconcileFileGroupTask plannedTask,
      List<TargetStatsRecord> fileStats) {
    long snapshotId = plannedTask.snapshotId();
    List<TargetStatsRecord> nonNullStats = validateFileStats(tableId, snapshotId, fileStats);
    Set<String> plannedPaths = new LinkedHashSet<>(plannedTask.filePaths());
    for (TargetStatsRecord record : nonNullStats) {
      if (!plannedPaths.contains(record.getFile().getFilePath())) {
        throw new IllegalArgumentException("file-group stats include an unplanned file");
      }
    }
    List<StatsTarget> targetsToReplace =
        plannedTask.filePaths().stream().map(path -> StatsTargetIdentity.fileTarget(path)).toList();
    statsStore.replaceTargetStatsInGeneration(
        tableId, snapshotId, statsGenerationId(lease), targetsToReplace, nonNullStats);
  }

  private static List<TargetStatsRecord> validateFileStats(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> fileStats) {
    List<TargetStatsRecord> nonNullStats = nonNullStatsRecords(fileStats);
    for (TargetStatsRecord record : nonNullStats) {
      if (!record.hasFile() || !record.hasTarget()) {
        throw new IllegalArgumentException("file-group stats must be file-target records");
      }
      if (!tableId.equals(record.getTableId()) || record.getSnapshotId() != snapshotId) {
        throw new IllegalArgumentException("file-group stats do not match file-group task");
      }
    }
    return nonNullStats;
  }

  static String statsGenerationId(ReconcileJobStore.LeasedJob lease) {
    String parentJobId = lease == null || lease.parentJobId == null ? "" : lease.parentJobId.trim();
    if (parentJobId.isBlank()) {
      throw new IllegalArgumentException(
          "parent reconcile job id is required for stats generation");
    }
    return "full-rescan-" + parentJobId;
  }

  private void persistIndexArtifacts(
      PrincipalContext principalContext,
      ResourceId tableId,
      long snapshotId,
      String resultId,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts,
      ChunkPersistMetrics metrics) {
    List<IndexArtifactRecord> persistedRecords = new java.util.ArrayList<>();
    long batchStartNanos = System.nanoTime();
    for (ReconcilerBackend.StagedIndexArtifact stagedArtifact : stagedIndexArtifacts) {
      if (stagedArtifact == null || stagedArtifact.record() == null) {
        continue;
      }
      PutIndexArtifactItem item = toPutIndexArtifactItem(stagedArtifact);
      persistedRecords.add(prepareIndexArtifactRecord(item, metrics));
    }
    if (persistedRecords.isEmpty()) {
      return;
    }
    long repoPutStartNanos = System.nanoTime();
    indexArtifactRepo.putIndexArtifactsBatch(persistedRecords);
    metrics.indexRepoPutNanos += System.nanoTime() - repoPutStartNanos;
    long batchNanos = System.nanoTime() - batchStartNanos;
    metrics.indexNanos += batchNanos;
    metrics.indexItemCount += persistedRecords.size();
  }

  private static AuthConfig toAuthConfig(ConnectorConfig.Auth resolved) {
    return AuthConfig.newBuilder()
        .setScheme(resolved.scheme() == null ? "" : resolved.scheme())
        .putAllProperties(resolved.props())
        .putAllHeaderHints(resolved.headerHints())
        .build();
  }

  private ConnectorConfig resolveCredentials(Connector connector) {
    ConnectorConfig base = ConnectorConfigMapper.fromProto(connector);
    AuthConfig auth = connector == null ? AuthConfig.getDefaultInstance() : connector.getAuth();
    if (auth.hasCredentials()
        && auth.getCredentials().getCredentialCase()
            != AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return CredentialResolverSupport.apply(base, auth.getCredentials());
    }
    if (connector == null
        || !connector.hasResourceId()
        || auth.getScheme().isBlank()
        || "none".equalsIgnoreCase(auth.getScheme())) {
      return base;
    }
    return credentialResolver
        .resolve(connector.getResourceId().getAccountId(), connector.getResourceId().getId())
        .map(c -> CredentialResolverSupport.apply(base, c, AuthResolutionContext.empty()))
        .orElse(base);
  }

  private IndexArtifactRecord prepareIndexArtifactRecord(
      PutIndexArtifactItem item, ChunkPersistMetrics metrics) {
    IndexArtifactRecord record = item.getRecord();
    String contentType =
        item.getContentType() == null || item.getContentType().isBlank()
            ? "application/x-parquet"
            : item.getContentType();
    if (!item.getContent().isEmpty()) {
      long blobPutStartNanos = System.nanoTime();
      blobStore.put(record.getArtifactUri(), item.getContent().toByteArray(), contentType);
      metrics.indexBlobPutNanos += System.nanoTime() - blobPutStartNanos;
    }
    long blobHeadStartNanos = System.nanoTime();
    String etag =
        blobStore
            .head(record.getArtifactUri())
            .map(head -> head.getEtag())
            .orElse(record.getContentEtag());
    metrics.indexBlobHeadNanos += System.nanoTime() - blobHeadStartNanos;
    return record.toBuilder().setContentEtag(etag).build();
  }

  private static double millis(long nanos) {
    return nanos / 1_000_000.0;
  }

  private static double averageMillis(long totalNanos, long count) {
    return count == 0 ? 0.0 : millis(totalNanos) / count;
  }

  private static final class ChunkPersistMetrics {
    private int statsRecords;
    private int fileStatsRecords;
    private int partialAggregateRecords;
    private int indexArtifacts;
    private long totalNanos;
    private long idempotentNanos;
    private long creatorNanos;
    private long mergeNanos;
    private long statsNanos;
    private long statsItemCount;
    private long statsItemMaxNanos;
    private long statsStorePutNanos;
    private long indexNanos;
    private long indexItemCount;
    private long indexItemMaxNanos;
    private long indexBlobPutNanos;
    private long indexBlobHeadNanos;
    private long indexRepoPutNanos;
    private long persistResultNanos;
  }

  private static PutIndexArtifactItem toPutIndexArtifactItem(
      ReconcilerBackend.StagedIndexArtifact stagedArtifact) {
    return PutIndexArtifactItem.newBuilder()
        .setRecord(stagedArtifact.record())
        .setContent(com.google.protobuf.ByteString.copyFrom(stagedArtifact.content()))
        .setContentType(stagedArtifact.contentType() == null ? "" : stagedArtifact.contentType())
        .build();
  }

  private static SubmitLeasedFileGroupExecutionResultRequest.Chunk chunkPayload(
      String resultId,
      int chunkIndex,
      List<TargetStatsRecord> statsRecords,
      List<ai.floedb.floecat.reconciler.rpc.LeasedFileGroupIndexArtifact> indexArtifacts) {
    return SubmitLeasedFileGroupExecutionResultRequest.Chunk.newBuilder()
        .setResultId(resultId)
        .setChunkIndex(Math.max(0, chunkIndex))
        .addAllStatsRecords(nonNullStatsRecords(statsRecords))
        .addAllIndexArtifacts(indexArtifacts == null ? List.of() : indexArtifacts)
        .build();
  }

  private static List<TargetStatsRecord> nonNullStatsRecords(List<TargetStatsRecord> statsRecords) {
    if (statsRecords == null || statsRecords.isEmpty()) {
      return List.of();
    }
    return statsRecords.stream().filter(java.util.Objects::nonNull).toList();
  }

  private static List<TargetStatsRecord> fileScopedStatsRecords(
      List<TargetStatsRecord> statsRecords) {
    List<TargetStatsRecord> nonNullStats = nonNullStatsRecords(statsRecords);
    return nonNullStats.stream().filter(TargetStatsRecord::hasFile).toList();
  }

  private static List<TargetStatsRecord> partialAggregateRecords(
      List<TargetStatsRecord> statsRecords) {
    List<TargetStatsRecord> nonNullStats = nonNullStatsRecords(statsRecords);
    return nonNullStats.stream().filter(record -> !record.hasFile()).toList();
  }

  private static List<TargetStatsRecord> mergedPartialAggregates(
      ResourceId tableId,
      long snapshotId,
      ReconcileFileGroupTask plannedTask,
      List<TargetStatsRecord> chunkPartials) {
    List<TargetStatsRecord> nonNullChunkPartials = nonNullStatsRecords(chunkPartials);
    if (nonNullChunkPartials.isEmpty()) {
      return plannedTask.partialAggregateRecords();
    }
    List<TargetStatsRecord> out =
        new java.util.ArrayList<>(
            plannedTask.partialAggregateRecords().size() + nonNullChunkPartials.size());
    out.addAll(plannedTask.partialAggregateRecords());
    out.addAll(nonNullChunkPartials);
    return dedupePartialAggregatesByTarget(tableId, snapshotId, out);
  }

  private static List<TargetStatsRecord> dedupePartialAggregatesByTarget(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    LinkedHashMap<String, TargetStatsRecord> byTarget = new LinkedHashMap<>();
    for (TargetStatsRecord record : nonNullStatsRecords(records)) {
      if (record.hasFile() || !record.hasTarget()) {
        continue;
      }
      if (!tableId.equals(record.getTableId()) || record.getSnapshotId() != snapshotId) {
        throw new IllegalArgumentException("partial aggregate stats do not match file-group task");
      }
      byTarget.put(StatsTargetIdentity.storageId(record.getTarget()), record);
    }
    return List.copyOf(byTarget.values());
  }

  private List<ReconcilerBackend.StagedIndexArtifact> parseIndexArtifacts(
      List<ai.floedb.floecat.reconciler.rpc.LeasedFileGroupIndexArtifact> indexArtifacts) {
    List<ReconcilerBackend.StagedIndexArtifact> inlineArtifacts = new java.util.ArrayList<>();
    for (var artifact : indexArtifacts) {
      if (artifact.getContent().isEmpty() && artifact.getRecord().getArtifactUri().isBlank()) {
        throw new IllegalArgumentException(
            "inline index artifact content is required when artifact_uri is blank");
      }
      inlineArtifacts.add(
          new ReconcilerBackend.StagedIndexArtifact(
              artifact.getRecord(),
              artifact.getContent().toByteArray(),
              artifact.getContentType()));
    }
    return List.copyOf(inlineArtifacts);
  }

  private static List<ReconcileFileResult> validateFileResults(
      ReconcileFileGroupTask plannedTask, List<ReconcileFileResult> fileResults) {
    LinkedHashMap<String, ReconcileFileResult> byFile = new LinkedHashMap<>();
    for (ReconcileFileResult fileResult :
        fileResults == null ? List.<ReconcileFileResult>of() : fileResults) {
      if (fileResult == null || fileResult.filePath().isBlank()) {
        throw new IllegalArgumentException("file-group success file_path is required");
      }
      if (byFile.putIfAbsent(fileResult.filePath(), fileResult) != null) {
        throw new IllegalArgumentException(
            "duplicate file-group success file result for " + fileResult.filePath());
      }
    }
    List<String> plannedFiles = plannedTask.filePaths();
    if (byFile.size() != plannedFiles.size()) {
      throw new IllegalArgumentException(
          "file-group success file result count does not match plan");
    }
    for (String plannedFile : plannedFiles) {
      if (!byFile.containsKey(plannedFile)) {
        throw new IllegalArgumentException(
            "file-group success is missing file result for planned file " + plannedFile);
      }
    }
    return plannedFiles.stream().map(byFile::get).toList();
  }

  private static SubmitLeasedFileGroupExecutionResultRequest.Success successPayload(
      String resultId, List<ReconcileFileResult> fileResults, int chunkCount) {
    return SubmitLeasedFileGroupExecutionResultRequest.Success.newBuilder()
        .setResultId(resultId)
        .setChunkCount(Math.max(0, chunkCount))
        .addAllFileResults(
            fileResults.stream().map(LeasedFileGroupExecutionService::toProtoFileResult).toList())
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileFileResult toProtoFileResult(
      ReconcileFileResult fileResult) {
    ReconcileFileResult effective = fileResult == null ? ReconcileFileResult.empty() : fileResult;
    return ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.newBuilder()
        .setFilePath(effective.filePath())
        .setState(
            switch (effective.state()) {
              case SUCCEEDED ->
                  ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_SUCCEEDED;
              case FAILED -> ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_FAILED;
              case SKIPPED ->
                  ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_SKIPPED;
              case UNSPECIFIED ->
                  ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_UNSPECIFIED;
            })
        .setStatsProcessed(effective.statsProcessed())
        .setMessage(effective.message())
        .setIndexArtifact(toProtoIndexArtifact(effective.indexArtifact()))
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.ReconcileIndexArtifactResult
      toProtoIndexArtifact(ReconcileIndexArtifactResult indexArtifact) {
    ReconcileIndexArtifactResult effective =
        indexArtifact == null ? ReconcileIndexArtifactResult.empty() : indexArtifact;
    return ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.ReconcileIndexArtifactResult
        .newBuilder()
        .setArtifactUri(effective.artifactUri())
        .setArtifactFormat(effective.artifactFormat())
        .setArtifactFormatVersion(effective.artifactFormatVersion())
        .build();
  }

  private static SubmitLeasedFileGroupExecutionResultRequest.Failure failurePayload(
      String resultId, String message) {
    return SubmitLeasedFileGroupExecutionResultRequest.Failure.newBuilder()
        .setResultId(resultId)
        .setMessage(message == null ? "" : message)
        .build();
  }

  private static String chunkIdempotencyKey(String jobId, String resultId, int chunkIndex) {
    return resultIdempotencyKey(jobId, resultId) + ":chunk:" + Math.max(0, chunkIndex);
  }

  private List<SubmitLeasedFileGroupExecutionResultRequest.Chunk> loadStagedChunks(
      PrincipalContext principalContext, String jobId, String resultId, int chunkCount) {
    if (chunkCount <= 0) {
      requireNoStagedChunkAt(principalContext, jobId, resultId, 0, chunkCount);
      return List.of();
    }
    List<SubmitLeasedFileGroupExecutionResultRequest.Chunk> chunks =
        new java.util.ArrayList<>(chunkCount);
    for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
      chunks.add(loadStagedChunk(principalContext, jobId, resultId, chunkIndex));
    }
    // A too-low chunk_count would silently drop the partial aggregates of any staged chunk at
    // index >= chunkCount. Probe the next index and fail loudly (terminal) rather than under-count
    // the rollup. (A too-high chunk_count is caught by loadStagedChunk's retryable abort above,
    // which self-heals when the lease expires and the group re-executes.)
    requireNoStagedChunkAt(principalContext, jobId, resultId, chunkCount, chunkCount);
    return List.copyOf(chunks);
  }

  private void requireNoStagedChunkAt(
      PrincipalContext principalContext,
      String jobId,
      String resultId,
      int chunkIndex,
      int declaredChunkCount) {
    if (stagedChunkExists(principalContext, jobId, resultId, chunkIndex)) {
      throw new IllegalArgumentException(
          "file-group success declared chunk_count="
              + declaredChunkCount
              + " but a staged chunk exists at index "
              + chunkIndex
              + " for result "
              + resultId
              + "; the declared count is too low and would drop that chunk's partial aggregates");
    }
  }

  private boolean stagedChunkExists(
      PrincipalContext principalContext, String jobId, String resultId, int chunkIndex) {
    String idempotencyKey =
        Keys.idempotencyKey(
            principalContext.getAccountId(),
            "SubmitLeasedFileGroupExecutionResult",
            chunkIdempotencyKey(jobId, resultId, chunkIndex));
    return idempotencyStore
        .get(idempotencyKey)
        .map(record -> record.getStatus() == IdempotencyRecord.Status.SUCCEEDED)
        .orElse(false);
  }

  private SubmitLeasedFileGroupExecutionResultRequest.Chunk loadStagedChunk(
      PrincipalContext principalContext, String jobId, String resultId, int chunkIndex) {
    String idempotencyKey =
        Keys.idempotencyKey(
            principalContext.getAccountId(),
            "SubmitLeasedFileGroupExecutionResult",
            chunkIdempotencyKey(jobId, resultId, chunkIndex));
    IdempotencyRecord record =
        idempotencyStore
            .get(idempotencyKey)
            .orElseThrow(
                () ->
                    new StorageAbortRetryableException(
                        "file-group result chunk not yet staged: key=" + idempotencyKey));
    if (record.getStatus() != IdempotencyRecord.Status.SUCCEEDED) {
      throw new StorageAbortRetryableException(
          "file-group result chunk is not complete: key=" + idempotencyKey);
    }
    try {
      SubmitLeasedFileGroupExecutionResultRequest.Chunk chunk =
          SubmitLeasedFileGroupExecutionResultRequest.Chunk.parseFrom(record.getPayload());
      if (!resultId.equals(chunk.getResultId()) || chunk.getChunkIndex() != chunkIndex) {
        throw new IllegalArgumentException("staged file-group result chunk identity mismatch");
      }
      return chunk;
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new ai.floedb.floecat.service.repo.util.BaseResourceRepository.CorruptionException(
          "failed to parse staged file-group result chunk", e);
    }
  }

  private ReconcileJobStore.LeasedJob requireLeasedFileGroupJob(
      String corr, String jobId, String leaseEpoch) {
    boolean renewed = jobs.renewLease(jobId, leaseEpoch);
    if (!renewed) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile lease is no longer valid")
          .asRuntimeException();
    }
    ReconcileJobStore.ReconcileJob job =
        jobs.getLeaseView(jobId)
            .orElseThrow(() -> GrpcErrors.notFound(corr, TABLE, Map.of("job_id", jobId)));
    if (job.jobKind != ReconcileJobKind.EXEC_FILE_GROUP) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is not an EXEC_FILE_GROUP job")
          .asRuntimeException();
    }
    if (!isActiveLeasedState(job.state)) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "reconcile job is no longer active for lease "
                  + jobId
                  + " state="
                  + (job.state == null ? "" : job.state))
          .asRuntimeException();
    }
    return new ReconcileJobStore.LeasedJob(
        job.jobId,
        job.accountId,
        job.connectorId,
        job.fullRescan,
        job.captureMode == null
            ? ReconcilerService.CaptureMode.METADATA_AND_CAPTURE
            : job.captureMode,
        job.scope,
        job.executionPolicy,
        leaseEpoch,
        "",
        job.executorId,
        job.jobKind,
        job.tableTask,
        job.viewTask,
        job.snapshotTask,
        job.fileGroupTask,
        job.parentJobId);
  }

  private static String resultIdempotencyKey(String jobId, String resultId) {
    return (jobId == null ? "" : jobId.trim()) + ":" + resultId;
  }

  private static String requireResultId(String resultId) {
    if (resultId == null || resultId.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("result_id is required for file-group result submission")
          .asRuntimeException();
    }
    return resultId.trim();
  }

  private ReconcileFileGroupTask resolvePlannedTask(ReconcileJobStore.LeasedJob lease) {
    ReconcileFileGroupTask task =
        lease == null || lease.fileGroupTask == null
            ? ReconcileFileGroupTask.empty()
            : lease.fileGroupTask;
    if (jobs == null
        || lease == null
        || lease.parentJobId == null
        || lease.parentJobId.isBlank()
        || lease.accountId == null
        || lease.accountId.isBlank()) {
      throw unresolvedPlannedTask();
    }
    return jobs.get(lease.accountId, lease.parentJobId)
        .map(parent -> parent.snapshotTask)
        .filter(snapshotTask -> snapshotTask != null && !snapshotTask.isEmpty())
        .flatMap(snapshotTask -> resolveFromParentSnapshotTask(snapshotTask, task))
        .map(parentTask -> mergePersistedChildResult(parentTask, task))
        .orElseThrow(this::unresolvedPlannedTask);
  }

  static ReconcileFileGroupTask mergePersistedChildResult(
      ReconcileFileGroupTask plannedTask, ReconcileFileGroupTask persistedTask) {
    ReconcileFileGroupTask effectivePlanned =
        plannedTask == null ? ReconcileFileGroupTask.empty() : plannedTask;
    ReconcileFileGroupTask effectivePersisted =
        persistedTask == null ? ReconcileFileGroupTask.empty() : persistedTask;
    return effectivePlanned
        .withFileStatsBlob(
            effectivePersisted.fileStatsBlobUri(), effectivePersisted.fileStatsRecordCount())
        .withFileResults(effectivePersisted.fileResults())
        .withPartialAggregateRecords(effectivePersisted.partialAggregateRecords());
  }

  private ReconcileFileGroupTask latestPersistedChildResult(
      ReconcileJobStore.LeasedJob lease, ReconcileFileGroupTask plannedTask) {
    if (jobs == null || lease == null || lease.accountId == null || lease.jobId == null) {
      return plannedTask == null ? ReconcileFileGroupTask.empty() : plannedTask;
    }
    return jobs.get(lease.accountId, lease.jobId)
        .map(job -> mergePersistedChildResult(plannedTask, job.fileGroupTask))
        .orElse(plannedTask == null ? ReconcileFileGroupTask.empty() : plannedTask);
  }

  private static java.util.Optional<ReconcileFileGroupTask> resolveFromParentSnapshotTask(
      ReconcileSnapshotTask snapshotTask, ReconcileFileGroupTask task) {
    if (snapshotTask == null || snapshotTask.isEmpty() || task == null || task.isEmpty()) {
      return java.util.Optional.empty();
    }
    return snapshotTask.fileGroups().stream()
        .filter(group -> group != null && !group.isEmpty())
        .filter(group -> group.groupId().equals(task.groupId()))
        .filter(group -> group.planId().equals(task.planId()))
        .findFirst();
  }

  private StatusRuntimeException unresolvedPlannedTask() {
    return Status.FAILED_PRECONDITION
        .withDescription("planned file group could not be resolved from parent snapshot plan")
        .asRuntimeException();
  }

  private static boolean isActiveLeasedState(String state) {
    return "JS_RUNNING".equals(state) || "JS_CANCELLING".equals(state);
  }
}
