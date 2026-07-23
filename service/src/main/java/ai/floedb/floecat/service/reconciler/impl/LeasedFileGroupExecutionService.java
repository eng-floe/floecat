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
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
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
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultPayload;
import ai.floedb.floecat.reconciler.rpc.FileGroupStatsPayload;
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
import ai.floedb.floecat.storage.spi.BlobStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ApplicationScoped
public class LeasedFileGroupExecutionService extends BaseServiceImpl {
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
        Keys.reconcileFileGroupResultPayloadUri(
            lease.accountId, lease.parentJobId, lease.jobId, lease.leaseEpoch),
        Keys.reconcileFileGroupStatsPayloadUri(
            lease.accountId, lease.parentJobId, lease.jobId, lease.leaseEpoch),
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

  public boolean persistSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      ReconcileFileGroupResultDescriptor descriptor) {
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
    ReconcileFileGroupResultDescriptor validated =
        validateResultDescriptor(lease, plannedTask, requiredResultId, descriptor);
    byte[] requestBytes = descriptorFingerprint(validated).getBytes(StandardCharsets.UTF_8);
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedFileGroupExecutionResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () -> {
                      FileGroupResultPayload resultPayload =
                          loadValidatedResultPayload(
                              lease, plannedTask, requiredResultId, validated);
                      persistIndexArtifacts(
                          lease,
                          tableId,
                          plannedTask.snapshotId(),
                          plannedTask,
                          resultPayload.getIndexArtifactsList().stream()
                              .map(
                                  record ->
                                      new ReconcilerBackend.StagedIndexArtifact(
                                          record, new byte[0], ""))
                              .toList());
                      List<TargetStatsRecord> stagedFileStats =
                          loadValidatedStatsPayload(
                              lease, plannedTask, requiredResultId, validated);
                      if (lease.fullRescan) {
                        persistDraftFileGroupStats(lease, tableId, plannedTask, stagedFileStats);
                      } else {
                        persistTargetStats(tableId, plannedTask.snapshotId(), stagedFileStats);
                      }
                      boolean accepted =
                          jobs.completeFileGroupSuccess(
                              lease.jobId,
                              lease.leaseEpoch,
                              validated,
                              System.currentTimeMillis(),
                              "Executed file group " + plannedTask.groupId());
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

  private ReconcileFileGroupResultDescriptor validateResultDescriptor(
      ReconcileJobStore.LeasedJob lease,
      ReconcileFileGroupTask plannedTask,
      String resultId,
      ReconcileFileGroupResultDescriptor descriptor) {
    if (descriptor == null || descriptor.isEmpty() || descriptor.formatVersion() != 1) {
      throw new IllegalArgumentException("file-group result descriptor format_version must be 1");
    }
    String expectedUri =
        Keys.reconcileFileGroupResultPayloadUri(
            lease.accountId, lease.parentJobId, lease.jobId, lease.leaseEpoch);
    String expectedStatsUri =
        Keys.reconcileFileGroupStatsPayloadUri(
            lease.accountId, lease.parentJobId, lease.jobId, lease.leaseEpoch);
    if (!lease.accountId.equals(descriptor.accountId())
        || !lease.connectorId.equals(descriptor.connectorId())
        || !lease.parentJobId.equals(descriptor.parentJobId())
        || !lease.jobId.equals(descriptor.fileGroupJobId())
        || !plannedTask.planId().equals(descriptor.planId())
        || !plannedTask.groupId().equals(descriptor.groupId())
        || !plannedTask.tableId().equals(descriptor.tableId())
        || plannedTask.snapshotId() != descriptor.snapshotId()
        || !lease.leaseEpoch.equals(descriptor.leaseEpoch())
        || !resultId.equals(descriptor.resultId())) {
      throw new IllegalArgumentException("file-group result descriptor identity mismatch");
    }
    if (!expectedUri.equals(descriptor.payloadUri())) {
      throw new IllegalArgumentException(
          "file-group result descriptor payload_uri is outside the leased result location");
    }
    if (!expectedStatsUri.equals(descriptor.statsPayloadUri())) {
      throw new IllegalArgumentException(
          "file-group result descriptor stats_payload_uri is outside the leased stats location");
    }
    if (descriptor.payloadBytes() <= 0L
        || descriptor.payloadSha256() == null
        || descriptor.payloadSha256().isBlank()
        || descriptor.statsPayloadBytes() <= 0L
        || descriptor.statsPayloadSha256() == null
        || descriptor.statsPayloadSha256().isBlank()
        || descriptor.fileStatsRecordCount() < 0) {
      throw new IllegalArgumentException(
          "file-group result descriptor payload sizes, sha256 values, and stats count are required");
    }
    int plannedCount = plannedTask.filePaths().size();
    if (descriptor.plannedFileCount() != plannedCount
        || descriptor.succeededFileCount() != plannedCount
        || descriptor.failedFileCount() != 0
        || descriptor.skippedFileCount() != 0) {
      throw new IllegalArgumentException(
          "file-group result descriptor outcome counts do not match successful plan");
    }
    var header =
        blobStore
            .head(expectedUri)
            .orElseThrow(
                () ->
                    new StorageAbortRetryableException(
                        "file-group result object is not committed jobId="
                            + lease.jobId
                            + " uri="
                            + expectedUri));
    if (header.getContentLength() != descriptor.payloadBytes()) {
      throw new IllegalArgumentException(
          "file-group result object size mismatch jobId=" + lease.jobId + " uri=" + expectedUri);
    }
    var statsHeader =
        blobStore
            .head(expectedStatsUri)
            .orElseThrow(
                () ->
                    new StorageAbortRetryableException(
                        "file-group stats object is not committed jobId="
                            + lease.jobId
                            + " uri="
                            + expectedStatsUri));
    if (statsHeader.getContentLength() != descriptor.statsPayloadBytes()) {
      throw new IllegalArgumentException(
          "file-group stats object size mismatch jobId="
              + lease.jobId
              + " uri="
              + expectedStatsUri);
    }
    return descriptor;
  }

  private static String descriptorFingerprint(ReconcileFileGroupResultDescriptor descriptor) {
    return descriptor.fileGroupJobId()
        + "\n"
        + descriptor.leaseEpoch()
        + "\n"
        + descriptor.resultId()
        + "\n"
        + descriptor.payloadUri()
        + "\n"
        + descriptor.payloadBytes()
        + "\n"
        + descriptor.payloadSha256()
        + "\n"
        + descriptor.statsPayloadUri()
        + "\n"
        + descriptor.statsPayloadBytes()
        + "\n"
        + descriptor.statsPayloadSha256()
        + "\n"
        + descriptor.fileStatsRecordCount();
  }

  private FileGroupResultPayload loadValidatedResultPayload(
      ReconcileJobStore.LeasedJob lease,
      ReconcileFileGroupTask plannedTask,
      String resultId,
      ReconcileFileGroupResultDescriptor descriptor) {
    byte[] bytes = blobStore.get(descriptor.payloadUri());
    if (bytes.length != descriptor.payloadBytes()) {
      throw new IllegalArgumentException(
          "file-group result payload size does not match descriptor");
    }
    String actualSha256 = Base64.getEncoder().encodeToString(sha256(bytes));
    if (!MessageDigest.isEqual(
        actualSha256.getBytes(StandardCharsets.US_ASCII),
        descriptor.payloadSha256().getBytes(StandardCharsets.US_ASCII))) {
      throw new IllegalArgumentException(
          "file-group result payload sha256 does not match descriptor");
    }
    final FileGroupResultPayload payload;
    try {
      payload = FileGroupResultPayload.parseFrom(bytes);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("file-group result payload is not valid protobuf", e);
    }
    if (payload.getFormatVersion() != 1
        || !lease.accountId.equals(payload.getAccountId())
        || !lease.connectorId.equals(payload.getConnectorId())
        || !lease.parentJobId.equals(payload.getParentJobId())
        || !lease.jobId.equals(payload.getFileGroupJobId())
        || !plannedTask.planId().equals(payload.getPlanId())
        || !plannedTask.groupId().equals(payload.getGroupId())
        || !plannedTask.tableId().equals(payload.getTableId())
        || plannedTask.snapshotId() != payload.getSnapshotId()
        || !lease.leaseEpoch.equals(payload.getLeaseEpoch())
        || !resultId.equals(payload.getResultId())) {
      throw new IllegalArgumentException("file-group result payload identity mismatch");
    }
    if (payload.getFileResultsCount() != descriptor.succeededFileCount()
        || payload.getPartialAggregateRecordsCount() != descriptor.partialAggregateRecordCount()
        || payload.getIndexArtifactsCount() != descriptor.indexArtifactCount()) {
      throw new IllegalArgumentException(
          "file-group result payload counts do not match descriptor");
    }
    Set<String> plannedPaths = new LinkedHashSet<>(plannedTask.filePaths());
    Set<String> resultPaths = new LinkedHashSet<>();
    for (var fileResult : payload.getFileResultsList()) {
      if (fileResult.getState()
              != ai.floedb.floecat.reconciler.rpc.ReconcileFileResult.State.RFRS_SUCCEEDED
          || !plannedPaths.contains(fileResult.getFilePath())
          || !resultPaths.add(fileResult.getFilePath())) {
        throw new IllegalArgumentException(
            "file-group result payload contains an invalid file result");
      }
    }
    if (!resultPaths.equals(plannedPaths)) {
      throw new IllegalArgumentException(
          "file-group result payload does not cover the planned files");
    }
    return payload;
  }

  private List<TargetStatsRecord> loadValidatedStatsPayload(
      ReconcileJobStore.LeasedJob lease,
      ReconcileFileGroupTask plannedTask,
      String resultId,
      ReconcileFileGroupResultDescriptor descriptor) {
    byte[] bytes = blobStore.get(descriptor.statsPayloadUri());
    if (bytes.length != descriptor.statsPayloadBytes()) {
      throw new IllegalArgumentException("file-group stats payload size does not match descriptor");
    }
    String actualSha256 = Base64.getEncoder().encodeToString(sha256(bytes));
    if (!MessageDigest.isEqual(
        actualSha256.getBytes(StandardCharsets.US_ASCII),
        descriptor.statsPayloadSha256().getBytes(StandardCharsets.US_ASCII))) {
      throw new IllegalArgumentException(
          "file-group stats payload sha256 does not match descriptor");
    }
    final FileGroupStatsPayload payload;
    try {
      payload = FileGroupStatsPayload.parseFrom(bytes);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("file-group stats payload is not valid protobuf", e);
    }
    if (payload.getFormatVersion() != 1
        || !lease.accountId.equals(payload.getAccountId())
        || !lease.connectorId.equals(payload.getConnectorId())
        || !lease.parentJobId.equals(payload.getParentJobId())
        || !lease.jobId.equals(payload.getFileGroupJobId())
        || !plannedTask.planId().equals(payload.getPlanId())
        || !plannedTask.groupId().equals(payload.getGroupId())
        || !plannedTask.tableId().equals(payload.getTableId())
        || plannedTask.snapshotId() != payload.getSnapshotId()
        || !lease.leaseEpoch.equals(payload.getLeaseEpoch())
        || !resultId.equals(payload.getResultId())) {
      throw new IllegalArgumentException("file-group stats payload identity mismatch");
    }
    if (payload.getFileStatsCount() != descriptor.fileStatsRecordCount()) {
      throw new IllegalArgumentException(
          "file-group stats payload record count does not match descriptor");
    }
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(plannedTask.tableId())
            .build();
    List<TargetStatsRecord> fileStats =
        validateFileStats(tableId, plannedTask.snapshotId(), payload.getFileStatsList());
    validateCompleteStagedFileStats(lease, plannedTask, fileStats);
    return fileStats;
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private static void requireAcceptedLeaseOutcome(boolean accepted, String jobId) {
    if (!accepted) {
      throw ReconcileLeaseGrpcStatus.leasePreconditionFailed(
          "reconcile lease is no longer valid for job " + jobId);
    }
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
                    () ->
                        new IdempotencyGuard.CreateResult<>(
                            SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
                                .setAccepted(true)
                                .build(),
                            tableId),
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
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> statsRecords) {
    List<TargetStatsRecord> nonNullStats =
        statsRecords == null
            ? List.of()
            : statsRecords.stream().filter(java.util.Objects::nonNull).toList();
    if (nonNullStats.isEmpty()) {
      return;
    }
    List<TargetStatsRecord> created =
        statsStore.putTargetStatsBatchIfAbsent(tableId, snapshotId, nonNullStats);
    if (!created.isEmpty()) {
      statsOrchestrator.invalidateStatsCache(tableId, snapshotId, created);
    }
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

  private static void validateCompleteStagedFileStats(
      ReconcileJobStore.LeasedJob lease,
      ReconcileFileGroupTask plannedTask,
      List<TargetStatsRecord> fileStats) {
    Set<String> plannedPaths = new LinkedHashSet<>(plannedTask.filePaths());
    Set<String> stagedPaths =
        nonNullStatsRecords(fileStats).stream()
            .filter(TargetStatsRecord::hasFile)
            .map(record -> record.getFile().getFilePath())
            .filter(path -> path != null && !path.isBlank())
            .collect(java.util.stream.Collectors.toSet());
    if (!plannedPaths.containsAll(stagedPaths)) {
      throw new IllegalArgumentException("file-group stats include an unplanned file");
    }
    boolean requestsStats =
        !FileGroupExecutionSupport.requestedStatsTargetKinds(
                FileGroupExecutionSupport.effectiveCapturePolicy(lease))
            .isEmpty();
    if (requestsStats && !stagedPaths.containsAll(plannedPaths)) {
      throw new IllegalArgumentException("file-group stats are missing a planned file");
    }
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
      ReconcileJobStore.LeasedJob lease,
      ResourceId tableId,
      long snapshotId,
      ReconcileFileGroupTask plannedTask,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
    List<IndexArtifactRecord> persistedRecords = new java.util.ArrayList<>();
    Set<String> artifactFiles = new LinkedHashSet<>();
    for (ReconcilerBackend.StagedIndexArtifact stagedArtifact : stagedIndexArtifacts) {
      if (stagedArtifact == null || stagedArtifact.record() == null) {
        continue;
      }
      IndexArtifactRecord prepared =
          prepareIndexArtifactRecord(tableId, snapshotId, plannedTask, stagedArtifact.record());
      String filePath = prepared.getTarget().getFile().getFilePath();
      if (!artifactFiles.add(filePath)) {
        throw new IllegalArgumentException(
            "file-group result contains duplicate index artifacts for " + filePath);
      }
      persistedRecords.add(prepared);
    }
    if (FileGroupExecutionSupport.effectiveCapturePolicy(lease).requestsIndexes()
        && !artifactFiles.containsAll(plannedTask.filePaths())) {
      throw new IllegalArgumentException("file-group index artifacts are missing a planned file");
    }
    if (persistedRecords.isEmpty()) {
      return;
    }
    indexArtifactRepo.putIndexArtifactsBatch(persistedRecords);
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
      ResourceId tableId,
      long snapshotId,
      ReconcileFileGroupTask plannedTask,
      IndexArtifactRecord record) {
    if (!tableId.equals(record.getTableId()) || snapshotId != record.getSnapshotId()) {
      throw new IllegalArgumentException(
          "index artifact descriptor does not match the leased table and snapshot");
    }
    if (!record.hasTarget() || !record.getTarget().hasFile()) {
      throw new IllegalArgumentException("file-group index artifact must have a file target");
    }
    if (record.getArtifactUri().isBlank()
        || record.getArtifactFormat().isBlank()
        || record.getArtifactFormatVersion() == 0
        || record.getState() != IndexArtifactState.IAS_READY) {
      throw new IllegalArgumentException(
          "file-group index artifact must reference a committed ready artifact");
    }
    String filePath = record.getTarget().getFile().getFilePath();
    if (filePath == null
        || filePath.isBlank()
        || plannedTask == null
        || !plannedTask.filePaths().contains(filePath)) {
      throw new IllegalArgumentException("index artifact descriptor targets an unplanned file");
    }
    var header =
        blobStore
            .head(record.getArtifactUri())
            .orElseThrow(
                () ->
                    new StorageAbortRetryableException(
                        "index artifact object is not committed uri=" + record.getArtifactUri()));
    if (header.getContentLength() <= 0L) {
      throw new IllegalArgumentException(
          "file-group index artifact object is empty uri=" + record.getArtifactUri());
    }
    return record.toBuilder().setContentEtag(header.getEtag()).build();
  }

  private static List<TargetStatsRecord> nonNullStatsRecords(List<TargetStatsRecord> statsRecords) {
    if (statsRecords == null || statsRecords.isEmpty()) {
      return List.of();
    }
    return statsRecords.stream().filter(java.util.Objects::nonNull).toList();
  }

  private static SubmitLeasedFileGroupExecutionResultRequest.Failure failurePayload(
      String resultId, String message) {
    return SubmitLeasedFileGroupExecutionResultRequest.Failure.newBuilder()
        .setResultId(resultId)
        .setMessage(message == null ? "" : message)
        .build();
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
        .orElseThrow(this::unresolvedPlannedTask);
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
