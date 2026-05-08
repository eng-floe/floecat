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
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.PutIndexArtifactItem;
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
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.reconciler.impl.FileGroupExecutionSupport;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.StandaloneFileGroupExecutionPayload;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
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
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.spi.BlobStore;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class LeasedFileGroupExecutionService extends BaseServiceImpl {
  @Inject ReconcileJobStore jobs;
  @Inject TableRepository tableRepo;
  @Inject ConnectorRepository connectorRepo;
  @Inject CredentialResolver credentialResolver;
  @Inject StatsStore statsStore;
  @Inject IndexArtifactRepository indexArtifactRepo;
  @Inject BlobStore blobStore;
  @Inject IdempotencyRepository idempotencyStore;

  public StandaloneFileGroupExecutionPayload resolve(
      PrincipalContext principalContext, String jobId, String leaseEpoch) {
    String corr = principalContext.getCorrelationId();
    ReconcileJobStore.LeasedJob lease = requireLeasedFileGroupJob(corr, jobId, leaseEpoch);
    ReconcileFileGroupTask plannedTask =
        FileGroupExecutionSupport.resolvePlannedTask(
                jobs,
                lease,
                lease.fileGroupTask == null ? ReconcileFileGroupTask.empty() : lease.fileGroupTask)
            .orElseThrow(
                () ->
                    Status.FAILED_PRECONDITION
                        .withDescription(
                            "planned file group could not be resolved from parent snapshot plan")
                        .asRuntimeException());
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
    String metadataLocation = table.getPropertiesMap().getOrDefault("metadata-location", "").trim();
    if (metadataLocation.isBlank()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("table metadata-location property is required for file-group execution")
          .asRuntimeException();
    }
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
    Connector resolvedConnector = connector.toBuilder().setAuth(resolvedAuth(connector)).build();
    return new StandaloneFileGroupExecutionPayload(
        lease.jobId,
        lease.leaseEpoch,
        lease.parentJobId,
        resolvedConnector,
        metadataLocation,
        String.join(".", table.getUpstream().getNamespacePathList()),
        table.getUpstream().getTableDisplayName(),
        tableId,
        plannedTask.snapshotId(),
        plannedTask.planId(),
        plannedTask.groupId(),
        plannedTask.filePaths(),
        FileGroupExecutionSupport.effectiveCapturePolicy(lease));
  }

  public boolean persistSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      List<TargetStatsRecord> statsRecords,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
    String corr = principalContext.getCorrelationId();
    ReconcileJobStore.LeasedJob lease = requireLeasedFileGroupJob(corr, jobId, leaseEpoch);
    ReconcileFileGroupTask plannedTask =
        FileGroupExecutionSupport.resolvePlannedTask(
                jobs,
                lease,
                lease.fileGroupTask == null ? ReconcileFileGroupTask.empty() : lease.fileGroupTask)
            .orElseThrow(
                () ->
                    Status.FAILED_PRECONDITION
                        .withDescription(
                            "planned file group could not be resolved from parent snapshot plan")
                        .asRuntimeException());
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(plannedTask.tableId())
            .build();
    String requiredResultId = requireResultId(resultId);
    List<TargetStatsRecord> effectiveStats = nonNullStatsRecords(statsRecords);
    List<TargetStatsRecord> effectiveFileStats =
        effectiveStats.stream()
            .filter(
                record ->
                    record != null
                        && record.hasTarget()
                        && StatsTargetType.from(record.getTarget()) == StatsTargetType.FILE)
            .toList();
    List<ReconcilerBackend.StagedIndexArtifact> effectiveArtifacts =
        stagedIndexArtifacts == null ? List.of() : stagedIndexArtifacts;
    byte[] requestBytes =
        successPayload(requiredResultId, effectiveFileStats, effectiveArtifacts).toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedFileGroupExecutionResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () -> {
                      long snapshotId = plannedTask.snapshotId();
                      persistTargetStats(
                          principalContext,
                          tableId,
                          snapshotId,
                          requiredResultId,
                          effectiveFileStats);
                      persistIndexArtifacts(
                          principalContext,
                          tableId,
                          snapshotId,
                          requiredResultId,
                          effectiveArtifacts);
                      jobs.persistFileGroupResult(
                          lease.jobId,
                          plannedTask.withFileResults(
                              FileGroupExecutionSupport.fileResultsForSuccess(
                                  plannedTask, effectiveFileStats, effectiveArtifacts)));
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

  public boolean persistFailure(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      String message) {
    String corr = principalContext.getCorrelationId();
    ReconcileJobStore.LeasedJob lease = requireLeasedFileGroupJob(corr, jobId, leaseEpoch);
    ReconcileFileGroupTask plannedTask =
        FileGroupExecutionSupport.resolvePlannedTask(
                jobs,
                lease,
                lease.fileGroupTask == null ? ReconcileFileGroupTask.empty() : lease.fileGroupTask)
            .orElseThrow(
                () ->
                    Status.FAILED_PRECONDITION
                        .withDescription(
                            "planned file group could not be resolved from parent snapshot plan")
                        .asRuntimeException());
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
                          plannedTask.withFileResults(
                              FileGroupExecutionSupport.fileResultsForFailure(
                                  plannedTask, effectiveMessage)));
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
      List<TargetStatsRecord> statsRecords) {
    String accountId = principalContext.getAccountId();
    var now = nowTs();
    for (TargetStatsRecord targetRecord : statsRecords) {
      if (targetRecord == null) {
        continue;
      }
      String targetKey = StatsTargetIdentity.storageId(targetRecord.getTarget());
      String itemKey = itemIdempotencyKey(resultId, "target", hashString(targetKey));
      runIdempotentCreate(
          () ->
              MutationOps.createProto(
                  accountId,
                  "SubmitLeasedFileGroupExecutionResult",
                  itemKey,
                  targetRecord::toByteArray,
                  () -> {
                    statsStore.putTargetStats(targetRecord);
                    return new IdempotencyGuard.CreateResult<>(targetRecord, tableId);
                  },
                  rec -> statsStore.metaForTargetStats(tableId, snapshotId, rec.getTarget(), now),
                  idempotencyStore,
                  now,
                  idempotencyTtlSeconds(),
                  principalContext::getCorrelationId,
                  TargetStatsRecord::parseFrom));
    }
  }

  private void persistIndexArtifacts(
      PrincipalContext principalContext,
      ResourceId tableId,
      long snapshotId,
      String resultId,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
    String accountId = principalContext.getAccountId();
    var now = nowTs();
    for (ReconcilerBackend.StagedIndexArtifact stagedArtifact : stagedIndexArtifacts) {
      if (stagedArtifact == null || stagedArtifact.record() == null) {
        continue;
      }
      PutIndexArtifactItem item = toPutIndexArtifactItem(stagedArtifact);
      String itemKey =
          itemIdempotencyKey(
              resultId,
              "index_artifact",
              hashString(targetStorageId(item.getRecord().getTarget())));
      runIdempotentCreate(
          () ->
              MutationOps.createProto(
                  accountId,
                  "SubmitLeasedFileGroupExecutionResult",
                  itemKey,
                  item::toByteArray,
                  () -> {
                    persistIndexArtifact(item);
                    return new IdempotencyGuard.CreateResult<>(item, tableId);
                  },
                  persisted ->
                      indexArtifactRepo.metaForIndexArtifact(
                          tableId, snapshotId, persisted.getRecord().getTarget(), now),
                  idempotencyStore,
                  now,
                  idempotencyTtlSeconds(),
                  principalContext::getCorrelationId,
                  PutIndexArtifactItem::parseFrom));
    }
  }

  private void persistIndexArtifact(PutIndexArtifactItem item) {
    IndexArtifactRecord record = item.getRecord();
    String contentType =
        item.getContentType() == null || item.getContentType().isBlank()
            ? "application/x-parquet"
            : item.getContentType();
    blobStore.put(record.getArtifactUri(), item.getContent().toByteArray(), contentType);
    String etag =
        blobStore
            .head(record.getArtifactUri())
            .map(head -> head.getEtag())
            .orElse(record.getContentEtag());
    indexArtifactRepo.putIndexArtifact(record.toBuilder().setContentEtag(etag).build());
  }

  private AuthConfig resolvedAuth(Connector connector) {
    ConnectorConfig.Auth resolved = resolveCredentials(connector).auth();
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

  private static PutIndexArtifactItem toPutIndexArtifactItem(
      ReconcilerBackend.StagedIndexArtifact stagedArtifact) {
    return PutIndexArtifactItem.newBuilder()
        .setRecord(stagedArtifact.record())
        .setContent(com.google.protobuf.ByteString.copyFrom(stagedArtifact.content()))
        .setContentType(stagedArtifact.contentType() == null ? "" : stagedArtifact.contentType())
        .build();
  }

  private static SubmitLeasedFileGroupExecutionResultRequest.Success successPayload(
      String resultId,
      List<TargetStatsRecord> statsRecords,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
    SubmitLeasedFileGroupExecutionResultRequest.Success.Builder builder =
        SubmitLeasedFileGroupExecutionResultRequest.Success.newBuilder()
            .setResultId(resultId)
            .addAllStatsRecords(statsRecords);
    for (ReconcilerBackend.StagedIndexArtifact stagedArtifact : stagedIndexArtifacts) {
      if (stagedArtifact == null || stagedArtifact.record() == null) {
        continue;
      }
      builder.addIndexArtifacts(
          ai.floedb.floecat.reconciler.rpc.LeasedFileGroupIndexArtifact.newBuilder()
              .setRecord(stagedArtifact.record())
              .setContent(com.google.protobuf.ByteString.copyFrom(stagedArtifact.content()))
              .setContentType(
                  stagedArtifact.contentType() == null ? "" : stagedArtifact.contentType())
              .build());
    }
    return builder.build();
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
        jobs.get(jobId)
            .orElseThrow(() -> GrpcErrors.notFound(corr, TABLE, Map.of("job_id", jobId)));
    if (job.jobKind != ReconcileJobKind.EXEC_FILE_GROUP) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is not an EXEC_FILE_GROUP job")
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

  private static String itemIdempotencyKey(String baseKey, String kind, Object itemId) {
    return baseKey + ":" + kind + ":" + String.valueOf(itemId);
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

  private static String hashString(String value) {
    if (value == null || value.isBlank()) {
      return "empty";
    }
    return hashFingerprint(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String targetStorageId(IndexTarget target) {
    return switch (target.getTargetCase()) {
      case FILE -> "file:" + target.getFile().getFilePath();
      case TARGET_NOT_SET ->
          throw new IllegalArgumentException("target must be set on IndexArtifactRecord");
    };
  }
}
