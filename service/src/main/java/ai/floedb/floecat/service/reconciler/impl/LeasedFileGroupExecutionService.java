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
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.impl.FileGroupExecutionSupport;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.StandaloneFileGroupExecutionPayload;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileIndexArtifactResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultResponse;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedFileGroupExecutionResultStreamRequest;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IcebergMetadataLocationResolver;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.storage.impl.StorageAuthorityResolver;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class LeasedFileGroupExecutionService extends BaseServiceImpl {
  @FunctionalInterface
  interface ConnectorOpener {
    FloecatConnector open(ConnectorConfig config);
  }

  @Inject ReconcileJobStore jobs;
  @Inject TableRepository tableRepo;
  @Inject ConnectorRepository connectorRepo;
  @Inject CredentialResolver credentialResolver;
  @Inject StatsStore statsStore;
  @Inject IndexArtifactRepository indexArtifactRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject StorageAuthorityRepository storageAuthorityRepo;
  @Inject StorageAuthorityResolver storageAuthorityResolver;
  @Inject BlobStore blobStore;
  @Inject IdempotencyRepository idempotencyStore;
  ConnectorOpener connectorOpener = ConnectorFactory::create;

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
    String metadataLocation = resolveMetadataLocation(tableId, table, plannedTask, connector);
    if (metadataLocation == null || metadataLocation.isBlank()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("table metadata-location property is required for file-group execution")
          .asRuntimeException();
    }
    Map<String, String> sourceStorageConfig =
        resolveSourceStorageConfig(lease.accountId, metadataLocation);
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
        FileGroupExecutionSupport.effectiveCapturePolicy(lease),
        sourceStorageConfig);
  }

  private String resolveMetadataLocation(
      ResourceId tableId, Table table, ReconcileFileGroupTask plannedTask, Connector connector) {
    String metadataLocation =
        IcebergMetadataLocationResolver.resolve(
            table, snapshotRepo.getById(tableId, plannedTask.snapshotId()).orElse(null));
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      return metadataLocation;
    }
    return resolveMetadataLocationFromConnector(table, connector);
  }

  private String resolveMetadataLocationFromConnector(Table table, Connector connector) {
    if (table == null
        || connector == null
        || !table.hasUpstream()
        || table.getUpstream().getNamespacePathCount() == 0
        || table.getUpstream().getTableDisplayName().isBlank()) {
      return null;
    }
    ConnectorConfig resolvedConfig = resolveCredentials(connector);
    try (FloecatConnector source = connectorOpener.open(resolvedConfig)) {
      FloecatConnector.TableDescriptor descriptor =
          source.describe(
              String.join(".", table.getUpstream().getNamespacePathList()),
              table.getUpstream().getTableDisplayName());
      if (descriptor == null || descriptor.properties() == null) {
        return null;
      }
      String metadataLocation = descriptor.properties().get("metadata-location");
      if (metadataLocation == null) {
        return null;
      }
      metadataLocation = metadataLocation.trim();
      return metadataLocation.isBlank() ? null : metadataLocation;
    } catch (RuntimeException e) {
      return null;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to resolve metadata-location from source connector", e);
    }
  }

  private Map<String, String> resolveSourceStorageConfig(
      String accountId, String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return Map.of();
    }
    var authority =
        StorageAuthorityResolver.resolveBest(
                storageAuthorityRepo.list(accountId, Integer.MAX_VALUE, "", new StringBuilder()),
                metadataLocation)
            .orElse(null);
    if (authority == null) {
      return Map.of();
    }
    return storageAuthorityResolver.resolveServerSideStorageConfig(
        authority, metadataLocation, accountId);
  }

  public Uni<SubmitLeasedFileGroupExecutionResultResponse> persistStreamedResult(
      PrincipalContext principalContext,
      Multi<SubmitLeasedFileGroupExecutionResultStreamRequest> requests) {
    StreamingSubmitState state = new StreamingSubmitState();
    return requests
        .emitOn(Infrastructure.getDefaultExecutor())
        .onItem()
        .invoke(request -> processStreamFrame(principalContext, state, request))
        .collect()
        .last()
        .onItem()
        .ifNull()
        .failWith(
            () ->
                Status.INVALID_ARGUMENT
                    .withDescription("file-group result stream must not be empty")
                    .asRuntimeException())
        .replaceWith(() -> finalizeStreamedResult(principalContext, state));
  }

  private void processStreamFrame(
      PrincipalContext principalContext,
      StreamingSubmitState state,
      SubmitLeasedFileGroupExecutionResultStreamRequest request) {
    if (request == null
        || request.getFrameCase()
            == SubmitLeasedFileGroupExecutionResultStreamRequest.FrameCase.FRAME_NOT_SET) {
      throw Status.INVALID_ARGUMENT
          .withDescription("file-group result stream frame is required")
          .asRuntimeException();
    }
    if (state.completed) {
      throw Status.INVALID_ARGUMENT
          .withDescription("file-group result stream received frames after end")
          .asRuntimeException();
    }
    state.digest.update(request.toByteArray());
    switch (request.getFrameCase()) {
      case BEGIN -> processStreamBegin(principalContext, state, request.getBegin());
      case STATS_CHUNK -> processStatsChunk(principalContext, state, request.getStatsChunk());
      case INDEX_ARTIFACT_BEGIN ->
          processArtifactBegin(principalContext, state, request.getIndexArtifactBegin());
      case INDEX_ARTIFACT_CONTENT ->
          processArtifactContent(principalContext, state, request.getIndexArtifactContent());
      case INDEX_ARTIFACT_END -> processArtifactEnd(principalContext, state);
      case END -> processStreamEnd(state);
      case FRAME_NOT_SET ->
          throw Status.INVALID_ARGUMENT
              .withDescription("file-group result stream frame is required")
              .asRuntimeException();
    }
  }

  private void processStreamBegin(
      PrincipalContext principalContext,
      StreamingSubmitState state,
      SubmitLeasedFileGroupExecutionResultStreamRequest.Begin begin) {
    if (state.context != null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("file-group result stream begin frame must be first")
          .asRuntimeException();
    }
    if (begin.getOutcomeKind()
        == SubmitLeasedFileGroupExecutionResultStreamRequest.OutcomeKind.OK_UNSPECIFIED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("outcome_kind is required for file-group result streaming")
          .asRuntimeException();
    }
    PersistContext context =
        preparePersistContext(principalContext, begin.getJobId(), begin.getLeaseEpoch());
    state.context = context;
    state.resultId = requireResultId(begin.getResultId());
    state.outcomeKind = begin.getOutcomeKind();
    state.failureMessage = begin.getFailureMessage() == null ? "" : begin.getFailureMessage();
    for (String filePath : context.plannedTask().filePaths()) {
      state.statsByFile.put(filePath, 0L);
    }
  }

  private void processStatsChunk(
      PrincipalContext principalContext,
      StreamingSubmitState state,
      SubmitLeasedFileGroupExecutionResultStreamRequest.StatsChunk chunk) {
    PersistContext context = requireStreamingSuccessState(state);
    for (TargetStatsRecord record : nonNullStatsRecords(chunk.getStatsRecordsList())) {
      if (!isFileTargetStat(record)) {
        continue;
      }
      persistTargetStat(
          principalContext,
          context.tableId(),
          context.plannedTask().snapshotId(),
          state.resultId,
          record);
      String filePath = filePathForStatsRecord(record);
      if (!filePath.isBlank() && state.statsByFile.containsKey(filePath)) {
        state.statsByFile.computeIfPresent(filePath, (ignored, count) -> count + 1L);
      }
    }
  }

  private void processArtifactBegin(
      PrincipalContext principalContext,
      StreamingSubmitState state,
      SubmitLeasedFileGroupExecutionResultStreamRequest.IndexArtifactBegin begin) {
    requireStreamingSuccessState(state);
    if (state.currentArtifact != null) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "index artifact stream cannot begin a new artifact before ending the prior artifact")
          .asRuntimeException();
    }
    if (begin == null || !begin.hasRecord()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("index artifact begin frame requires a record")
          .asRuntimeException();
    }
    state.currentArtifact =
        new StreamingArtifact(
            begin.getRecord(), begin.getContentType(), new ByteArrayOutputStream());
  }

  private void processArtifactContent(
      PrincipalContext principalContext,
      StreamingSubmitState state,
      SubmitLeasedFileGroupExecutionResultStreamRequest.IndexArtifactContent content) {
    requireStreamingSuccessState(state);
    if (state.currentArtifact == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("index artifact content frame requires an active artifact")
          .asRuntimeException();
    }
    ByteString bytes = content == null ? ByteString.EMPTY : content.getContent();
    if (!bytes.isEmpty()) {
      state.currentArtifact.content().writeBytes(bytes.toByteArray());
    }
  }

  private void processArtifactEnd(PrincipalContext principalContext, StreamingSubmitState state) {
    PersistContext context = requireStreamingSuccessState(state);
    if (state.currentArtifact == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("index artifact end frame requires an active artifact")
          .asRuntimeException();
    }
    var artifact = state.currentArtifact;
    state.currentArtifact = null;
    var stagedArtifact =
        new ReconcilerBackend.StagedIndexArtifact(
            artifact.record(), artifact.content().toByteArray(), artifact.contentType());
    persistIndexArtifact(
        principalContext,
        context.tableId(),
        context.plannedTask().snapshotId(),
        state.resultId,
        stagedArtifact);
    String filePath = filePathForIndexArtifact(artifact.record());
    if (!filePath.isBlank()) {
      state.artifactByFile.put(
          filePath,
          ReconcileIndexArtifactResult.of(
              artifact.record().getArtifactUri(),
              artifact.record().getArtifactFormat(),
              artifact.record().getArtifactFormatVersion()));
    }
  }

  private void processStreamEnd(StreamingSubmitState state) {
    if (state.context == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("file-group result stream begin frame is required before end")
          .asRuntimeException();
    }
    if (state.currentArtifact != null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("file-group result stream ended with an incomplete index artifact")
          .asRuntimeException();
    }
    state.completed = true;
  }

  private SubmitLeasedFileGroupExecutionResultResponse finalizeStreamedResult(
      PrincipalContext principalContext, StreamingSubmitState state) {
    if (state.context == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("file-group result stream begin frame is required")
          .asRuntimeException();
    }
    if (!state.completed) {
      throw Status.INVALID_ARGUMENT
          .withDescription("file-group result stream end frame is required")
          .asRuntimeException();
    }
    return switch (state.outcomeKind) {
      case OK_SUCCESS ->
          SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
              .setAccepted(
                  persistStreamedSuccess(
                      principalContext,
                      state.context,
                      state.resultId,
                      state.digest.digest(),
                      streamFileResults(
                          state.context.plannedTask(), state.statsByFile, state.artifactByFile)))
              .build();
      case OK_FAILURE ->
          SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
              .setAccepted(
                  persistStreamedFailure(
                      principalContext,
                      state.context,
                      state.resultId,
                      state.failureMessage,
                      state.digest.digest()))
              .build();
      case OK_UNSPECIFIED, UNRECOGNIZED ->
          throw Status.INVALID_ARGUMENT
              .withDescription("outcome_kind is required for file-group result streaming")
              .asRuntimeException();
    };
  }

  public boolean persistSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      List<TargetStatsRecord> statsRecords,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
    PersistContext context = preparePersistContext(principalContext, jobId, leaseEpoch);
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
                      persistTargetStats(
                          principalContext,
                          context.tableId(),
                          context.plannedTask().snapshotId(),
                          requiredResultId,
                          effectiveFileStats);
                      persistIndexArtifacts(
                          principalContext,
                          context.tableId(),
                          context.plannedTask().snapshotId(),
                          requiredResultId,
                          effectiveArtifacts);
                      jobs.persistFileGroupResult(
                          context.lease().jobId,
                          context
                              .plannedTask()
                              .withFileResults(
                                  FileGroupExecutionSupport.fileResultsForSuccess(
                                      context.plannedTask(),
                                      effectiveFileStats,
                                      effectiveArtifacts)));
                      return new IdempotencyGuard.CreateResult<>(
                          SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
                              .setAccepted(true)
                              .build(),
                          context.tableId());
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
    PersistContext context = preparePersistContext(principalContext, jobId, leaseEpoch);
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
                          context.lease().jobId,
                          context
                              .plannedTask()
                              .withFileResults(
                                  FileGroupExecutionSupport.fileResultsForFailure(
                                      context.plannedTask(), effectiveMessage)));
                      return new IdempotencyGuard.CreateResult<>(
                          SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
                              .setAccepted(true)
                              .build(),
                          context.tableId());
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

  private PersistContext preparePersistContext(
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
    return new PersistContext(lease, plannedTask, tableId);
  }

  private boolean persistStreamedSuccess(
      PrincipalContext principalContext,
      PersistContext context,
      String resultId,
      byte[] fingerprint,
      List<ReconcileFileResult> fileResults) {
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedFileGroupExecutionResultStream",
                    resultIdempotencyKey(context.lease().jobId, resultId),
                    () -> fingerprint,
                    () -> {
                      jobs.persistFileGroupResult(
                          context.lease().jobId,
                          context.plannedTask().withFileResults(fileResults));
                      return new IdempotencyGuard.CreateResult<>(
                          SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
                              .setAccepted(true)
                              .build(),
                          context.tableId());
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

  private boolean persistStreamedFailure(
      PrincipalContext principalContext,
      PersistContext context,
      String resultId,
      String message,
      byte[] fingerprint) {
    String effectiveMessage = message == null ? "" : message;
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedFileGroupExecutionResultStream",
                    resultIdempotencyKey(context.lease().jobId, resultId),
                    () -> fingerprint,
                    () -> {
                      jobs.persistFileGroupResult(
                          context.lease().jobId,
                          context
                              .plannedTask()
                              .withFileResults(
                                  FileGroupExecutionSupport.fileResultsForFailure(
                                      context.plannedTask(), effectiveMessage)));
                      return new IdempotencyGuard.CreateResult<>(
                          SubmitLeasedFileGroupExecutionResultResponse.newBuilder()
                              .setAccepted(true)
                              .build(),
                          context.tableId());
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
    for (TargetStatsRecord targetRecord : statsRecords) {
      if (targetRecord == null) {
        continue;
      }
      persistTargetStat(principalContext, tableId, snapshotId, resultId, targetRecord);
    }
  }

  private void persistIndexArtifacts(
      PrincipalContext principalContext,
      ResourceId tableId,
      long snapshotId,
      String resultId,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
    for (ReconcilerBackend.StagedIndexArtifact stagedArtifact : stagedIndexArtifacts) {
      if (stagedArtifact == null || stagedArtifact.record() == null) {
        continue;
      }
      persistIndexArtifact(principalContext, tableId, snapshotId, resultId, stagedArtifact);
    }
  }

  private void persistTargetStat(
      PrincipalContext principalContext,
      ResourceId tableId,
      long snapshotId,
      String resultId,
      TargetStatsRecord targetRecord) {
    String accountId = principalContext.getAccountId();
    var now = nowTs();
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

  private void persistIndexArtifact(
      PrincipalContext principalContext,
      ResourceId tableId,
      long snapshotId,
      String resultId,
      ReconcilerBackend.StagedIndexArtifact stagedArtifact) {
    String accountId = principalContext.getAccountId();
    var now = nowTs();
    PutIndexArtifactItem item = toPutIndexArtifactItem(stagedArtifact);
    String itemKey =
        itemIdempotencyKey(
            resultId, "index_artifact", hashString(targetStorageId(item.getRecord().getTarget())));
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

  private static boolean isFileTargetStat(TargetStatsRecord record) {
    return record != null
        && record.hasTarget()
        && StatsTargetType.from(record.getTarget()) == StatsTargetType.FILE;
  }

  private static String filePathForStatsRecord(TargetStatsRecord record) {
    if (record == null) {
      return "";
    }
    if (record.hasFile() && record.getFile().getFilePath() != null) {
      return record.getFile().getFilePath();
    }
    if (record.hasTarget() && record.getTarget().hasFile()) {
      return record.getTarget().getFile().getFilePath();
    }
    return "";
  }

  private static String filePathForIndexArtifact(IndexArtifactRecord record) {
    if (record == null || !record.hasTarget() || !record.getTarget().hasFile()) {
      return "";
    }
    return record.getTarget().getFile().getFilePath();
  }

  private static List<ReconcileFileResult> streamFileResults(
      ReconcileFileGroupTask plannedTask,
      LinkedHashMap<String, Long> statsByFile,
      HashMap<String, ReconcileIndexArtifactResult> artifactByFile) {
    List<ReconcileFileResult> results = new ArrayList<>(plannedTask.filePaths().size());
    for (String filePath : plannedTask.filePaths()) {
      results.add(
          ReconcileFileResult.succeeded(
              filePath,
              statsByFile.getOrDefault(filePath, 0L),
              artifactByFile.getOrDefault(filePath, ReconcileIndexArtifactResult.empty())));
    }
    return List.copyOf(results);
  }

  private static PersistContext requireStreamingSuccessState(StreamingSubmitState state) {
    if (state.context == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription("file-group result stream begin frame is required before data frames")
          .asRuntimeException();
    }
    if (state.outcomeKind
        != SubmitLeasedFileGroupExecutionResultStreamRequest.OutcomeKind.OK_SUCCESS) {
      throw Status.INVALID_ARGUMENT
          .withDescription("success data frames are only valid for success outcomes")
          .asRuntimeException();
    }
    return state.context;
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

  private record PersistContext(
      ReconcileJobStore.LeasedJob lease, ReconcileFileGroupTask plannedTask, ResourceId tableId) {}

  private record StreamingArtifact(
      IndexArtifactRecord record, String contentType, ByteArrayOutputStream content) {
    StreamingArtifact {
      contentType = contentType == null ? "" : contentType;
    }
  }

  private static final class StreamingSubmitState {
    private PersistContext context;
    private String resultId = "";
    private String failureMessage = "";
    private SubmitLeasedFileGroupExecutionResultStreamRequest.OutcomeKind outcomeKind =
        SubmitLeasedFileGroupExecutionResultStreamRequest.OutcomeKind.OK_UNSPECIFIED;
    private final LinkedHashMap<String, Long> statsByFile = new LinkedHashMap<>();
    private final HashMap<String, ReconcileIndexArtifactResult> artifactByFile = new HashMap<>();
    private StreamingArtifact currentArtifact;
    private boolean completed;
    private final MessageDigest digest = newSha256();
  }

  private static MessageDigest newSha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          "SHA-256 digest is required for reconcile result streaming", e);
    }
  }
}
