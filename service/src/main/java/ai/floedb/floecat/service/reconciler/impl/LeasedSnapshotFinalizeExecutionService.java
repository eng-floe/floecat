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

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.impl.ReconcileLeaseGrpcStatus;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.SnapshotFinalizeStatsDescriptor;
import ai.floedb.floecat.reconciler.rpc.SnapshotFinalizeStatsPayload;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultResponse;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.spi.BlobStore;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class LeasedSnapshotFinalizeExecutionService extends BaseServiceImpl {
  @Inject ReconcileJobStore jobs;
  @Inject ai.floedb.floecat.service.repo.IdempotencyRepository idempotencyStore;
  @Inject SnapshotFinalizePersistenceService persistence;
  @Inject SnapshotFinalizeChildStateService childStateService;
  @Inject CurrentSnapshotPointerService currentSnapshotPointerService;
  @Inject BlobStore blobStore;

  public boolean persistSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      SnapshotFinalizeStatsDescriptor descriptor) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedSnapshotFinalizeJob(principalContext.getCorrelationId(), jobId, leaseEpoch);
    ReconcileSnapshotTask snapshotTask = requireSnapshotTask(lease);
    ResourceId tableId = tableId(lease, snapshotTask);
    String requiredResultId = requireResultId(resultId);
    List<TargetStatsRecord> statsRecords =
        loadValidatedStatsPayload(lease, snapshotTask, requiredResultId, descriptor);
    byte[] requestBytes = successPayload(requiredResultId, descriptor).toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedSnapshotFinalizeResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () -> {
                      persistStatsPayload(
                          lease, snapshotTask, tableId, snapshotTask.snapshotId(), statsRecords);
                      finalizeStatsPublication(
                          lease,
                          snapshotTask,
                          tableId,
                          snapshotTask.snapshotId(),
                          statsRecords.size());
                      currentSnapshotPointerService.maybeAdvance(
                          tableId, snapshotTask.snapshotId(), lease.jobId);
                      boolean accepted =
                          jobs.applyLeaseOutcome(
                              lease.jobId,
                              lease.leaseEpoch,
                              ReconcileJobStore.CompletionKind.SUCCEEDED,
                              System.currentTimeMillis(),
                              "Finalized snapshot " + snapshotTask.snapshotId(),
                              0L,
                              0L,
                              0L,
                              0L,
                              0L,
                              1L,
                              statsRecords.size());
                      requireAcceptedLeaseOutcome(accepted, lease.jobId);
                      return new IdempotencyGuard.CreateResult<>(
                          SubmitLeasedSnapshotFinalizeResultResponse.newBuilder()
                              .setAccepted(true)
                              .build(),
                          tableId);
                    },
                    ignored -> MutationMeta.getDefaultInstance(),
                    idempotencyStore,
                    nowTs(),
                    idempotencyTtlSeconds(),
                    principalContext::getCorrelationId,
                    SubmitLeasedSnapshotFinalizeResultResponse::parseFrom))
        .body
        .getAccepted();
  }

  private List<TargetStatsRecord> loadValidatedStatsPayload(
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      String resultId,
      SnapshotFinalizeStatsDescriptor descriptor) {
    if (descriptor == null || descriptor.getFormatVersion() != 1) {
      throw new IllegalArgumentException(
          "snapshot finalize stats descriptor format_version must be 1");
    }
    String expectedUri =
        Keys.reconcileSnapshotFinalizeStatsPayloadUri(
            lease.accountId, lease.parentJobId, lease.jobId, lease.leaseEpoch);
    if (!lease.accountId.equals(descriptor.getAccountId())
        || !lease.connectorId.equals(descriptor.getConnectorId())
        || !lease.parentJobId.equals(descriptor.getParentJobId())
        || !lease.jobId.equals(descriptor.getFinalizeJobId())
        || !snapshotTask.tableId().equals(descriptor.getTableId())
        || snapshotTask.snapshotId() != descriptor.getSnapshotId()
        || !lease.leaseEpoch.equals(descriptor.getLeaseEpoch())
        || !resultId.equals(descriptor.getResultId())) {
      throw new IllegalArgumentException("snapshot finalize stats descriptor identity mismatch");
    }
    if (!expectedUri.equals(descriptor.getPayloadUri())) {
      throw new IllegalArgumentException(
          "snapshot finalize stats payload_uri is outside the leased result location");
    }
    if (descriptor.getPayloadBytes() <= 0L || descriptor.getPayloadSha256().size() != 32) {
      throw new IllegalArgumentException(
          "snapshot finalize stats descriptor size and sha256 are required");
    }
    var header =
        blobStore
            .head(expectedUri)
            .orElseThrow(
                () ->
                    new StorageAbortRetryableException(
                        "snapshot finalize stats object is not committed jobId="
                            + lease.jobId
                            + " uri="
                            + expectedUri));
    if (header.getContentLength() != descriptor.getPayloadBytes()) {
      throw new IllegalArgumentException(
          "snapshot finalize stats object size mismatch jobId="
              + lease.jobId
              + " uri="
              + expectedUri);
    }
    byte[] bytes = blobStore.get(expectedUri);
    if (bytes.length != descriptor.getPayloadBytes()
        || !MessageDigest.isEqual(sha256(bytes), descriptor.getPayloadSha256().toByteArray())) {
      throw new IllegalArgumentException(
          "snapshot finalize stats payload does not match descriptor");
    }
    final SnapshotFinalizeStatsPayload payload;
    try {
      payload = SnapshotFinalizeStatsPayload.parseFrom(bytes);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "snapshot finalize stats payload is not valid protobuf", e);
    }
    if (payload.getFormatVersion() != 1
        || !descriptor.getAccountId().equals(payload.getAccountId())
        || !descriptor.getConnectorId().equals(payload.getConnectorId())
        || !descriptor.getParentJobId().equals(payload.getParentJobId())
        || !descriptor.getFinalizeJobId().equals(payload.getFinalizeJobId())
        || !descriptor.getTableId().equals(payload.getTableId())
        || descriptor.getSnapshotId() != payload.getSnapshotId()
        || !descriptor.getLeaseEpoch().equals(payload.getLeaseEpoch())
        || !descriptor.getResultId().equals(payload.getResultId())
        || descriptor.getStatsRecordCount() != payload.getStatsRecordsCount()) {
      throw new IllegalArgumentException("snapshot finalize stats payload identity mismatch");
    }
    return payload.getStatsRecordsList();
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  public boolean persistFailure(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      String message) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedSnapshotFinalizeJob(principalContext.getCorrelationId(), jobId, leaseEpoch);
    ReconcileSnapshotTask snapshotTask = requireSnapshotTask(lease);
    ResourceId tableId = tableId(lease, snapshotTask);
    String requiredResultId = requireResultId(resultId);
    String effectiveMessage = message == null ? "" : message;
    byte[] requestBytes = failurePayload(requiredResultId, effectiveMessage).toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedSnapshotFinalizeResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () ->
                        new IdempotencyGuard.CreateResult<>(
                            SubmitLeasedSnapshotFinalizeResultResponse.newBuilder()
                                .setAccepted(true)
                                .build(),
                            tableId),
                    ignored -> MutationMeta.getDefaultInstance(),
                    idempotencyStore,
                    nowTs(),
                    idempotencyTtlSeconds(),
                    principalContext::getCorrelationId,
                    SubmitLeasedSnapshotFinalizeResultResponse::parseFrom))
        .body
        .getAccepted();
  }

  void persistStatsPayload(
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      ResourceId tableId,
      long snapshotId,
      List<TargetStatsRecord> statsRecords) {
    SnapshotFinalizeCoverageService.ExpectedCoverage coverage = compactCoverage(snapshotTask);
    requireKnownCoverage(coverage);
    boolean requestsStatsOutputs = requestsStatsOutputs(lease);
    switch (coverage.state()) {
      case NON_EMPTY -> {
        if (!requestsStatsOutputs) {
          requireNoStatsRecords(statsRecords);
          return;
        }
        List<TargetStatsRecord> aggregateStats =
            persistence.validateAggregateStats(statsRecords, tableId, snapshotId);
        if (aggregateStats.isEmpty()) {
          return;
        }
        if (lease.fullRescan) {
          persistence.stageStatsGenerationChunk(
              tableId,
              snapshotId,
              LeasedFileGroupExecutionService.statsGenerationId(lease),
              aggregateStats);
          return;
        }
        persistence.persistStats(aggregateStats);
      }
      case DIRECT_STATS -> {
        if (!requestsStatsOutputs) {
          requireNoStatsRecords(statsRecords);
          return;
        }
        List<TargetStatsRecord> directStats =
            persistence.validateReplacementStats(statsRecords, tableId, snapshotId);
        if (directStats.isEmpty()) {
          if (lease.fullRescan) {
            // A full-rescan finalize that finds no files RE-FINALIZES a LIVE snapshot: it must
            // RETAIN superseded generations (a live query may have frozen one), not eagerly wipe
            // every generation's blobs. Publish an empty generation exactly like the non-empty
            // branch below — retention leaves the old generation for deleteUnreferencedGenerations
            // to reclaim under its reference/age guards. deleteAllStatsForSnapshot's whole-prefix
            // teardown is reserved for actual snapshot deletion.
            persistence.replaceAllStatsForSnapshot(tableId, snapshotId, java.util.List.of());
          }
          return;
        }
        if (lease.fullRescan) {
          persistence.replaceAllStatsForSnapshot(tableId, snapshotId, directStats);
        } else {
          persistence.persistStats(directStats);
        }
      }
      case EXPLICIT_EMPTY -> {
        requireNoStatsRecords(statsRecords);
      }
      default ->
          throw Status.FAILED_PRECONDITION.withDescription(coverage.message()).asRuntimeException();
    }
  }

  void finalizeStatsPublication(
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      ResourceId tableId,
      long snapshotId,
      int statsRecordCount) {
    SnapshotFinalizeCoverageService.ExpectedCoverage coverage = compactCoverage(snapshotTask);
    requireKnownCoverage(coverage);
    boolean requestsStatsOutputs = requestsStatsOutputs(lease);
    switch (coverage.state()) {
      case NON_EMPTY -> {
        if (!requestsStatsOutputs) {
          return;
        }
        requireReadyCompactChildState(lease, snapshotTask.fileGroupCount());
        if (lease.fullRescan) {
          persistence.publishFileGroupStatsGeneration(
              tableId,
              snapshotId,
              LeasedFileGroupExecutionService.statsGenerationId(lease),
              List.of());
        }
      }
      case DIRECT_STATS -> {
        if (!requestsStatsOutputs) {
          return;
        }
        requirePlannerDirectStatsRecordCount(snapshotTask, statsRecordCount);
      }
      case EXPLICIT_EMPTY -> {
        if (requestsStatsOutputs) {
          persistence.persistEmptySnapshotCompletionMarker(tableId, snapshotId, lease.fullRescan);
        }
      }
      default ->
          throw Status.FAILED_PRECONDITION.withDescription(coverage.message()).asRuntimeException();
    }
  }

  private static SnapshotFinalizeCoverageService.ExpectedCoverage compactCoverage(
      ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (!effective.fileGroupPlanRecorded()) {
      return new SnapshotFinalizeCoverageService.ExpectedCoverage(
          SnapshotFinalizeCoverageService.PlannedCoverageState.UNKNOWN,
          List.of(),
          List.of(),
          "snapshot finalization requires explicit snapshot coverage metadata");
    }
    if (effective.completionMode() == ReconcileSnapshotTask.CompletionMode.DIRECT_STATS) {
      return new SnapshotFinalizeCoverageService.ExpectedCoverage(
          SnapshotFinalizeCoverageService.PlannedCoverageState.DIRECT_STATS,
          List.of(),
          List.of(),
          "");
    }
    return new SnapshotFinalizeCoverageService.ExpectedCoverage(
        effective.fileGroupCount() == 0
            ? SnapshotFinalizeCoverageService.PlannedCoverageState.EXPLICIT_EMPTY
            : SnapshotFinalizeCoverageService.PlannedCoverageState.NON_EMPTY,
        List.of(),
        List.of(),
        "");
  }

  private SnapshotFinalizeChildStateService.ChildState requireReadyCompactChildState(
      ReconcileJobStore.LeasedJob lease, int expectedGroupCount) {
    SnapshotFinalizeChildStateService.ChildState childState =
        childStateService.compactChildState(
            lease.accountId, lease.parentJobId, lease.jobId, expectedGroupCount);
    if (!childState.duplicateGroups().isEmpty()
        || !childState.invalidSucceededGroups().isEmpty()
        || !childState.failedGroups().isEmpty()
        || !childState.cancelledGroups().isEmpty()
        || !childState.pendingGroups().isEmpty()
        || !childState.missingGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("snapshot finalization child results are not ready")
          .asRuntimeException();
    }
    return childState;
  }

  private static void requireAcceptedLeaseOutcome(boolean accepted, String jobId) {
    if (!accepted) {
      throw ReconcileLeaseGrpcStatus.leasePreconditionFailed(
          "reconcile lease is no longer valid for job " + jobId);
    }
  }

  private ReconcileJobStore.LeasedJob requireLeasedSnapshotFinalizeJob(
      String corr, String jobId, String leaseEpoch) {
    boolean renewed = jobs.renewLease(jobId, leaseEpoch);
    if (!renewed) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile lease is no longer valid")
          .asRuntimeException();
    }
    ReconcileJobStore.ReconcileJob job =
        jobs.getCompactLeaseView(jobId)
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription("reconcile job not found " + jobId)
                        .asRuntimeException());
    if (job.jobKind != ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is not a FINALIZE_SNAPSHOT_CAPTURE job")
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
        job.captureMode,
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

  private static ReconcileSnapshotTask requireSnapshotTask(ReconcileJobStore.LeasedJob lease) {
    ReconcileSnapshotTask snapshotTask =
        lease == null || lease.snapshotTask == null
            ? ReconcileSnapshotTask.empty()
            : lease.snapshotTask;
    if (snapshotTask.isEmpty()
        || snapshotTask.tableId().isBlank()
        || snapshotTask.snapshotId() < 0L) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot task is required for FINALIZE_SNAPSHOT_CAPTURE result submission")
          .asRuntimeException();
    }
    return snapshotTask;
  }

  private static ResourceId tableId(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask snapshotTask) {
    return ResourceId.newBuilder()
        .setAccountId(lease.accountId)
        .setKind(ResourceKind.RK_TABLE)
        .setId(snapshotTask.tableId())
        .build();
  }

  private static SubmitLeasedSnapshotFinalizeResultRequest.Success successPayload(
      String resultId, SnapshotFinalizeStatsDescriptor descriptor) {
    return SubmitLeasedSnapshotFinalizeResultRequest.Success.newBuilder()
        .setResultId(resultId)
        .setStatsDescriptor(descriptor)
        .build();
  }

  private static SubmitLeasedSnapshotFinalizeResultRequest.Failure failurePayload(
      String resultId, String message) {
    return SubmitLeasedSnapshotFinalizeResultRequest.Failure.newBuilder()
        .setResultId(resultId)
        .setMessage(message == null ? "" : message)
        .build();
  }

  private static String resultIdempotencyKey(String jobId, String resultId) {
    return (jobId == null ? "" : jobId.trim()) + ":" + resultId;
  }

  private static String requireResultId(String resultId) {
    if (resultId == null || resultId.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("result_id is required for snapshot finalize result submission")
          .asRuntimeException();
    }
    return resultId.trim();
  }

  private static Set<FloecatConnector.StatsTargetKind> requestedAggregateKinds(
      ReconcileJobStore.LeasedJob lease) {
    var policy =
        lease == null || lease.scope == null
            ? ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy.empty()
            : lease.scope.capturePolicy();
    EnumSet<FloecatConnector.StatsTargetKind> out =
        EnumSet.noneOf(FloecatConnector.StatsTargetKind.class);
    for (var output : policy.outputs()) {
      switch (output) {
        case TABLE_STATS -> out.add(FloecatConnector.StatsTargetKind.TABLE);
        case COLUMN_STATS -> out.add(FloecatConnector.StatsTargetKind.COLUMN);
        default -> {}
      }
    }
    return out;
  }

  private static boolean requestsStatsOutputs(ReconcileJobStore.LeasedJob lease) {
    var policy =
        lease == null || lease.scope == null
            ? ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy.empty()
            : lease.scope.capturePolicy();
    for (var output : policy.outputs()) {
      switch (output) {
        case TABLE_STATS, FILE_STATS, COLUMN_STATS -> {
          return true;
        }
        default -> {}
      }
    }
    return false;
  }

  private static boolean isActiveLeasedState(String state) {
    return "JS_RUNNING".equals(state) || "JS_CANCELLING".equals(state);
  }

  private static void requireKnownCoverage(
      SnapshotFinalizeCoverageService.ExpectedCoverage coverage) {
    if (coverage.state() == SnapshotFinalizeCoverageService.PlannedCoverageState.UNKNOWN) {
      throw Status.FAILED_PRECONDITION.withDescription(coverage.message()).asRuntimeException();
    }
  }

  private static void requireNoStatsRecords(List<TargetStatsRecord> statsRecords) {
    if (statsRecords != null && !statsRecords.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "snapshot finalize payload must not include stats records for this submission")
          .asRuntimeException();
    }
  }

  private static void requirePlannerDirectStatsRecordCount(
      ReconcileSnapshotTask snapshotTask, int actualRecordCount) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.directStatsRecordCount() > 0
        && actualRecordCount != effective.directStatsRecordCount()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalize direct stats record count mismatch expected="
                  + effective.directStatsRecordCount()
                  + " actual="
                  + actualRecordCount)
          .asRuntimeException();
    }
  }
}
