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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest;
import ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultResponse;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.MutationOps;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class LeasedSnapshotFinalizeExecutionService extends BaseServiceImpl {
  @Inject ReconcileJobStore jobs;
  @Inject ai.floedb.floecat.service.repo.IdempotencyRepository idempotencyStore;
  @Inject SnapshotFinalizePersistenceService persistence;
  @Inject SnapshotFinalizeCoverageService coverageService;
  @Inject SnapshotFinalizeChildStateService childStateService;

  public boolean persistChunk(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      int chunkIndex,
      List<TargetStatsRecord> statsRecords) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedSnapshotFinalizeJob(principalContext.getCorrelationId(), jobId, leaseEpoch);
    ReconcileSnapshotTask snapshotTask = requireSnapshotTask(lease);
    ResourceId tableId = tableId(lease, snapshotTask);
    String requiredResultId = requireResultId(resultId);
    List<TargetStatsRecord> nonNullRecords =
        statsRecords == null
            ? List.of()
            : statsRecords.stream().filter(java.util.Objects::nonNull).toList();
    byte[] requestBytes = chunkPayload(requiredResultId, chunkIndex, nonNullRecords).toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedSnapshotFinalizeResult",
                    chunkIdempotencyKey(jobId, requiredResultId, chunkIndex),
                    () -> requestBytes,
                    () -> {
                      persistStatsChunk(
                          lease,
                          snapshotTask,
                          tableId,
                          snapshotTask.snapshotId(),
                          Math.max(0, chunkIndex),
                          nonNullRecords);
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

  public boolean persistSuccess(
      PrincipalContext principalContext, String jobId, String leaseEpoch, String resultId) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedSnapshotFinalizeJob(principalContext.getCorrelationId(), jobId, leaseEpoch);
    ReconcileSnapshotTask snapshotTask = requireSnapshotTask(lease);
    ResourceId tableId = tableId(lease, snapshotTask);
    String requiredResultId = requireResultId(resultId);
    byte[] requestBytes = successPayload(requiredResultId).toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedSnapshotFinalizeResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () -> {
                      finalizeChunkedSuccess(
                          lease, snapshotTask, tableId, snapshotTask.snapshotId());
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
                              snapshotTask.directStatsPersistedRecordCount());
                      return new IdempotencyGuard.CreateResult<>(
                          SubmitLeasedSnapshotFinalizeResultResponse.newBuilder()
                              .setAccepted(accepted)
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

  void persistStatsChunk(
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      ResourceId tableId,
      long snapshotId,
      int chunkIndex,
      List<TargetStatsRecord> statsRecords) {
    SnapshotFinalizeCoverageService.ExpectedCoverage coverage =
        coverageService.expectedCoverage(snapshotTask);
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
          return;
        }
        persistence.persistStats(aggregateStats);
        jobs.persistSnapshotFinalizeDirectStatsProgress(
            lease.jobId, lease.leaseEpoch, lease.fullRescan, chunkIndex, aggregateStats.size());
      }
      case DIRECT_STATS -> {
        if (!requestsStatsOutputs) {
          requireNoStatsRecords(statsRecords);
          return;
        }
        List<TargetStatsRecord> directStats =
            persistence.validateReplacementStats(statsRecords, tableId, snapshotId);
        if (directStats.isEmpty()) {
          if (lease.fullRescan && chunkIndex == 0) {
            persistence.deleteAllStatsForSnapshot(tableId, snapshotId);
            jobs.persistSnapshotFinalizeDirectStatsProgress(
                lease.jobId, lease.leaseEpoch, true, 0, 0);
          }
          return;
        }
        if (lease.fullRescan && chunkIndex == 0) {
          persistence.replaceAllStatsForSnapshot(tableId, snapshotId, directStats);
        } else {
          persistence.persistStats(directStats);
        }
        jobs.persistSnapshotFinalizeDirectStatsProgress(
            lease.jobId, lease.leaseEpoch, lease.fullRescan, chunkIndex, directStats.size());
      }
      case EXPLICIT_EMPTY -> {
        requireNoStatsRecords(statsRecords);
      }
      default ->
          throw Status.FAILED_PRECONDITION.withDescription(coverage.message()).asRuntimeException();
    }
  }

  void finalizeChunkedSuccess(
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      ResourceId tableId,
      long snapshotId) {
    SnapshotFinalizeCoverageService.ExpectedCoverage coverage =
        coverageService.expectedCoverage(snapshotTask);
    requireKnownCoverage(coverage);
    boolean requestsStatsOutputs = requestsStatsOutputs(lease);
    switch (coverage.state()) {
      case NON_EMPTY -> {
        if (!requestsStatsOutputs) {
          return;
        }
        if (!lease.fullRescan && snapshotTask.directStatsPersistedRecordCount() > 0) {
          return;
        }
        SnapshotFinalizeChildStateService.ChildState childState =
            requireReadyChildState(lease, coverage);
        Set<FloecatConnector.StatsTargetKind> aggregateKinds = requestedAggregateKinds(lease);
        List<TargetStatsRecord> mergedAggregates =
            aggregateKinds.isEmpty()
                ? List.of()
                : persistence.mergeCompletedGroupPartials(
                    tableId, snapshotId, aggregateKinds, childState.completedGroupTasks());
        if (lease.fullRescan) {
          persistence.replaceFileGroupStatsForSnapshot(
              tableId, snapshotId, coverage.expectedFiles(), mergedAggregates);
        } else if (!mergedAggregates.isEmpty()) {
          persistence.persistStats(mergedAggregates);
        }
      }
      case DIRECT_STATS -> {
        if (!requestsStatsOutputs) {
          return;
        }
        requirePlannerDirectStatsRecordCount(
            snapshotTask, snapshotTask.directStatsPersistedRecordCount());
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

  private ReconcileJobStore.LeasedJob requireLeasedSnapshotFinalizeJob(
      String corr, String jobId, String leaseEpoch) {
    boolean renewed = jobs.renewLease(jobId, leaseEpoch);
    if (!renewed) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile lease is no longer valid")
          .asRuntimeException();
    }
    ReconcileJobStore.ReconcileJob job =
        jobs.getLeaseView(jobId)
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

  private static SubmitLeasedSnapshotFinalizeResultRequest.Chunk chunkPayload(
      String resultId, int chunkIndex, List<TargetStatsRecord> statsRecords) {
    return SubmitLeasedSnapshotFinalizeResultRequest.Chunk.newBuilder()
        .setResultId(resultId)
        .setChunkIndex(Math.max(0, chunkIndex))
        .addAllStatsRecords(statsRecords == null ? List.of() : statsRecords)
        .build();
  }

  private static SubmitLeasedSnapshotFinalizeResultRequest.Success successPayload(String resultId) {
    return SubmitLeasedSnapshotFinalizeResultRequest.Success.newBuilder()
        .setResultId(resultId)
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

  private static String chunkIdempotencyKey(String jobId, String resultId, int chunkIndex) {
    return resultIdempotencyKey(jobId, resultId) + ":chunk:" + Math.max(0, chunkIndex);
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
              "snapshot finalize chunk must not include stats records for this submission")
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

  private SnapshotFinalizeChildStateService.ChildState requireReadyChildState(
      ReconcileJobStore.LeasedJob lease,
      SnapshotFinalizeCoverageService.ExpectedCoverage coverage) {
    SnapshotFinalizeChildStateService.ChildState childState =
        childStateService.childState(
            lease.accountId, lease.parentJobId, lease.jobId, coverage.expectedGroups());
    if (!childState.duplicateGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization found duplicate EXEC_FILE_GROUP children for planned groups "
                  + childState.duplicateGroups())
          .asRuntimeException();
    }
    if (!childState.invalidSucceededGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization found succeeded file-group jobs without persisted success"
                  + " results "
                  + childState.invalidSucceededGroups())
          .asRuntimeException();
    }
    if (!childState.failedGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization blocked by failed file-group jobs "
                  + childState.failedGroups())
          .asRuntimeException();
    }
    if (!childState.cancelledGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization blocked by cancelled file-group jobs "
                  + childState.cancelledGroups())
          .asRuntimeException();
    }
    if (!childState.pendingGroups().isEmpty() || !childState.missingGroups().isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalization waiting for snapshot file groups "
                  + (childState.pendingGroups().isEmpty()
                      ? childState.missingGroups()
                      : childState.pendingGroups()))
          .asRuntimeException();
    }
    return childState;
  }
}
