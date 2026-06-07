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
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore;
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
  @Inject SnapshotPlanBlobStore snapshotPlanBlobStore;
  @Inject SnapshotFinalizeCoverageService coverageService;

  public boolean persistSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      String statsBlobUri,
      int statsRecordCount,
      SubmitLeasedSnapshotFinalizeResultRequest.SuccessMode mode) {
    ReconcileJobStore.LeasedJob lease =
        requireLeasedSnapshotFinalizeJob(principalContext.getCorrelationId(), jobId, leaseEpoch);
    ReconcileSnapshotTask snapshotTask = requireSnapshotTask(lease);
    ResourceId tableId = tableId(lease, snapshotTask);
    String requiredResultId = requireResultId(resultId);
    SubmitLeasedSnapshotFinalizeResultRequest.SuccessMode effectiveMode = requireMode(mode);
    String effectiveBlobUri = effectiveStatsBlobUri(effectiveMode, statsBlobUri);
    int effectiveRecordCount = requireStatsRecordCount(statsRecordCount);
    byte[] requestBytes =
        successPayload(requiredResultId, effectiveBlobUri, effectiveRecordCount, effectiveMode)
            .toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "SubmitLeasedSnapshotFinalizeResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () -> {
                      persistSuccessOutputBlob(
                          lease,
                          snapshotTask,
                          tableId,
                          snapshotTask.snapshotId(),
                          effectiveBlobUri,
                          effectiveRecordCount,
                          effectiveMode);
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

  void persistSuccessOutputBlob(
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      ResourceId tableId,
      long snapshotId,
      String statsBlobUri,
      int statsRecordCount,
      SubmitLeasedSnapshotFinalizeResultRequest.SuccessMode mode) {
    SnapshotFinalizeCoverageService.ExpectedCoverage coverage =
        coverageService.expectedCoverage(snapshotTask);
    requireKnownCoverage(coverage);
    switch (mode) {
      case SFM_REPLACE_ALL -> {
        List<TargetStatsRecord> records = loadStatsBlob(statsBlobUri, statsRecordCount);
        List<TargetStatsRecord> replacement =
            persistence.validateReplacementStats(records, tableId, snapshotId);
        requireValidCoverage(
            coverageService.validateCoverage(coverage.expectedFiles(), replacement));
        persistence.replaceAllStatsForSnapshot(tableId, snapshotId, replacement);
      }
      case SFM_INCREMENTAL_DELTA -> {
        List<TargetStatsRecord> records = loadStatsBlob(statsBlobUri, statsRecordCount);
        List<TargetStatsRecord> deltaFileStats =
            persistence.validateIncrementalDeltaFileStats(records, tableId, snapshotId);
        persistence.persistStats(deltaFileStats);
        List<TargetStatsRecord> fileStats = persistence.listFileStats(tableId, snapshotId);
        requireValidCoverage(coverageService.validateCoverage(coverage.expectedFiles(), fileStats));
        Set<FloecatConnector.StatsTargetKind> aggregateKinds = requestedAggregateKinds(lease);
        if (!aggregateKinds.isEmpty()) {
          List<TargetStatsRecord> aggregateStats =
              persistence.buildAggregateStats(tableId, snapshotId, aggregateKinds, fileStats);
          persistence.persistStats(aggregateStats);
        }
      }
      case SFM_DIRECT_STATS -> {
        requireDirectStatsCoverage(coverage);
        if (!requestsStatsOutputs(lease)) {
          return;
        }
        List<TargetStatsRecord> records = loadStatsBlob(statsBlobUri, statsRecordCount);
        List<TargetStatsRecord> directStats =
            persistence.validateReplacementStats(records, tableId, snapshotId);
        if (lease.fullRescan) {
          persistence.replaceAllStatsForSnapshot(tableId, snapshotId, directStats);
        } else {
          persistence.persistStats(directStats);
        }
      }
      case SFM_EMPTY_SNAPSHOT -> {
        requireExplicitEmptyCoverage(coverage);
        if (requestsStatsOutputs(lease)) {
          persistence.persistEmptySnapshotCompletionMarker(tableId, snapshotId, lease.fullRescan);
        }
      }
      default ->
          throw Status.INVALID_ARGUMENT
              .withDescription("snapshot finalize success mode is required")
              .asRuntimeException();
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

  private static SubmitLeasedSnapshotFinalizeResultRequest.Success successPayload(
      String resultId,
      String statsBlobUri,
      int statsRecordCount,
      SubmitLeasedSnapshotFinalizeResultRequest.SuccessMode mode) {
    return SubmitLeasedSnapshotFinalizeResultRequest.Success.newBuilder()
        .setResultId(resultId)
        .setStatsBlobUri(statsBlobUri == null ? "" : statsBlobUri)
        .setStatsRecordCount(Math.max(0, statsRecordCount))
        .setMode(mode)
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

  private static String requireStatsBlobUri(String statsBlobUri) {
    if (statsBlobUri == null || statsBlobUri.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("stats_blob_uri is required for snapshot finalize result submission")
          .asRuntimeException();
    }
    return statsBlobUri.trim();
  }

  private static String effectiveStatsBlobUri(
      SubmitLeasedSnapshotFinalizeResultRequest.SuccessMode mode, String statsBlobUri) {
    return switch (mode) {
      case SFM_EMPTY_SNAPSHOT -> statsBlobUri == null ? "" : statsBlobUri.trim();
      case SFM_REPLACE_ALL, SFM_INCREMENTAL_DELTA, SFM_DIRECT_STATS ->
          requireStatsBlobUri(statsBlobUri);
      default -> statsBlobUri == null ? "" : statsBlobUri.trim();
    };
  }

  private static int requireStatsRecordCount(int statsRecordCount) {
    if (statsRecordCount < 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("stats_record_count must be non-negative")
          .asRuntimeException();
    }
    return statsRecordCount;
  }

  private static SubmitLeasedSnapshotFinalizeResultRequest.SuccessMode requireMode(
      SubmitLeasedSnapshotFinalizeResultRequest.SuccessMode mode) {
    if (mode == null
        || mode == SubmitLeasedSnapshotFinalizeResultRequest.SuccessMode.SFM_UNSPECIFIED) {
      throw Status.INVALID_ARGUMENT
          .withDescription("snapshot finalize success mode is required")
          .asRuntimeException();
    }
    return mode;
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

  private List<TargetStatsRecord> loadStatsBlob(String statsBlobUri, int statsRecordCount) {
    List<TargetStatsRecord> records = snapshotPlanBlobStore.loadTargetStatsBlob(statsBlobUri);
    if (statsRecordCount > 0 && records.size() != statsRecordCount) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalize stats blob record count mismatch expected="
                  + statsRecordCount
                  + " actual="
                  + records.size())
          .asRuntimeException();
    }
    return records;
  }

  private static void requireKnownCoverage(
      SnapshotFinalizeCoverageService.ExpectedCoverage coverage) {
    if (coverage.state() == SnapshotFinalizeCoverageService.PlannedCoverageState.UNKNOWN) {
      throw Status.FAILED_PRECONDITION.withDescription(coverage.message()).asRuntimeException();
    }
  }

  private static void requireValidCoverage(
      SnapshotFinalizeCoverageService.CoverageValidation coverageValidation) {
    if (!coverageValidation.valid()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(coverageValidation.message())
          .asRuntimeException();
    }
  }

  private static void requireDirectStatsCoverage(
      SnapshotFinalizeCoverageService.ExpectedCoverage coverage) {
    if (coverage.state() != SnapshotFinalizeCoverageService.PlannedCoverageState.DIRECT_STATS) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "snapshot finalize direct-stats submission requires direct-stats coverage")
          .asRuntimeException();
    }
  }

  private static void requireExplicitEmptyCoverage(
      SnapshotFinalizeCoverageService.ExpectedCoverage coverage) {
    if (coverage.state() != SnapshotFinalizeCoverageService.PlannedCoverageState.EXPLICIT_EMPTY) {
      throw Status.FAILED_PRECONDITION
          .withDescription("snapshot finalize empty submission requires explicit-empty coverage")
          .asRuntimeException();
    }
  }
}
