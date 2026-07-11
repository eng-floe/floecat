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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.impl.ReconcileExecutor;
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class SnapshotFinalizeReconcileExecutor implements ReconcileExecutor {
  private static final Logger LOG = Logger.getLogger(SnapshotFinalizeReconcileExecutor.class);

  @Inject ReconcileJobStore jobs;
  @Inject SnapshotPlanBlobStore snapshotPlanBlobStore;
  @Inject SnapshotFinalizePersistenceService persistence;
  @Inject SnapshotFinalizeChildStateService childStateService;
  @Inject SnapshotFinalizeCoverageService coverageService;
  @Inject CurrentSnapshotPointerService currentSnapshotPointerService;

  @ConfigProperty(
      name = "floecat.reconciler.executor.snapshot-finalize.enabled",
      defaultValue = "true")
  boolean enabled;

  @Override
  public String id() {
    return "snapshot_finalize";
  }

  @Override
  public boolean enabled() {
    return enabled;
  }

  @Override
  public int priority() {
    return 30;
  }

  @Override
  public Set<ReconcileJobKind> supportedJobKinds() {
    return EnumSet.of(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE);
  }

  @Override
  public Set<String> supportedLanes() {
    return Set.of();
  }

  @Override
  public boolean supportsLane(String lane) {
    return true;
  }

  @Override
  public boolean supports(ReconcileJobStore.LeasedJob lease) {
    return lease != null && lease.jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE;
  }

  @Override
  public ExecutionResult execute(ExecutionContext context) {
    ReconcileJobStore.LeasedJob lease = context.lease();
    if (lease == null || lease.jobKind != ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE) {
      return ExecutionResult.terminalFailure(
          0, 0, 0, 0, 1, 0, 0, "Unsupported reconcile job kind", new IllegalArgumentException());
    }
    ReconcileSnapshotTask snapshotTask =
        lease.snapshotTask == null ? ReconcileSnapshotTask.empty() : lease.snapshotTask;
    if (snapshotTask.isEmpty()
        || snapshotTask.tableId().isBlank()
        || snapshotTask.snapshotId() < 0L) {
      return ExecutionResult.terminalFailure(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          "snapshot task is required for FINALIZE_SNAPSHOT_CAPTURE jobs",
          new IllegalArgumentException("snapshot task is required"));
    }

    String parentJobId = lease.parentJobId == null ? "" : lease.parentJobId.trim();
    SnapshotFinalizeCoverageService.ExpectedCoverage coverage =
        coverageService.expectedCoverage(snapshotTask);
    if (coverage.state() == SnapshotFinalizeCoverageService.PlannedCoverageState.UNKNOWN) {
      return ExecutionResult.terminalFailure(
          0, 0, 0, 0, 1, 0, 0, coverage.message(), new IllegalStateException(coverage.message()));
    }
    if (parentJobId.isBlank()) {
      return ExecutionResult.terminalFailure(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          "snapshot finalization requires parent snapshot plan job",
          new IllegalStateException("parent snapshot plan job is required"));
    }
    boolean requestsStatsOutputs = requestsStatsOutputs(lease);
    Set<FloecatConnector.StatsTargetKind> aggregateKinds = requestedAggregateKinds(lease);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(snapshotTask.tableId())
            .build();
    Optional<ReconcileJobStore.FinalizedSnapshotEvent> finalizedSnapshot =
        jobs.getFinalizedSnapshot(
            lease.accountId, snapshotTask.tableId(), snapshotTask.snapshotId());
    if (finalizedSnapshot.isPresent()
        && !lease.jobId.equals(finalizedSnapshot.orElseThrow().finalizerJobId)) {
      ReconcileJobStore.FinalizedSnapshotEvent finalized = finalizedSnapshot.orElseThrow();
      String message =
          "Snapshot "
              + snapshotTask.snapshotId()
              + " already finalized by job "
              + finalized.finalizerJobId;
      LOG.infof(
          "Skipping stale snapshot finalizer jobId=%s tableId=%s snapshotId=%d finalizedBy=%s",
          lease.jobId, snapshotTask.tableId(), snapshotTask.snapshotId(), finalized.finalizerJobId);
      return ExecutionResult.obsolete(
          0, 0, 0, 0, 0, 0, 0, ExecutionResult.FailureKind.NONE, message, null);
    }
    if (coverage.state() == SnapshotFinalizeCoverageService.PlannedCoverageState.DIRECT_STATS) {
      try {
        long statsProcessed =
            requestsStatsOutputs
                ? ingestDirectStats(snapshotTask, tableId, lease.fullRescan, aggregateKinds)
                : snapshotTask.directStatsRecordCount();
        RuntimeException pointerFailure = advanceCurrentSnapshot(tableId, snapshotTask, lease);
        if (pointerFailure != null) {
          return currentSnapshotAdvanceFailure(snapshotTask, pointerFailure);
        }
        return ExecutionResult.success(
            0,
            0,
            0,
            0,
            0,
            1,
            statsProcessed,
            "Finalized snapshot capture " + snapshotTask.snapshotId() + " from direct stats");
      } catch (IllegalStateException e) {
        return ExecutionResult.terminalFailure(0, 0, 0, 0, 1, 0, 0, e.getMessage(), e);
      } catch (RuntimeException e) {
        return ExecutionResult.failure(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            "Direct stats blob ingest failed for snapshot "
                + snapshotTask.snapshotId()
                + ": "
                + e.getMessage(),
            e);
      }
    }
    if (coverage.state() == SnapshotFinalizeCoverageService.PlannedCoverageState.EXPLICIT_EMPTY) {
      List<String> unexpectedChildren =
          fileGroupChildDescriptions(lease.accountId, parentJobId, lease.jobId);
      if (!unexpectedChildren.isEmpty()) {
        return ExecutionResult.terminalFailure(
            0,
            0,
            0,
            0,
            unexpectedChildren.size(),
            0,
            0,
            "Snapshot finalization found EXEC_FILE_GROUP children for explicit-empty coverage "
                + unexpectedChildren,
            new IllegalStateException("snapshot file-group child jobs unexpected for empty plan"));
      }
      long statsProcessed =
          requestsStatsOutputs
              ? persistEmptySnapshotCompletionMarker(lease, snapshotTask, tableId)
              : 0L;
      RuntimeException pointerFailure = advanceCurrentSnapshot(tableId, snapshotTask, lease);
      if (pointerFailure != null) {
        return currentSnapshotAdvanceFailure(snapshotTask, pointerFailure);
      }
      return ExecutionResult.success(
          0,
          0,
          0,
          0,
          0,
          1,
          statsProcessed,
          "Skipped snapshot finalization "
              + snapshotTask.snapshotId()
              + " (no planned file groups)");
    }
    if (coverage.expectedFiles().isEmpty()) {
      LOG.warnf(
          "Snapshot finalizer proceeding with zero expected files accountId=%s parentJobId=%s"
              + " tableId=%s snapshotId=%d",
          lease.accountId, parentJobId, snapshotTask.tableId(), snapshotTask.snapshotId());
    }
    if (context.shouldStop().getAsBoolean()) {
      return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
    }
    SnapshotFinalizeChildStateService.ChildState childState =
        childStateService.childState(
            lease.accountId, parentJobId, lease.jobId, coverage.expectedGroups());
    if (!childState.duplicateGroups().isEmpty()) {
      return ExecutionResult.terminalFailure(
          0,
          0,
          0,
          0,
          childState.duplicateGroups().size(),
          0,
          0,
          "Snapshot finalization found duplicate EXEC_FILE_GROUP children for planned groups "
              + childState.duplicateGroups(),
          new IllegalStateException("snapshot file-group child jobs duplicated"));
    }
    if (!childState.invalidSucceededGroups().isEmpty()) {
      return ExecutionResult.terminalFailure(
          0,
          0,
          0,
          0,
          childState.invalidSucceededGroups().size(),
          0,
          0,
          "Snapshot finalization found succeeded file-group jobs without persisted success"
              + " results "
              + childState.invalidSucceededGroups(),
          new IllegalStateException("snapshot file-group results incomplete"));
    }
    if (!childState.failedGroups().isEmpty()) {
      return ExecutionResult.terminalFailure(
          0,
          0,
          0,
          0,
          childState.failedGroups().size(),
          0,
          0,
          "Snapshot finalization blocked by failed file-group jobs " + childState.failedGroups(),
          new IllegalStateException("snapshot file-group execution failed"));
    }
    if (!childState.cancelledGroups().isEmpty()) {
      return ExecutionResult.obsolete(
          0,
          0,
          0,
          0,
          childState.cancelledGroups().size(),
          0,
          0,
          ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
          "Snapshot finalization blocked by cancelled file-group jobs "
              + childState.cancelledGroups(),
          new IllegalStateException("snapshot file-group execution cancelled"));
    }
    if (!childState.pendingGroups().isEmpty()) {
      return ExecutionResult.terminalFailure(
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          "Snapshot finalization was scheduled before file-group completion "
              + "completed="
              + childState.completedGroups()
              + "/"
              + childState.expectedGroups()
              + " pending="
              + childState.pendingGroups(),
          new IllegalStateException(
              "snapshot finalization scheduled before file-group completion"));
    }
    if (!childState.missingGroups().isEmpty()) {
      return ExecutionResult.terminalFailure(
          0,
          0,
          0,
          0,
          childState.missingGroups().size(),
          0,
          0,
          "Snapshot finalization missing EXEC_FILE_GROUP children for planned groups "
              + childState.missingGroups(),
          new IllegalStateException("snapshot file-group child jobs missing"));
    }
    if (!requestsStatsOutputs) {
      RuntimeException pointerFailure = advanceCurrentSnapshot(tableId, snapshotTask, lease);
      if (pointerFailure != null) {
        return currentSnapshotAdvanceFailure(snapshotTask, pointerFailure);
      }
      return ExecutionResult.success(
          0,
          0,
          0,
          0,
          0,
          1,
          0,
          "Skipped snapshot finalization " + snapshotTask.snapshotId() + " (no stats outputs)");
    }
    List<TargetStatsRecord> aggregateStats =
        aggregateKinds.isEmpty()
            ? List.of()
            : persistence.mergeCompletedGroupPartials(
                tableId,
                snapshotTask.snapshotId(),
                aggregateKinds,
                childState.completedGroupTasks());
    if (aggregateKinds.isEmpty() && !lease.fullRescan) {
      RuntimeException pointerFailure = advanceCurrentSnapshot(tableId, snapshotTask, lease);
      if (pointerFailure != null) {
        return currentSnapshotAdvanceFailure(snapshotTask, pointerFailure);
      }
      return ExecutionResult.success(
          0,
          0,
          0,
          0,
          0,
          1,
          0,
          "Skipped snapshot finalization " + snapshotTask.snapshotId() + " (no aggregate outputs)");
    }
    long statsProcessed =
        lease.fullRescan
            ? persistence.replaceFileGroupStatsForSnapshot(
                tableId, snapshotTask.snapshotId(), coverage.expectedFiles(), aggregateStats)
            : persistence.persistStats(aggregateStats);
    RuntimeException pointerFailure = advanceCurrentSnapshot(tableId, snapshotTask, lease);
    if (pointerFailure != null) {
      return currentSnapshotAdvanceFailure(snapshotTask, pointerFailure);
    }
    return ExecutionResult.success(
        0,
        0,
        0,
        0,
        0,
        1,
        statsProcessed,
        "Finalized snapshot capture " + snapshotTask.snapshotId());
  }

  private RuntimeException advanceCurrentSnapshot(
      ResourceId tableId, ReconcileSnapshotTask snapshotTask, ReconcileJobStore.LeasedJob lease) {
    if (currentSnapshotPointerService == null) {
      return null;
    }
    String corr = lease == null || lease.jobId == null ? "" : lease.jobId;
    try {
      // A reconcile pass may re-finalize a snapshot that is already current; the advance is a
      // pointer no-op then, but it still re-commits the snapshot's root entry — the periodic
      // self-heal that converges a root a failed commit left behind.
      currentSnapshotPointerService.maybeAdvance(tableId, snapshotTask.snapshotId(), corr);
      return null;
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "Could not advance current snapshot pointer for finalized table %s snapshot %d",
          tableId == null ? "" : tableId.getId(),
          snapshotTask == null ? -1L : snapshotTask.snapshotId());
      return e;
    }
  }

  private ExecutionResult currentSnapshotAdvanceFailure(
      ReconcileSnapshotTask snapshotTask, RuntimeException error) {
    long snapshotId = snapshotTask == null ? -1L : snapshotTask.snapshotId();
    return ExecutionResult.failure(
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        ExecutionResult.FailureKind.INTERNAL,
        "Current snapshot pointer advance failed for snapshot "
            + snapshotId
            + ": "
            + (error == null ? "" : error.getMessage()),
        error);
  }

  private long persistEmptySnapshotCompletionMarker(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask snapshotTask, ResourceId tableId) {
    if (lease == null
        || snapshotTask == null
        || tableId == null
        || lease.accountId == null
        || lease.accountId.isBlank()
        || snapshotTask.tableId().isBlank()
        || snapshotTask.snapshotId() < 0L) {
      return 0L;
    }
    return persistence.persistEmptySnapshotCompletionMarker(
        tableId, snapshotTask.snapshotId(), lease.fullRescan);
  }

  private List<String> fileGroupChildDescriptions(
      String accountId, String parentJobId, String finalizerJobId) {
    if (parentJobId == null || parentJobId.isBlank()) {
      return List.of();
    }
    LinkedHashSet<String> childDescriptions = new LinkedHashSet<>();
    for (ReconcileJobStore.ReconcileJob child : childJobs(accountId, parentJobId)) {
      if (child == null
          || child.jobId == null
          || child.jobId.equals(finalizerJobId)
          || child.jobKind != ReconcileJobKind.EXEC_FILE_GROUP) {
        continue;
      }
      String description = describeGroup(child.fileGroupTask);
      childDescriptions.add(
          description.equals("unknown-group") ? "unknown-group:" + child.jobId : description);
    }
    return List.copyOf(childDescriptions);
  }

  private List<ReconcileJobStore.ReconcileJob> childJobs(String accountId, String parentJobId) {
    if (accountId == null || accountId.isBlank() || parentJobId == null || parentJobId.isBlank()) {
      return List.of();
    }
    List<ReconcileJobStore.ReconcileJob> out = new ArrayList<>();
    String pageToken = "";
    do {
      ReconcileJobStore.ReconcileJobPage page =
          jobs.childJobsPage(accountId, parentJobId, 200, pageToken);
      if (page == null || page.jobs == null || page.jobs.isEmpty()) {
        break;
      }
      out.addAll(page.jobs);
      pageToken = page.nextPageToken == null ? "" : page.nextPageToken;
    } while (!pageToken.isBlank());
    return List.copyOf(out);
  }

  private long ingestDirectStats(
      ReconcileSnapshotTask snapshotTask,
      ResourceId tableId,
      boolean fullRescan,
      Set<FloecatConnector.StatsTargetKind> aggregateKinds) {
    List<TargetStatsRecord> records = snapshotPlanBlobStore.loadDirectStats(snapshotTask);
    if (snapshotTask.directStatsRecordCount() > 0
        && records.size() != snapshotTask.directStatsRecordCount()) {
      throw new IllegalStateException(
          "Direct stats blob record count mismatch expected="
              + snapshotTask.directStatsRecordCount()
              + " actual="
              + records.size());
    }
    List<TargetStatsRecord> completedRecords =
        aggregateKinds.isEmpty()
            ? records
            : persistence.completeStatsWithAggregates(
                tableId, snapshotTask.snapshotId(), aggregateKinds, records);
    return fullRescan
        ? persistence.replaceAllStatsForSnapshot(
            tableId, snapshotTask.snapshotId(), completedRecords)
        : persistence.persistStats(completedRecords);
  }

  private static Set<FloecatConnector.StatsTargetKind> requestedAggregateKinds(
      ReconcileJobStore.LeasedJob lease) {
    ReconcileCapturePolicy policy =
        lease == null || lease.scope == null
            ? ReconcileCapturePolicy.empty()
            : lease.scope.capturePolicy();
    EnumSet<FloecatConnector.StatsTargetKind> out =
        EnumSet.noneOf(FloecatConnector.StatsTargetKind.class);
    for (ReconcileCapturePolicy.Output output : policy.outputs()) {
      switch (output) {
        case TABLE_STATS -> out.add(FloecatConnector.StatsTargetKind.TABLE);
        case COLUMN_STATS -> out.add(FloecatConnector.StatsTargetKind.COLUMN);
        default -> {}
      }
    }
    return out;
  }

  private static boolean requestsStatsOutputs(ReconcileJobStore.LeasedJob lease) {
    ReconcileCapturePolicy policy =
        lease == null || lease.scope == null
            ? ReconcileCapturePolicy.empty()
            : lease.scope.capturePolicy();
    for (ReconcileCapturePolicy.Output output : policy.outputs()) {
      switch (output) {
        case TABLE_STATS, FILE_STATS, COLUMN_STATS -> {
          return true;
        }
        default -> {}
      }
    }
    return false;
  }

  private static String describeGroup(ReconcileFileGroupTask fileGroupTask) {
    return SnapshotFinalizeChildStateService.describeGroup(fileGroupTask);
  }
}
