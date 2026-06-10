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
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
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
    if (coverage.state() == SnapshotFinalizeCoverageService.PlannedCoverageState.DIRECT_STATS) {
      try {
        long statsProcessed =
            requestsStatsOutputs
                ? ingestDirectStats(snapshotTask, tableId, lease.fullRescan)
                : snapshotTask.directStatsRecordCount();
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
    List<TargetStatsRecord> loadedFileStats;
    try {
      loadedFileStats = loadFileGroupStatsBlobs(childState.completedGroupTasks());
    } catch (RuntimeException e) {
      return ExecutionResult.failure(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          "File-group stats blob ingest failed for snapshot "
              + snapshotTask.snapshotId()
              + ": "
              + e.getMessage(),
          e);
    }
    if (lease.fullRescan) {
      SnapshotFinalizeCoverageService.CoverageValidation coverageValidation =
          coverageService.validateCoverage(coverage.expectedFiles(), loadedFileStats);
      if (!coverageValidation.valid()) {
        return ExecutionResult.terminalFailure(
            0,
            0,
            0,
            0,
            coverageValidation.missingFiles().size()
                + coverageValidation.unexpectedFiles().size()
                + coverageValidation.duplicateFiles().size(),
            0,
            0,
            coverageValidation.message(),
            new IllegalStateException(coverageValidation.message()));
      }
      persistence.replaceAllStatsForSnapshot(tableId, snapshotTask.snapshotId(), loadedFileStats);
    } else {
      persistence.persistStats(loadedFileStats);
    }
    List<TargetStatsRecord> fileStats =
        lease.fullRescan
            ? List.copyOf(loadedFileStats)
            : persistence.listFileStats(tableId, snapshotTask.snapshotId());
    SnapshotFinalizeCoverageService.CoverageValidation coverageValidation =
        coverageService.validateCoverage(coverage.expectedFiles(), fileStats);
    if (!coverageValidation.valid()) {
      return ExecutionResult.terminalFailure(
          0,
          0,
          0,
          0,
          coverageValidation.missingFiles().size()
              + coverageValidation.unexpectedFiles().size()
              + coverageValidation.duplicateFiles().size(),
          0,
          0,
          coverageValidation.message(),
          new IllegalStateException(coverageValidation.message()));
    }
    if (aggregateKinds.isEmpty()) {
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
    List<TargetStatsRecord> aggregateStats =
        persistence.buildAggregateStats(
            tableId, snapshotTask.snapshotId(), aggregateKinds, fileStats);
    persistence.persistStats(aggregateStats);
    return ExecutionResult.success(
        0,
        0,
        0,
        0,
        0,
        1,
        aggregateStats.size(),
        "Finalized snapshot capture " + snapshotTask.snapshotId());
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
      ReconcileSnapshotTask snapshotTask, ResourceId tableId, boolean fullRescan) {
    List<TargetStatsRecord> records = snapshotPlanBlobStore.loadDirectStats(snapshotTask);
    if (snapshotTask.directStatsRecordCount() > 0
        && records.size() != snapshotTask.directStatsRecordCount()) {
      throw new IllegalStateException(
          "Direct stats blob record count mismatch expected="
              + snapshotTask.directStatsRecordCount()
              + " actual="
              + records.size());
    }
    return fullRescan
        ? persistence.replaceAllStatsForSnapshot(tableId, snapshotTask.snapshotId(), records)
        : persistence.persistStats(records);
  }

  private List<TargetStatsRecord> loadFileGroupStatsBlobs(
      List<ReconcileFileGroupTask> completedGroups) {
    List<TargetStatsRecord> allRecords = new ArrayList<>();
    List<ReconcileFileGroupTask> groups =
        completedGroups == null ? List.<ReconcileFileGroupTask>of() : completedGroups;
    for (ReconcileFileGroupTask group : groups) {
      if (group == null || group.fileStatsBlobUri().isBlank()) {
        continue;
      }
      List<TargetStatsRecord> records =
          snapshotPlanBlobStore.loadFileGroupStats(group.fileStatsBlobUri());
      if (group.fileStatsRecordCount() > 0 && records.size() != group.fileStatsRecordCount()) {
        throw new IllegalStateException(
            "File-group stats blob record count mismatch expected="
                + group.fileStatsRecordCount()
                + " actual="
                + records.size()
                + " group="
                + describeGroup(group));
      }
      for (TargetStatsRecord record : records) {
        allRecords.add(TargetStatsRecords.canonicalize(record));
      }
    }
    return List.copyOf(allRecords);
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
