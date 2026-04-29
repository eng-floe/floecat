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

import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.impl.FileGroupTargetStatsRollup;
import ai.floedb.floecat.reconciler.impl.ReconcileExecutor;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class SnapshotFinalizeReconcileExecutor implements ReconcileExecutor {
  private static final Logger LOG = Logger.getLogger(SnapshotFinalizeReconcileExecutor.class);

  @Inject ReconcileJobStore jobs;
  @Inject StatsStore statsStore;

  @Override
  public String id() {
    return "snapshot_finalize";
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
    ExpectedCoverage coverage = expectedCoverage(snapshotTask);
    if (coverage.state() == PlannedCoverageState.UNKNOWN) {
      return ExecutionResult.terminalFailure(
          0, 0, 0, 0, 1, 0, 0, coverage.message(), new IllegalStateException(coverage.message()));
    }
    Set<FloecatConnector.StatsTargetKind> aggregateKinds = requestedAggregateKinds(lease);
    if (coverage.state() == PlannedCoverageState.EXPLICIT_EMPTY) {
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
          persistEmptySnapshotCompletionMarker(lease, snapshotTask, aggregateKinds);
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
    if (!parentJobId.isBlank() && coverage.expectedFiles().isEmpty()) {
      LOG.warnf(
          "Snapshot finalizer proceeding with zero expected files accountId=%s parentJobId=%s"
              + " tableId=%s snapshotId=%d",
          lease.accountId, parentJobId, snapshotTask.tableId(), snapshotTask.snapshotId());
    }
    if (context.shouldStop().getAsBoolean()) {
      return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
    }
    ChildState childState =
        childState(lease.accountId, parentJobId, lease.jobId, coverage.expectedGroups());
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
      return ExecutionResult.dependencyNotReady(
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          "Waiting for snapshot file groups "
              + childState.completedGroups()
              + "/"
              + childState.expectedGroups()
              + " pending="
              + childState.pendingGroups());
    }
    if (!childState.missingGroups().isEmpty()) {
      return ExecutionResult.stateUncertain(
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

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(snapshotTask.tableId())
            .build();
    List<TargetStatsRecord> fileStats = listFileStats(tableId, snapshotTask.snapshotId());
    CoverageValidation coverageValidation = validateCoverage(coverage.expectedFiles(), fileStats);
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
        FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
                tableId, snapshotTask.snapshotId(), aggregateKinds, fileStats)
            .stream()
            .filter(record -> record != null && !record.hasFile())
            .toList();
    for (TargetStatsRecord aggregateStat : aggregateStats) {
      statsStore.putTargetStats(aggregateStat);
    }
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
      ReconcileJobStore.LeasedJob lease,
      ReconcileSnapshotTask snapshotTask,
      Set<FloecatConnector.StatsTargetKind> aggregateKinds) {
    if (lease == null
        || snapshotTask == null
        || lease.accountId == null
        || lease.accountId.isBlank()
        || snapshotTask.tableId().isBlank()
        || snapshotTask.snapshotId() < 0L) {
      return 0L;
    }
    if (aggregateKinds == null
        || !aggregateKinds.contains(FloecatConnector.StatsTargetKind.TABLE)) {
      return 0L;
    }
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(snapshotTask.tableId())
            .build();
    statsStore.putTargetStats(
        TargetStatsRecords.tableRecord(
            tableId,
            snapshotTask.snapshotId(),
            TableValueStats.newBuilder()
                .setRowCount(0L)
                .setDataFileCount(0L)
                .setTotalSizeBytes(0L)
                .build(),
            null));
    return 1L;
  }

  private ChildState childState(
      String accountId,
      String parentJobId,
      String finalizerJobId,
      List<ReconcileFileGroupTask> expectedGroups) {
    if (parentJobId == null || parentJobId.isBlank()) {
      return new ChildState(0, 0, List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
    }
    HashMap<String, ReconcileJobStore.ReconcileJob> childByGroupKey = new HashMap<>();
    LinkedHashSet<String> duplicateGroups = new LinkedHashSet<>();
    for (ReconcileJobStore.ReconcileJob child : jobs.childJobs(accountId, parentJobId)) {
      if (child == null
          || child.jobId == null
          || child.jobId.equals(finalizerJobId)
          || child.jobKind != ReconcileJobKind.EXEC_FILE_GROUP) {
        continue;
      }
      String groupKey = groupKey(child.fileGroupTask);
      if (groupKey.isBlank()) {
        duplicateGroups.add("unkeyed-child:" + child.jobId);
        continue;
      }
      ReconcileJobStore.ReconcileJob previous = childByGroupKey.putIfAbsent(groupKey, child);
      if (previous != null) {
        duplicateGroups.add(describeGroup(child.fileGroupTask));
      }
    }
    int completedGroups = 0;
    LinkedHashSet<String> pendingGroups = new LinkedHashSet<>();
    LinkedHashSet<String> failedGroups = new LinkedHashSet<>();
    LinkedHashSet<String> cancelledGroups = new LinkedHashSet<>();
    LinkedHashSet<String> missingGroups = new LinkedHashSet<>();
    LinkedHashSet<String> invalidSucceededGroups = new LinkedHashSet<>();
    for (ReconcileFileGroupTask expectedGroup : expectedGroups) {
      String groupKey = groupKey(expectedGroup);
      String description = describeGroup(expectedGroup);
      if (groupKey.isBlank()) {
        missingGroups.add(description);
        continue;
      }
      ReconcileJobStore.ReconcileJob child = childByGroupKey.get(groupKey);
      if (child == null) {
        missingGroups.add(description);
        continue;
      }
      if ("JS_SUCCEEDED".equals(child.state)) {
        if (hasPersistedSuccessResults(expectedGroup, child.fileGroupTask)) {
          completedGroups++;
        } else {
          invalidSucceededGroups.add(description);
        }
      } else if ("JS_FAILED".equals(child.state)) {
        failedGroups.add(describeFailure(child, expectedGroup));
      } else if ("JS_CANCELLED".equals(child.state)) {
        cancelledGroups.add(describeFailure(child, expectedGroup));
      } else {
        pendingGroups.add(description + "(" + blankToUnknown(child.state) + ")");
      }
    }
    return new ChildState(
        expectedGroups.size(),
        completedGroups,
        List.copyOf(pendingGroups),
        List.copyOf(failedGroups),
        List.copyOf(cancelledGroups),
        List.copyOf(duplicateGroups),
        List.copyOf(missingGroups),
        List.copyOf(invalidSucceededGroups));
  }

  private List<String> fileGroupChildDescriptions(
      String accountId, String parentJobId, String finalizerJobId) {
    if (parentJobId == null || parentJobId.isBlank()) {
      return List.of();
    }
    LinkedHashSet<String> childDescriptions = new LinkedHashSet<>();
    for (ReconcileJobStore.ReconcileJob child : jobs.childJobs(accountId, parentJobId)) {
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

  private List<TargetStatsRecord> listFileStats(ResourceId tableId, long snapshotId) {
    List<TargetStatsRecord> out = new ArrayList<>();
    String pageToken = "";
    do {
      StatsStore.StatsStorePage page =
          statsStore.listTargetStats(
              tableId, snapshotId, Optional.of(StatsTargetType.FILE), 256, pageToken);
      out.addAll(page.records());
      pageToken = page.nextPageToken();
    } while (pageToken != null && !pageToken.isBlank());
    return List.copyOf(out);
  }

  private ExpectedCoverage expectedCoverage(ReconcileSnapshotTask snapshotTask) {
    snapshotTask = snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (!snapshotTask.fileGroupPlanRecorded()) {
      return new ExpectedCoverage(
          PlannedCoverageState.UNKNOWN,
          List.of(),
          List.of(),
          "snapshot finalization requires explicit snapshot coverage metadata");
    }
    LinkedHashSet<String> expectedFiles = new LinkedHashSet<>();
    for (ReconcileFileGroupTask fileGroup : snapshotTask.fileGroups()) {
      if (fileGroup == null) {
        continue;
      }
      expectedFiles.addAll(fileGroup.filePaths());
    }
    List<ReconcileFileGroupTask> expectedGroups =
        snapshotTask.fileGroups().stream()
            .filter(group -> group != null && !group.isEmpty())
            .toList();
    if (expectedGroups.isEmpty()) {
      return new ExpectedCoverage(
          PlannedCoverageState.EXPLICIT_EMPTY, List.of(), List.copyOf(expectedFiles), "");
    }
    return new ExpectedCoverage(
        PlannedCoverageState.NON_EMPTY,
        List.copyOf(expectedGroups),
        List.copyOf(expectedFiles),
        "");
  }

  private CoverageValidation validateCoverage(
      List<String> expectedFiles, List<TargetStatsRecord> fileStats) {
    LinkedHashSet<String> expected =
        new LinkedHashSet<>(expectedFiles == null ? List.of() : expectedFiles);
    LinkedHashMap<String, Integer> actualCounts = new LinkedHashMap<>();
    for (TargetStatsRecord record : fileStats) {
      if (record == null || !record.hasFile()) {
        continue;
      }
      String filePath = record.getFile().getFilePath();
      if (filePath == null || filePath.isBlank()) {
        continue;
      }
      actualCounts.merge(filePath, 1, Integer::sum);
    }
    LinkedHashSet<String> actual = new LinkedHashSet<>(actualCounts.keySet());
    LinkedHashSet<String> missing = new LinkedHashSet<>(expected);
    missing.removeAll(actual);
    LinkedHashSet<String> unexpected = new LinkedHashSet<>(actual);
    unexpected.removeAll(expected);
    LinkedHashSet<String> duplicates = new LinkedHashSet<>();
    for (var entry : actualCounts.entrySet()) {
      if (entry.getValue() > 1) {
        duplicates.add(entry.getKey());
      }
    }
    if (missing.isEmpty() && unexpected.isEmpty() && duplicates.isEmpty()) {
      return new CoverageValidation(true, List.of(), List.of(), List.of(), "");
    }

    StringBuilder message = new StringBuilder("Snapshot finalization coverage mismatch");
    if (!missing.isEmpty()) {
      message.append(" missing=").append(missing);
    }
    if (!unexpected.isEmpty()) {
      message.append(" unexpected=").append(unexpected);
    }
    if (!duplicates.isEmpty()) {
      message.append(" duplicates=").append(duplicates);
    }
    return new CoverageValidation(
        false,
        List.copyOf(missing),
        List.copyOf(unexpected),
        List.copyOf(duplicates),
        message.toString());
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

  private static boolean hasPersistedSuccessResults(
      ReconcileFileGroupTask expectedGroup, ReconcileFileGroupTask persistedGroup) {
    if (expectedGroup == null || persistedGroup == null) {
      return false;
    }
    if (!groupKey(expectedGroup).equals(groupKey(persistedGroup))) {
      return false;
    }
    LinkedHashMap<String, ReconcileFileResult.State> statesByFile = new LinkedHashMap<>();
    for (ReconcileFileResult result : persistedGroup.fileResults()) {
      if (result == null || result.filePath().isBlank()) {
        continue;
      }
      statesByFile.put(result.filePath(), result.state());
    }
    for (String filePath : expectedGroup.filePaths()) {
      if (statesByFile.get(filePath) != ReconcileFileResult.State.SUCCEEDED) {
        return false;
      }
    }
    return !expectedGroup.filePaths().isEmpty() || !persistedGroup.fileResults().isEmpty();
  }

  private static String groupKey(ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null) {
      return "";
    }
    String planId = fileGroupTask.planId() == null ? "" : fileGroupTask.planId().trim();
    String groupId = fileGroupTask.groupId() == null ? "" : fileGroupTask.groupId().trim();
    if (planId.isBlank() || groupId.isBlank()) {
      return "";
    }
    return planId + "|" + groupId;
  }

  private static String describeGroup(ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null) {
      return "unknown-group";
    }
    String planId = fileGroupTask.planId() == null ? "" : fileGroupTask.planId().trim();
    String groupId = fileGroupTask.groupId() == null ? "" : fileGroupTask.groupId().trim();
    if (planId.isBlank() && groupId.isBlank()) {
      return "unknown-group";
    }
    return planId + "/" + groupId;
  }

  private static String describeFailure(
      ReconcileJobStore.ReconcileJob child, ReconcileFileGroupTask expectedGroup) {
    String message = child == null || child.message == null ? "" : child.message.trim();
    return message.isBlank()
        ? describeGroup(expectedGroup)
        : describeGroup(expectedGroup) + ": " + message;
  }

  private static String blankToUnknown(String value) {
    return value == null || value.isBlank() ? "unknown" : value;
  }

  private record ChildState(
      int expectedGroups,
      int completedGroups,
      List<String> pendingGroups,
      List<String> failedGroups,
      List<String> cancelledGroups,
      List<String> duplicateGroups,
      List<String> missingGroups,
      List<String> invalidSucceededGroups) {}

  private enum PlannedCoverageState {
    UNKNOWN,
    EXPLICIT_EMPTY,
    NON_EMPTY
  }

  private record ExpectedCoverage(
      PlannedCoverageState state,
      List<ReconcileFileGroupTask> expectedGroups,
      List<String> expectedFiles,
      String message) {}

  private record CoverageValidation(
      boolean valid,
      List<String> missingFiles,
      List<String> unexpectedFiles,
      List<String> duplicateFiles,
      String message) {}
}
