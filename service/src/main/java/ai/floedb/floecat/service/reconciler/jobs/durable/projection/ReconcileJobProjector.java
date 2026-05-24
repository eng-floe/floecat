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

package ai.floedb.floecat.service.reconciler.jobs.durable.projection;

import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobContribution;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ReconcileJobProjector {
  private ReconcilePayloadStore payloadStore;

  public void bind(ReconcilePayloadStore payloadStore) {
    this.payloadStore = payloadStore;
  }

  public ReconcileJob toPublicJob(StoredReconcileJob stored, boolean includeDetails) {
    boolean aggregateSummaryPresent = isParentCapable(stored.jobKind());
    ProjectedPublicJob projected =
        aggregateSummaryPresent
            ? ProjectedPublicJob.self(stored, inlineSummaryProjection(stored))
            : projectSelfPublicJob(stored, includeDetails);
    StoredJobDefinition definition = includeDetails ? payloadStore.requireDefinition(stored) : null;
    ReconcileSnapshotTask snapshotTask =
        includeDetails ? payloadStore.snapshotTaskFor(stored) : ReconcileSnapshotTask.empty();
    ReconcileFileGroupTask fileGroupTask =
        includeDetails ? payloadStore.fileGroupTaskFor(stored) : ReconcileFileGroupTask.empty();
    return new ReconcileJob(
        stored.jobId,
        stored.accountId,
        stored.connectorId,
        projected.state,
        projected.message,
        projected.startedAtMs,
        projected.finishedAtMs,
        projected.tablesScanned,
        projected.tablesChanged,
        projected.viewsScanned,
        projected.viewsChanged,
        projected.errors,
        stored.fullRescan,
        stored.captureMode(),
        projected.snapshotsProcessed,
        projected.statsProcessed,
        projected.projection.indexesProcessed,
        aggregateSummaryPresent,
        includeDetails ? definition.toScope() : ReconcileScope.empty(),
        stored.executionPolicy(),
        stored.pinnedExecutorId(),
        projected.executorId,
        stored.jobKind(),
        includeDetails ? definition.tableTask() : ReconcileTableTask.empty(),
        includeDetails ? definition.viewTask() : ReconcileViewTask.empty(),
        snapshotTask,
        fileGroupTask,
        projected.projection.plannedFileGroups,
        projected.projection.plannedFiles,
        projected.projection.completedFileGroups,
        projected.projection.failedFileGroups,
        projected.projection.completedFiles,
        projected.projection.failedFiles,
        stored.parentJobId());
  }

  public ReconcileJob toPublicJobSummary(StoredReconcileJob stored) {
    boolean aggregateSummaryPresent = isParentCapable(stored.jobKind());
    JobProjection projection = inlineSummaryProjection(stored);
    String state = blankToEmpty(stored.state);
    return new ReconcileJob(
        stored.jobId,
        stored.accountId,
        stored.connectorId,
        state,
        normalizeWaitingStateMessage(state, stored.message),
        stored.startedAtMs,
        stored.finishedAtMs,
        stored.tablesScanned,
        stored.tablesChanged,
        stored.viewsScanned,
        stored.viewsChanged,
        stored.errors,
        stored.fullRescan,
        stored.captureMode(),
        stored.snapshotsProcessed,
        stored.statsProcessed,
        projection.indexesProcessed,
        aggregateSummaryPresent,
        ReconcileScope.empty(),
        stored.executionPolicy(),
        stored.pinnedExecutorId(),
        stored.executorId(),
        stored.jobKind(),
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        projection.plannedFileGroups,
        projection.plannedFiles,
        projection.completedFileGroups,
        projection.failedFileGroups,
        projection.completedFiles,
        projection.failedFiles,
        stored.parentJobId());
  }

  public ReconcileJob toCanonicalLeaseView(StoredReconcileJob stored) {
    StoredJobDefinition definition = payloadStore.requireDefinition(stored);
    ReconcileSnapshotTask snapshotTask = payloadStore.snapshotTaskFor(stored);
    ReconcileFileGroupTask fileGroupTask = payloadStore.fileGroupTaskFor(stored);
    JobProjection projection = inlineSummaryProjection(stored);
    String state = blankToEmpty(stored.state);
    return new ReconcileJob(
        stored.jobId,
        stored.accountId,
        stored.connectorId,
        state,
        normalizeWaitingStateMessage(state, stored.message),
        stored.startedAtMs,
        stored.finishedAtMs,
        stored.tablesScanned,
        stored.tablesChanged,
        stored.viewsScanned,
        stored.viewsChanged,
        stored.errors,
        stored.fullRescan,
        stored.captureMode(),
        stored.snapshotsProcessed,
        stored.statsProcessed,
        projection.indexesProcessed,
        false,
        definition.toScope(),
        stored.executionPolicy(),
        stored.pinnedExecutorId(),
        stored.executorId(),
        stored.jobKind(),
        definition.tableTask(),
        definition.viewTask(),
        snapshotTask,
        fileGroupTask,
        projection.plannedFileGroups,
        projection.plannedFiles,
        projection.completedFileGroups,
        projection.failedFileGroups,
        projection.completedFiles,
        projection.failedFiles,
        stored.parentJobId());
  }

  public ProjectedPublicJob projectSelfPublicJob(
      StoredReconcileJob stored, boolean includeSelfProjectionPayloads) {
    if (stored == null) {
      return ProjectedPublicJob.empty();
    }
    ReconcileSnapshotTask snapshotTask =
        includeSelfProjectionPayloads && stored.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
            ? payloadStore.snapshotTaskFor(stored)
            : ReconcileSnapshotTask.empty();
    ReconcileFileGroupTask fileGroupTask =
        includeSelfProjectionPayloads && stored.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP
            ? payloadStore.fileGroupTaskFor(stored)
            : ReconcileFileGroupTask.empty();
    JobProjection selfProjection =
        isParentCapable(stored.jobKind())
            ? inlineSummaryProjection(stored)
            : projectJob(stored, snapshotTask, fileGroupTask);
    return ProjectedPublicJob.self(stored, selfProjection);
  }

  public JobProjection projectJob(
      StoredReconcileJob stored,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask) {
    if (stored == null) {
      return JobProjection.empty();
    }
    if (stored.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      return projectExecFileGroup(fileGroupTask, stored.state);
    }
    if (stored.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT) {
      return projectSnapshotPlan(snapshotTask);
    }
    return JobProjection.empty();
  }

  public JobProjection inlineSummaryProjection(StoredReconcileJob stored) {
    if (stored == null) {
      return JobProjection.empty();
    }
    if (isParentCapable(stored.jobKind())) {
      return new JobProjection(
          stored.indexesProcessed,
          stored.plannedFileGroups,
          stored.plannedFiles,
          stored.completedFileGroups,
          stored.failedFileGroups,
          stored.completedFiles,
          stored.failedFiles);
    }
    if (stored.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      long plannedFiles = Math.max(0L, stored.fileGroupFileCount);
      long completedFileGroups = "JS_SUCCEEDED".equals(stored.state) ? 1L : 0L;
      long failedFileGroups =
          ("JS_FAILED".equals(stored.state) || "JS_CANCELLED".equals(stored.state)) ? 1L : 0L;
      long completedFiles = completedFileGroups > 0L ? plannedFiles : 0L;
      long failedFiles = failedFileGroups > 0L ? plannedFiles : 0L;
      return new JobProjection(
          0L, 1L, plannedFiles, completedFileGroups, failedFileGroups, completedFiles, failedFiles);
    }
    return JobProjection.empty();
  }

  public DirectChildCounts countDirectChildStates(
      java.util.List<StoredJobContribution> contributions) {
    DirectChildCounts counts = DirectChildCounts.empty();
    for (StoredJobContribution contribution : contributions) {
      counts = counts.incrementedBy(contribution == null ? "" : contribution.state);
    }
    return counts;
  }

  public JobProjection projectSnapshotPlan(ReconcileSnapshotTask snapshotTask) {
    if (snapshotTask == null || snapshotTask.isEmpty()) {
      return JobProjection.empty();
    }
    long plannedFileGroups =
        snapshotTask.fileGroupCount() > 0
            ? snapshotTask.fileGroupCount()
            : snapshotTask.fileGroups().size();
    long plannedFiles =
        snapshotTask.fileGroups().stream().mapToLong(this::plannedFilesForGroup).sum();
    return new JobProjection(0L, plannedFileGroups, plannedFiles, 0L, 0L, 0L, 0L);
  }

  public JobProjection projectExecFileGroup(ReconcileFileGroupTask fileGroupTask, String state) {
    if (fileGroupTask == null || fileGroupTask.isEmpty()) {
      return JobProjection.empty();
    }
    long plannedFiles = plannedFilesForGroup(fileGroupTask);
    long indexesProcessed = 0L;
    long completedFiles = 0L;
    long failedFiles = 0L;
    for (ReconcileFileResult result : fileGroupTask.fileResults()) {
      if (result == null || result.isEmpty()) {
        continue;
      }
      if (hasIndexArtifact(result)) {
        indexesProcessed++;
      }
      if (result.state() == ReconcileFileResult.State.SUCCEEDED) {
        completedFiles++;
      } else {
        failedFiles++;
      }
    }
    if (completedFiles == 0L && failedFiles == 0L) {
      if ("JS_SUCCEEDED".equals(state)) {
        completedFiles = plannedFiles;
      } else if ("JS_FAILED".equals(state) || "JS_CANCELLED".equals(state)) {
        failedFiles = plannedFiles;
      }
    }
    return new JobProjection(
        indexesProcessed,
        1L,
        plannedFiles,
        "JS_SUCCEEDED".equals(state) ? 1L : 0L,
        ("JS_FAILED".equals(state) || "JS_CANCELLED".equals(state)) ? 1L : 0L,
        completedFiles,
        failedFiles);
  }

  public boolean isParentCapable(ReconcileJobKind jobKind) {
    return jobKind == ReconcileJobKind.PLAN_CONNECTOR
        || jobKind == ReconcileJobKind.PLAN_TABLE
        || jobKind == ReconcileJobKind.PLAN_SNAPSHOT;
  }

  public static String normalizeSucceededMessage(String message) {
    String normalized = blankToEmpty(message);
    if (normalized.isBlank()) {
      return "Succeeded";
    }
    if (normalized.startsWith("Planned ") || normalized.startsWith("Snapshot plan recorded")) {
      return "Succeeded";
    }
    return switch (normalized) {
      case "Queued",
          "Queued (full)",
          "Leased",
          "Running",
          "Waiting",
          "Cancelling",
          "Retrying",
          "Waiting on dependency",
          "Waiting on child work" ->
          "Succeeded";
      default -> normalized;
    };
  }

  public static String normalizeWaitingStateMessage(String state, String message) {
    String normalizedState = blankToEmpty(state);
    String normalizedMessage = blankToEmpty(message);
    if (!"JS_WAITING".equals(normalizedState)) {
      return normalizedMessage;
    }
    return switch (normalizedMessage) {
      case "", "Queued", "Queued (full)", "Leased", "Running", "Waiting", "Retrying" ->
          "Waiting on child work";
      default -> normalizedMessage;
    };
  }

  private static boolean hasIndexArtifact(ReconcileFileResult result) {
    return result != null
        && result.indexArtifact() != null
        && (!result.indexArtifact().artifactUri().isBlank()
            || !result.indexArtifact().artifactFormat().isBlank()
            || result.indexArtifact().artifactFormatVersion() > 0);
  }

  private long plannedFilesForGroup(ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null || fileGroupTask.isEmpty()) {
      return 0L;
    }
    if (fileGroupTask.fileCount() > 0) {
      return fileGroupTask.fileCount();
    }
    return fileGroupTask.filePaths().size();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  public record ProjectedPublicJob(
      String state,
      String message,
      long startedAtMs,
      long finishedAtMs,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String executorId,
      JobProjection projection) {
    public static ProjectedPublicJob empty() {
      return new ProjectedPublicJob(
          "", "", 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, "", JobProjection.empty());
    }

    public static ProjectedPublicJob self(StoredReconcileJob stored, JobProjection projection) {
      String state = blankToEmpty(stored.state);
      return new ProjectedPublicJob(
          state,
          normalizeWaitingStateMessage(state, stored.message),
          stored.startedAtMs,
          stored.finishedAtMs,
          stored.tablesScanned,
          stored.tablesChanged,
          stored.viewsScanned,
          stored.viewsChanged,
          stored.errors,
          stored.snapshotsProcessed,
          stored.statsProcessed,
          blankToEmpty(stored.executorId),
          projection);
    }
  }

  public record DirectChildCounts(long completed, long failed, long cancelled, long totalObserved) {
    public static DirectChildCounts empty() {
      return new DirectChildCounts(0L, 0L, 0L, 0L);
    }

    public DirectChildCounts incrementedBy(String state) {
      return switch (blankToEmpty(state)) {
        case "JS_SUCCEEDED" ->
            new DirectChildCounts(completed + 1L, failed, cancelled, totalObserved + 1L);
        case "JS_FAILED" ->
            new DirectChildCounts(completed, failed + 1L, cancelled, totalObserved + 1L);
        case "JS_CANCELLED" ->
            new DirectChildCounts(completed, failed, cancelled + 1L, totalObserved + 1L);
        default -> new DirectChildCounts(completed, failed, cancelled, totalObserved + 1L);
      };
    }

    public long totalTerminal() {
      return completed + failed + cancelled;
    }
  }

  public record JobProjection(
      long indexesProcessed,
      long plannedFileGroups,
      long plannedFiles,
      long completedFileGroups,
      long failedFileGroups,
      long completedFiles,
      long failedFiles) {
    public static JobProjection empty() {
      return new JobProjection(0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }
  }
}
