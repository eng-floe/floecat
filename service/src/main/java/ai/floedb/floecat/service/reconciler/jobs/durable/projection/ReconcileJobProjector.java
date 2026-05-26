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
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobDetailLoader;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ReconcileJobProjector {
  private ReconcileJobDetailLoader detailLoader;

  public void bind(ReconcileJobDetailLoader detailLoader) {
    this.detailLoader = detailLoader;
  }

  public ReconcileJob toPublicJob(StoredReconcileJob stored, boolean includeDetails) {
    return toPublicJob(stored, null, includeDetails);
  }

  public ReconcileJob toPublicTreeJob(
      StoredReconcileJob stored, StoredReconcileJobProjection projection) {
    boolean aggregateSummaryPresent = projection != null && isParentCapable(stored.jobKind());
    ProjectedPublicJob projected =
        isParentCapable(stored.jobKind())
            ? projectedParentJob(stored, projection)
            : projectSelfPublicJob(stored, false);
    StoredJobDefinition definition = detailLoader.requireDefinition(stored);
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
        definition.toScope(),
        stored.executionPolicy(),
        stored.pinnedExecutorId(),
        projected.executorId,
        stored.jobKind(),
        definition.tableTask(),
        definition.viewTask(),
        lightweightSnapshotTask(stored),
        lightweightFileGroupTask(stored),
        projected.projection.plannedFileGroups,
        projected.projection.plannedFiles,
        projected.projection.completedFileGroups,
        projected.projection.failedFileGroups,
        projected.projection.completedFiles,
        projected.projection.failedFiles,
        stored.parentJobId());
  }

  public ReconcileJob toPublicJob(
      StoredReconcileJob stored, StoredReconcileJobProjection projection, boolean includeDetails) {
    boolean aggregateSummaryPresent = projection != null && isParentCapable(stored.jobKind());
    ProjectedPublicJob projected =
        isParentCapable(stored.jobKind())
            ? projectedParentJob(stored, projection)
            : projectSelfPublicJob(stored, includeDetails);
    StoredJobDefinition definition = includeDetails ? detailLoader.requireDefinition(stored) : null;
    ReconcileSnapshotTask snapshotTask =
        includeDetails ? detailLoader.snapshotTask(stored) : ReconcileSnapshotTask.empty();
    ReconcileFileGroupTask fileGroupTask =
        includeDetails ? detailLoader.fileGroupTask(stored) : ReconcileFileGroupTask.empty();
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
    return toPublicJobSummary(stored, null);
  }

  public ReconcileJob toPublicJobSummary(
      StoredReconcileJob stored, StoredReconcileJobProjection storedProjection) {
    boolean aggregateSummaryPresent = storedProjection != null && isParentCapable(stored.jobKind());
    StoredReconcileJobProjection projection = effectiveParentProjection(stored, storedProjection);
    String state =
        isParentCapable(stored.jobKind()) ? projection.state() : blankToEmpty(stored.state);
    return new ReconcileJob(
        stored.jobId,
        stored.accountId,
        stored.connectorId,
        state,
        normalizeWaitingStateMessage(
            state, isParentCapable(stored.jobKind()) ? projection.message() : stored.message),
        isParentCapable(stored.jobKind()) ? projection.startedAtMs() : stored.startedAtMs,
        isParentCapable(stored.jobKind()) ? projection.finishedAtMs() : stored.finishedAtMs,
        isParentCapable(stored.jobKind()) ? projection.tablesScanned() : stored.tablesScanned,
        isParentCapable(stored.jobKind()) ? projection.tablesChanged() : stored.tablesChanged,
        isParentCapable(stored.jobKind()) ? projection.viewsScanned() : stored.viewsScanned,
        isParentCapable(stored.jobKind()) ? projection.viewsChanged() : stored.viewsChanged,
        isParentCapable(stored.jobKind()) ? projection.errors() : stored.errors,
        stored.fullRescan,
        stored.captureMode(),
        isParentCapable(stored.jobKind())
            ? projection.snapshotsProcessed()
            : stored.snapshotsProcessed,
        isParentCapable(stored.jobKind()) ? projection.statsProcessed() : stored.statsProcessed,
        isParentCapable(stored.jobKind())
            ? projection.indexesProcessed()
            : inlineSummaryProjection(stored).indexesProcessed,
        aggregateSummaryPresent,
        ReconcileScope.empty(),
        stored.executionPolicy(),
        stored.pinnedExecutorId(),
        isParentCapable(stored.jobKind()) ? projection.executorId() : stored.executorId(),
        stored.jobKind(),
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        isParentCapable(stored.jobKind())
            ? projection.plannedFileGroups()
            : inlineSummaryProjection(stored).plannedFileGroups,
        isParentCapable(stored.jobKind())
            ? projection.plannedFiles()
            : inlineSummaryProjection(stored).plannedFiles,
        isParentCapable(stored.jobKind())
            ? projection.completedFileGroups()
            : inlineSummaryProjection(stored).completedFileGroups,
        isParentCapable(stored.jobKind())
            ? projection.failedFileGroups()
            : inlineSummaryProjection(stored).failedFileGroups,
        isParentCapable(stored.jobKind())
            ? projection.completedFiles()
            : inlineSummaryProjection(stored).completedFiles,
        isParentCapable(stored.jobKind())
            ? projection.failedFiles()
            : inlineSummaryProjection(stored).failedFiles,
        stored.parentJobId());
  }

  private static ReconcileSnapshotTask lightweightSnapshotTask(StoredReconcileJob stored) {
    if (stored == null) {
      return ReconcileSnapshotTask.empty();
    }
    return ReconcileSnapshotTask.of(
        stored.snapshotTaskTableId,
        stored.snapshotTaskSnapshotId,
        stored.snapshotTaskSourceNamespace,
        stored.snapshotTaskSourceTable,
        java.util.List.of(),
        stored.snapshotTaskFileGroupPlanRecorded,
        ReconcileSnapshotTask.CompletionMode.fromString(stored.snapshotTaskCompletionMode),
        blankToEmpty(stored.snapshotPlanBlobUri),
        0,
        blankToEmpty(stored.snapshotTaskDirectStatsBlobUri),
        (int) Math.max(0L, stored.snapshotTaskDirectStatsRecordCount));
  }

  private static ReconcileFileGroupTask lightweightFileGroupTask(StoredReconcileJob stored) {
    if (stored == null) {
      return ReconcileFileGroupTask.empty();
    }
    return ReconcileFileGroupTask.of(
        stored.fileGroupPlanId,
        stored.fileGroupGroupId,
        stored.fileGroupTableId,
        stored.fileGroupSnapshotId,
        stored.fileGroupFileCount,
        "",
        0,
        java.util.List.of(),
        java.util.List.of());
  }

  public ReconcileJob toCanonicalLeaseView(StoredReconcileJob stored) {
    StoredJobDefinition definition = detailLoader.requireDefinition(stored);
    ReconcileSnapshotTask snapshotTask = detailLoader.snapshotTask(stored);
    ReconcileFileGroupTask fileGroupTask = detailLoader.fileGroupTask(stored);
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
    boolean parentCapable = isParentCapable(stored.jobKind());
    ReconcileSnapshotTask snapshotTask =
        includeSelfProjectionPayloads
                && !parentCapable
                && stored.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
            ? detailLoader.snapshotTask(stored)
            : ReconcileSnapshotTask.empty();
    ReconcileFileGroupTask fileGroupTask =
        includeSelfProjectionPayloads && stored.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP
            ? detailLoader.fileGroupTask(stored)
            : ReconcileFileGroupTask.empty();
    JobProjection selfProjection =
        parentCapable
            ? inlineSummaryProjection(stored)
            : projectJob(stored, snapshotTask, fileGroupTask);
    return ProjectedPublicJob.self(stored, selfProjection);
  }

  public ProjectedPublicJob projectSelfPublicJobForRollup(StoredReconcileJob stored) {
    if (stored == null) {
      return ProjectedPublicJob.empty();
    }
    JobProjection selfProjection =
        isParentCapable(stored.jobKind())
            ? inlineSummaryProjection(stored)
            : intrinsicProjectionForRollup(stored);
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
      long completedFileGroups = Math.max(0L, stored.completedFileGroups);
      long failedFileGroups = Math.max(0L, stored.failedFileGroups);
      long completedFiles = Math.max(0L, stored.completedFiles);
      long failedFiles = Math.max(0L, stored.failedFiles);
      long indexesProcessed = Math.max(0L, stored.indexesProcessed);
      if (completedFileGroups == 0L && failedFileGroups == 0L) {
        completedFileGroups = "JS_SUCCEEDED".equals(stored.state) ? 1L : 0L;
        failedFileGroups =
            ("JS_FAILED".equals(stored.state) || "JS_CANCELLED".equals(stored.state)) ? 1L : 0L;
      }
      if (completedFiles == 0L && failedFiles == 0L) {
        completedFiles = completedFileGroups > 0L ? plannedFiles : 0L;
        failedFiles = failedFileGroups > 0L ? plannedFiles : 0L;
      }
      return new JobProjection(
          indexesProcessed,
          Math.max(1L, Math.max(0L, stored.plannedFileGroups)),
          Math.max(plannedFiles, Math.max(0L, stored.plannedFiles)),
          completedFileGroups,
          failedFileGroups,
          completedFiles,
          failedFiles);
    }
    return JobProjection.empty();
  }

  public JobProjection intrinsicProjectionForDetail(StoredReconcileJob stored) {
    if (stored == null) {
      return JobProjection.empty();
    }
    if (stored.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT) {
      return projectSnapshotPlan(detailLoader.snapshotTask(stored));
    }
    if (stored.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      return projectExecFileGroup(detailLoader.fileGroupTask(stored), stored.state);
    }
    return JobProjection.empty();
  }

  public JobProjection intrinsicProjectionForRollup(StoredReconcileJob stored) {
    if (stored == null) {
      return JobProjection.empty();
    }
    return inlineSummaryProjection(stored);
  }

  public StoredReconcileJobProjection toStoredProjection(StoredReconcileJob stored) {
    if (stored == null) {
      return null;
    }
    JobProjection projection =
        isParentCapable(stored.jobKind())
            ? inlineSummaryProjection(stored)
            : projectSelfPublicJob(stored, true).projection;
    String state = blankToEmpty(stored.state);
    return new StoredReconcileJobProjection(
        blankToEmpty(stored.accountId),
        blankToEmpty(stored.jobId),
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
        projection.indexesProcessed,
        projection.plannedFileGroups,
        projection.plannedFiles,
        projection.completedFileGroups,
        projection.failedFileGroups,
        projection.completedFiles,
        projection.failedFiles,
        blankToEmpty(stored.executorId),
        isParentCapable(stored.jobKind()));
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

  private ProjectedPublicJob projectedParentJob(
      StoredReconcileJob stored, StoredReconcileJobProjection storedProjection) {
    StoredReconcileJobProjection projection = effectiveParentProjection(stored, storedProjection);
    return new ProjectedPublicJob(
        blankToEmpty(projection.state()),
        normalizeWaitingStateMessage(projection.state(), projection.message()),
        projection.startedAtMs(),
        projection.finishedAtMs(),
        projection.tablesScanned(),
        projection.tablesChanged(),
        projection.viewsScanned(),
        projection.viewsChanged(),
        projection.errors(),
        projection.snapshotsProcessed(),
        projection.statsProcessed(),
        blankToEmpty(projection.executorId()),
        new JobProjection(
            projection.indexesProcessed(),
            projection.plannedFileGroups(),
            projection.plannedFiles(),
            projection.completedFileGroups(),
            projection.failedFileGroups(),
            projection.completedFiles(),
            projection.failedFiles()));
  }

  private StoredReconcileJobProjection effectiveParentProjection(
      StoredReconcileJob stored, StoredReconcileJobProjection storedProjection) {
    StoredReconcileJobProjection canonical = canonicalParentProjection(stored);
    if (storedProjection == null) {
      return canonical;
    }
    String canonicalState = blankToEmpty(stored.state);
    String projectionState = blankToEmpty(storedProjection.state());
    String effectiveState = effectiveParentState(canonicalState, projectionState);
    String effectiveMessage =
        effectiveState.equals(canonicalState)
            ? normalizeWaitingStateMessage(canonicalState, stored.message)
            : normalizeWaitingStateMessage(projectionState, storedProjection.message());
    String effectiveExecutorId =
        effectiveState.equals(canonicalState)
            ? blankToEmpty(stored.executorId)
            : blankToEmpty(storedProjection.executorId());
    boolean useProjectedAggregates =
        shouldUseProjectedAggregates(stored, storedProjection, effectiveState);
    long effectiveStartedAtMs =
        useProjectedAggregates
            ? earliestPositiveStartedAtMs(stored.startedAtMs, storedProjection.startedAtMs())
            : stored.startedAtMs;
    return new StoredReconcileJobProjection(
        blankToEmpty(stored.accountId),
        blankToEmpty(stored.jobId),
        effectiveState,
        effectiveMessage,
        effectiveStartedAtMs,
        effectiveFinishedAtMs(effectiveState, stored.finishedAtMs, storedProjection.finishedAtMs()),
        useProjectedAggregates ? storedProjection.tablesScanned() : stored.tablesScanned,
        useProjectedAggregates ? storedProjection.tablesChanged() : stored.tablesChanged,
        useProjectedAggregates ? storedProjection.viewsScanned() : stored.viewsScanned,
        useProjectedAggregates ? storedProjection.viewsChanged() : stored.viewsChanged,
        useProjectedAggregates ? storedProjection.errors() : stored.errors,
        useProjectedAggregates ? storedProjection.snapshotsProcessed() : stored.snapshotsProcessed,
        useProjectedAggregates ? storedProjection.statsProcessed() : stored.statsProcessed,
        useProjectedAggregates ? storedProjection.indexesProcessed() : stored.indexesProcessed,
        useProjectedAggregates ? storedProjection.plannedFileGroups() : stored.plannedFileGroups,
        useProjectedAggregates ? storedProjection.plannedFiles() : stored.plannedFiles,
        useProjectedAggregates
            ? storedProjection.completedFileGroups()
            : stored.completedFileGroups,
        useProjectedAggregates ? storedProjection.failedFileGroups() : stored.failedFileGroups,
        useProjectedAggregates ? storedProjection.completedFiles() : stored.completedFiles,
        useProjectedAggregates ? storedProjection.failedFiles() : stored.failedFiles,
        effectiveExecutorId,
        true);
  }

  private StoredReconcileJobProjection canonicalParentProjection(StoredReconcileJob stored) {
    String state = blankToEmpty(stored.state);
    return new StoredReconcileJobProjection(
        blankToEmpty(stored.accountId),
        blankToEmpty(stored.jobId),
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
        stored.indexesProcessed,
        stored.plannedFileGroups,
        stored.plannedFiles,
        stored.completedFileGroups,
        stored.failedFileGroups,
        stored.completedFiles,
        stored.failedFiles,
        blankToEmpty(stored.executorId),
        false);
  }

  private static String effectiveParentState(String canonicalState, String projectionState) {
    if (canonicalState.isBlank()) {
      return projectionState;
    }
    if ("JS_QUEUED".equals(canonicalState)) {
      return "JS_QUEUED".equals(projectionState) || projectionState.isBlank()
          ? canonicalState
          : projectionState;
    }
    if ("JS_SUCCEEDED".equals(canonicalState)) {
      return switch (projectionState) {
        case "JS_WAITING", "JS_RUNNING", "JS_FAILED", "JS_CANCELLED", "JS_SUCCEEDED" ->
            projectionState;
        default -> canonicalState;
      };
    }
    if (!"JS_WAITING".equals(canonicalState)) {
      return canonicalState;
    }
    return switch (projectionState) {
      case "JS_WAITING", "JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED" -> projectionState;
      default -> canonicalState;
    };
  }

  private static long effectiveFinishedAtMs(
      String effectiveState, long canonicalFinishedAtMs, long projectedFinishedAtMs) {
    if ("JS_RUNNING".equals(effectiveState)
        || "JS_QUEUED".equals(effectiveState)
        || "JS_WAITING".equals(effectiveState)
        || "JS_CANCELLING".equals(effectiveState)) {
      return 0L;
    }
    return Math.max(canonicalFinishedAtMs, projectedFinishedAtMs);
  }

  private static boolean shouldUseProjectedAggregates(
      StoredReconcileJob stored,
      StoredReconcileJobProjection storedProjection,
      String effectiveState) {
    String canonicalState = blankToEmpty(stored == null ? "" : stored.state);
    String projectionState = blankToEmpty(storedProjection == null ? "" : storedProjection.state());
    if (projectionState.isBlank()) {
      return false;
    }
    if ("JS_QUEUED".equals(projectionState) && !hasAggregateMetrics(storedProjection)) {
      return false;
    }
    if ("JS_WAITING".equals(canonicalState) || "JS_SUCCEEDED".equals(canonicalState)) {
      return true;
    }
    if (!effectiveState.equals(canonicalState)) {
      return true;
    }
    if (!hasAggregateMetrics(storedProjection)) {
      return false;
    }
    return !"JS_QUEUED".equals(projectionState);
  }

  private static boolean hasAggregateMetrics(StoredReconcileJobProjection projection) {
    return projection != null
        && (projection.tablesScanned() > 0L
            || projection.tablesChanged() > 0L
            || projection.viewsScanned() > 0L
            || projection.viewsChanged() > 0L
            || projection.errors() > 0L
            || projection.snapshotsProcessed() > 0L
            || projection.statsProcessed() > 0L
            || projection.indexesProcessed() > 0L
            || projection.plannedFileGroups() > 0L
            || projection.plannedFiles() > 0L
            || projection.completedFileGroups() > 0L
            || projection.failedFileGroups() > 0L
            || projection.completedFiles() > 0L
            || projection.failedFiles() > 0L);
  }

  private static long earliestPositiveStartedAtMs(
      long canonicalStartedAtMs, long projectedStartedAtMs) {
    long canonical = Math.max(0L, canonicalStartedAtMs);
    long projected = Math.max(0L, projectedStartedAtMs);
    if (canonical <= 0L) {
      return projected;
    }
    if (projected <= 0L) {
      return canonical;
    }
    return Math.min(canonical, projected);
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
