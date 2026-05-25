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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.DirectChildCounts;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.JobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.ProjectedPublicJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class ReconcileAncestorRollupService {
  public record SnapshotEnvelope(CanonicalPointerSnapshot snapshot, StoredReconcileJob record) {}

  @FunctionalInterface
  public interface LoadSnapshotByJobId {
    Optional<SnapshotEnvelope> apply(String jobId);
  }

  @FunctionalInterface
  public interface LoadSnapshotByCanonicalKey {
    Optional<SnapshotEnvelope> apply(String canonicalPointerKey);
  }

  @FunctionalInterface
  public interface ReconcileLeaseLiveness {
    boolean hasLiveLease(StoredReconcileJob record, boolean tolerateLeasePointerDrift, long nowMs);
  }

  private ReconcileJobIndexStore jobIndexStore;
  private ReconcileJobProjector projector;
  private LoadSnapshotByJobId loadSnapshotByJobId;
  private LoadSnapshotByCanonicalKey loadSnapshotByCanonicalKey;
  private ReconcileLeaseLiveness leaseLiveness;

  public void bind(
      ReconcileJobIndexStore jobIndexStore,
      ReconcileJobProjector projector,
      LoadSnapshotByJobId loadSnapshotByJobId,
      LoadSnapshotByCanonicalKey loadSnapshotByCanonicalKey,
      ReconcileLeaseLiveness leaseLiveness) {
    this.jobIndexStore = jobIndexStore;
    this.projector = projector;
    this.loadSnapshotByJobId = loadSnapshotByJobId;
    this.loadSnapshotByCanonicalKey = loadSnapshotByCanonicalKey;
    this.leaseLiveness = leaseLiveness;
  }

  public List<ReconcileJobIndexStore.CanonicalRecordMutation> buildAncestorMutations(
      StoredReconcileJob previousChild, StoredReconcileJob currentChild) {
    List<ReconcileJobIndexStore.CanonicalRecordMutation> mutations = new ArrayList<>();
    StoredReconcileJob prior = previousChild;
    StoredReconcileJob current = currentChild;
    while (current != null && !blank(current.parentJobId)) {
      SnapshotEnvelope parentEnvelope = loadSnapshotByJobId.apply(current.parentJobId).orElse(null);
      if (parentEnvelope == null || parentEnvelope.record() == null) {
        break;
      }
      StoredReconcileJob recomputedParent =
          applyChildDelta(parentEnvelope.record(), prior, current);
      if (!sameParentSummary(parentEnvelope.record(), recomputedParent)) {
        recomputedParent.updatedAtMs = System.currentTimeMillis();
        recomputedParent.canonicalPointerKey = parentEnvelope.snapshot().canonicalPointerKey();
        mutations.add(
            new ReconcileJobIndexStore.CanonicalRecordMutation(
                parentEnvelope.snapshot(),
                jobIndexStore.cloneStoredRecord(parentEnvelope.record()),
                recomputedParent));
      }
      prior = parentEnvelope.record();
      current = recomputedParent;
    }
    return mutations;
  }

  private StoredReconcileJob applyChildDelta(
      StoredReconcileJob parent,
      StoredReconcileJob previousChild,
      StoredReconcileJob currentChild) {
    if (parent == null || !projector.isParentCapable(parent.jobKind())) {
      return parent;
    }

    StoredReconcileJob next = jobIndexStore.cloneStoredRecord(parent);
    ChildContribution previousContribution = contributionForParent(parent, previousChild);
    ChildContribution currentContribution = contributionForParent(parent, currentChild);
    JobProjection intrinsicProjection = projector.intrinsicProjection(parent);
    next.childTablesScanned =
        Math.max(
            0L,
            Math.max(0L, parent.childTablesScanned)
                - previousContribution.tablesScanned()
                + currentContribution.tablesScanned());
    next.childTablesChanged =
        Math.max(
            0L,
            Math.max(0L, parent.childTablesChanged)
                - previousContribution.tablesChanged()
                + currentContribution.tablesChanged());
    next.childViewsScanned =
        Math.max(
            0L,
            Math.max(0L, parent.childViewsScanned)
                - previousContribution.viewsScanned()
                + currentContribution.viewsScanned());
    next.childViewsChanged =
        Math.max(
            0L,
            Math.max(0L, parent.childViewsChanged)
                - previousContribution.viewsChanged()
                + currentContribution.viewsChanged());
    next.childErrors =
        Math.max(
            0L,
            Math.max(0L, parent.childErrors)
                - previousContribution.errors()
                + currentContribution.errors());
    next.childSnapshotsProcessed =
        Math.max(
            0L,
            Math.max(0L, parent.childSnapshotsProcessed)
                - previousContribution.snapshotsProcessed()
                + currentContribution.snapshotsProcessed());
    next.childStatsProcessed =
        Math.max(
            0L,
            Math.max(0L, parent.childStatsProcessed)
                - previousContribution.statsProcessed()
                + currentContribution.statsProcessed());
    next.childIndexesProcessed =
        Math.max(
            0L,
            Math.max(0L, parent.childIndexesProcessed)
                - previousContribution.indexesProcessed()
                + currentContribution.indexesProcessed());
    next.childPlannedFileGroups =
        Math.max(
            0L,
            Math.max(0L, parent.childPlannedFileGroups)
                - previousContribution.plannedFileGroups()
                + currentContribution.plannedFileGroups());
    next.childPlannedFiles =
        Math.max(
            0L,
            Math.max(0L, parent.childPlannedFiles)
                - previousContribution.plannedFiles()
                + currentContribution.plannedFiles());
    next.childCompletedFileGroups =
        Math.max(
            0L,
            Math.max(0L, parent.childCompletedFileGroups)
                - previousContribution.completedFileGroups()
                + currentContribution.completedFileGroups());
    next.childFailedFileGroups =
        Math.max(
            0L,
            Math.max(0L, parent.childFailedFileGroups)
                - previousContribution.failedFileGroups()
                + currentContribution.failedFileGroups());
    next.childCompletedFiles =
        Math.max(
            0L,
            Math.max(0L, parent.childCompletedFiles)
                - previousContribution.completedFiles()
                + currentContribution.completedFiles());
    next.childFailedFiles =
        Math.max(
            0L,
            Math.max(0L, parent.childFailedFiles)
                - previousContribution.failedFiles()
                + currentContribution.failedFiles());
    next.tablesScanned = next.childTablesScanned;
    next.tablesChanged = next.childTablesChanged;
    next.viewsScanned = next.childViewsScanned;
    next.viewsChanged = next.childViewsChanged;
    next.errors = next.childErrors;
    next.snapshotsProcessed = next.childSnapshotsProcessed;
    next.statsProcessed = next.childStatsProcessed;
    next.indexesProcessed = next.childIndexesProcessed;
    next.plannedFileGroups =
        Math.max(intrinsicProjection.plannedFileGroups(), next.childPlannedFileGroups);
    next.plannedFiles = Math.max(intrinsicProjection.plannedFiles(), next.childPlannedFiles);
    next.completedFileGroups = next.childCompletedFileGroups;
    next.failedFileGroups = next.childFailedFileGroups;
    next.completedFiles = next.childCompletedFiles;
    next.failedFiles = next.childFailedFiles;

    next.expectedChildJobs =
        Math.max(
            0L,
            Math.max(0L, parent.expectedChildJobs)
                - previousContribution.directChildObserved()
                + currentContribution.directChildObserved());
    next.queuedChildJobs =
        Math.max(
            0L,
            Math.max(0L, parent.queuedChildJobs)
                - previousContribution.queuedChildJobs()
                + currentContribution.queuedChildJobs());
    next.waitingChildJobs =
        Math.max(
            0L,
            Math.max(0L, parent.waitingChildJobs)
                - previousContribution.waitingChildJobs()
                + currentContribution.waitingChildJobs());
    next.runningChildJobs =
        Math.max(
            0L,
            Math.max(0L, parent.runningChildJobs)
                - previousContribution.runningChildJobs()
                + currentContribution.runningChildJobs());
    next.cancellingChildJobs =
        Math.max(
            0L,
            Math.max(0L, parent.cancellingChildJobs)
                - previousContribution.cancellingChildJobs()
                + currentContribution.cancellingChildJobs());
    next.completedChildJobs =
        Math.max(
            0L,
            Math.max(0L, parent.completedChildJobs)
                - previousContribution.completedChildJobs()
                + currentContribution.completedChildJobs());
    next.failedChildJobs =
        Math.max(
            0L,
            Math.max(0L, parent.failedChildJobs)
                - previousContribution.failedChildJobs()
                + currentContribution.failedChildJobs());
    next.cancelledChildJobs =
        Math.max(
            0L,
            Math.max(0L, parent.cancelledChildJobs)
                - previousContribution.cancelledChildJobs()
                + currentContribution.cancelledChildJobs());
    long directTerminalChildren =
        Math.max(0L, next.completedChildJobs)
            + Math.max(0L, next.failedChildJobs)
            + Math.max(0L, next.cancelledChildJobs);
    if (Math.max(0L, next.expectedChildJobs) > 0L
        && directTerminalChildren >= Math.max(0L, next.expectedChildJobs)
        && next.waitingChildJobs == 0L
        && next.runningChildJobs == 0L
        && next.cancellingChildJobs == 0L) {
      // Parent jobs may intentionally remain queued while direct children execute. When the
      // direct child set is now fully terminal, any residual queued count is stale and must not
      // block terminalization.
      next.queuedChildJobs = 0L;
    }
    next.maxDirectChildFinishedAtMs =
        Math.max(
            Math.max(0L, parent.maxDirectChildFinishedAtMs), currentContribution.finishedAtMs());
    if (currentContribution.startedAtMs() > 0L) {
      next.startedAtMs =
          parent.startedAtMs <= 0L
              ? currentContribution.startedAtMs()
              : Math.min(parent.startedAtMs, currentContribution.startedAtMs());
    }

    DirectChildCounts directChildCounts =
        new DirectChildCounts(
            next.completedChildJobs,
            next.failedChildJobs,
            next.cancelledChildJobs,
            next.expectedChildJobs);
    long directChildJobs = Math.max(0L, next.expectedChildJobs);
    boolean allSucceeded =
        directChildJobs > 0L
            && next.completedChildJobs >= directChildJobs
            && next.failedChildJobs == 0L
            && next.cancelledChildJobs == 0L
            && next.queuedChildJobs == 0L
            && next.waitingChildJobs == 0L
            && next.runningChildJobs == 0L
            && next.cancellingChildJobs == 0L;
    ProjectedPublicJob currentProjected =
        currentChild == null
            ? ProjectedPublicJob.empty()
            : projector.projectSelfPublicJob(currentChild, true);
    String currentChildState = effectiveProjectedState(currentChild, currentProjected);
    String currentChildMessage = blankToEmpty(currentProjected.message());
    String currentChildExecutorId = blankToEmpty(currentProjected.executorId());
    if (parent.jobKind() == ReconcileJobKind.PLAN_CONNECTOR) {
      next.tablesScanned =
          Math.max(
              Math.max(0L, parent.tablesScanned),
              Math.max(
                  Math.max(0L, next.childTablesScanned), Math.max(0L, next.expectedChildJobs)));
    }
    boolean leaseOwnsCanonicalState =
        leaseLiveness.hasLiveLease(parent, true, System.currentTimeMillis());
    String state = blankToEmpty(parent.state);
    String message = blankToEmpty(parent.message);
    String executorId = blankToEmpty(parent.executorId);
    if (!leaseOwnsCanonicalState) {
      if ("JS_FAILED".equals(parent.state)) {
        state = "JS_FAILED";
        message = blankToEmpty(parent.message);
      } else if ("JS_CANCELLED".equals(parent.state)) {
        state = "JS_CANCELLED";
        message = blankToEmpty(parent.message);
      } else if (next.cancellingChildJobs > 0L) {
        state = "JS_CANCELLING";
        message = firstNonBlank(currentChildMessage, message, "Cancelling");
        executorId = firstNonBlank(currentChildExecutorId, executorId);
      } else if (next.runningChildJobs > 0L) {
        state = "JS_RUNNING";
        message = firstNonBlank(currentChildMessage, message, "Running");
        executorId = firstNonBlank(currentChildExecutorId, executorId);
      } else if (next.failedChildJobs > 0L) {
        state = "JS_FAILED";
        message =
            "JS_FAILED".equals(currentChildState)
                ? firstNonBlank(currentChildMessage, message, "Failed")
                : firstNonBlank(message, "Failed");
        executorId =
            "JS_FAILED".equals(currentChildState)
                ? firstNonBlank(currentChildExecutorId, executorId)
                : executorId;
      } else if (next.cancelledChildJobs > 0L) {
        state = "JS_CANCELLED";
        message =
            "JS_CANCELLED".equals(currentChildState)
                ? firstNonBlank(currentChildMessage, message, "Cancelled")
                : firstNonBlank(message, "Cancelled");
        executorId =
            "JS_CANCELLED".equals(currentChildState)
                ? firstNonBlank(currentChildExecutorId, executorId)
                : executorId;
      } else if (next.waitingChildJobs > 0L) {
        state = "JS_WAITING";
        message =
            ReconcileJobProjector.normalizeWaitingStateMessage(
                "JS_WAITING", firstNonBlank(currentChildMessage, message, "Waiting on child work"));
        executorId = "";
      } else if ("JS_WAITING".equals(parent.state)
          && !directChildJobsComplete(next.expectedChildJobs, directChildCounts)) {
        state = "JS_WAITING";
        message =
            ReconcileJobProjector.normalizeWaitingStateMessage(
                "JS_WAITING", firstNonBlank(parent.message, "Waiting on child work"));
        executorId = "";
      } else if (next.queuedChildJobs > 0L
          && ("JS_RUNNING".equals(parent.state)
              || "JS_WAITING".equals(parent.state)
              || "JS_SUCCEEDED".equals(parent.state)
              || isDependencyWaitingQueuedChild(currentProjected))) {
        state = "JS_WAITING";
        message =
            ReconcileJobProjector.normalizeWaitingStateMessage(
                "JS_WAITING", firstNonBlank(currentChildMessage, message, "Waiting on child work"));
        executorId = "";
      } else if (allSucceeded
          && directChildJobsComplete(next.expectedChildJobs, directChildCounts)) {
        state = "JS_SUCCEEDED";
        message = ReconcileJobProjector.normalizeSucceededMessage(message);
      } else if (isLogicalSucceededParent(next, message)) {
        state = "JS_SUCCEEDED";
        message = ReconcileJobProjector.normalizeSucceededMessage(message);
      } else if (allSucceeded && next.expectedChildJobs > 0L) {
        state = "JS_WAITING";
        message = "Waiting on child work";
        executorId = "";
      } else if ("JS_RUNNING".equals(state) && next.expectedChildJobs > 0L) {
        state = "JS_WAITING";
        message = "Waiting on child work";
        executorId = "";
      }
    }

    long finishedAtMs =
        leaseOwnsCanonicalState
            ? parent.finishedAtMs
            : (isTerminalState(state) ? maxTerminalFinishedAtMs(parent, next, state) : 0L);
    if (isTerminalState(parent.state) && !blank(parent.executorId)) {
      executorId = parent.executorId;
    }

    next.finishedAtMs = finishedAtMs;
    next.state = state;
    next.message = message;
    next.executorId = executorId;
    if (isTerminalState(state)) {
      next.readyPointerKey = null;
    }
    return next;
  }

  private boolean sameParentSummary(StoredReconcileJob previous, StoredReconcileJob current) {
    if (previous == current) {
      return true;
    }
    if (previous == null || current == null) {
      return false;
    }
    return previous.tablesScanned == current.tablesScanned
        && previous.tablesChanged == current.tablesChanged
        && previous.viewsScanned == current.viewsScanned
        && previous.viewsChanged == current.viewsChanged
        && previous.errors == current.errors
        && previous.snapshotsProcessed == current.snapshotsProcessed
        && previous.statsProcessed == current.statsProcessed
        && previous.indexesProcessed == current.indexesProcessed
        && previous.plannedFileGroups == current.plannedFileGroups
        && previous.plannedFiles == current.plannedFiles
        && previous.completedFileGroups == current.completedFileGroups
        && previous.failedFileGroups == current.failedFileGroups
        && previous.completedFiles == current.completedFiles
        && previous.failedFiles == current.failedFiles
        && previous.childTablesScanned == current.childTablesScanned
        && previous.childTablesChanged == current.childTablesChanged
        && previous.childViewsScanned == current.childViewsScanned
        && previous.childViewsChanged == current.childViewsChanged
        && previous.childErrors == current.childErrors
        && previous.childSnapshotsProcessed == current.childSnapshotsProcessed
        && previous.childStatsProcessed == current.childStatsProcessed
        && previous.childIndexesProcessed == current.childIndexesProcessed
        && previous.childPlannedFileGroups == current.childPlannedFileGroups
        && previous.childPlannedFiles == current.childPlannedFiles
        && previous.childCompletedFileGroups == current.childCompletedFileGroups
        && previous.childFailedFileGroups == current.childFailedFileGroups
        && previous.childCompletedFiles == current.childCompletedFiles
        && previous.childFailedFiles == current.childFailedFiles
        && previous.expectedChildJobs == current.expectedChildJobs
        && previous.queuedChildJobs == current.queuedChildJobs
        && previous.waitingChildJobs == current.waitingChildJobs
        && previous.runningChildJobs == current.runningChildJobs
        && previous.cancellingChildJobs == current.cancellingChildJobs
        && previous.completedChildJobs == current.completedChildJobs
        && previous.failedChildJobs == current.failedChildJobs
        && previous.cancelledChildJobs == current.cancelledChildJobs
        && previous.maxDirectChildFinishedAtMs == current.maxDirectChildFinishedAtMs
        && previous.startedAtMs == current.startedAtMs
        && previous.finishedAtMs == current.finishedAtMs
        && blankToEmpty(previous.state).equals(blankToEmpty(current.state))
        && blankToEmpty(previous.message).equals(blankToEmpty(current.message))
        && blankToEmpty(previous.executorId).equals(blankToEmpty(current.executorId))
        && blankToEmpty(previous.readyPointerKey).equals(blankToEmpty(current.readyPointerKey));
  }

  private static boolean directChildJobsComplete(
      long expectedChildJobs, DirectChildCounts directChildCounts) {
    long expected = Math.max(0L, expectedChildJobs);
    return expected <= 0L || directChildCounts.totalTerminal() >= expected;
  }

  private static boolean isDependencyWaitingQueuedChild(ProjectedPublicJob projected) {
    if (projected == null || !"JS_QUEUED".equals(blankToEmpty(projected.state()))) {
      return false;
    }
    String message = blankToEmpty(projected.message());
    return "Waiting on dependency".equals(message) || "Waiting on child work".equals(message);
  }

  private static boolean isTerminalState(String state) {
    return switch (blankToEmpty(state)) {
      case "JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED" -> true;
      default -> false;
    };
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return "";
    }
    for (String value : values) {
      if (!blank(value)) {
        return value.trim();
      }
    }
    return "";
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static long maxTerminalFinishedAtMs(
      StoredReconcileJob parent, StoredReconcileJob current, String projectedState) {
    long result = isTerminalState(parent.state) ? Math.max(0L, parent.finishedAtMs) : 0L;
    if (current != null
        && current.maxDirectChildFinishedAtMs > 0L
        && (!"JS_SUCCEEDED".equals(projectedState)
            || current.completedChildJobs >= current.expectedChildJobs)) {
      result = Math.max(result, current.maxDirectChildFinishedAtMs);
    }
    return result;
  }

  private ChildContribution contributionForParent(
      StoredReconcileJob parent, StoredReconcileJob childRecord) {
    if (parent == null || childRecord == null) {
      return ChildContribution.empty();
    }
    ProjectedPublicJob projected = projector.projectSelfPublicJob(childRecord, true);
    JobProjection projection = projected.projection();
    boolean planTableScanned = planTableContributionScanned(projected, projection);
    boolean planTableChanged = planTableContributionChanged(projected, projection);
    long tablesScanned =
        parent.jobKind() == ReconcileJobKind.PLAN_TABLE
            ? (planTableScanned ? 1L : 0L)
            : projected.tablesScanned();
    long tablesChanged =
        parent.jobKind() == ReconcileJobKind.PLAN_TABLE
            ? (planTableChanged ? 1L : 0L)
            : projected.tablesChanged();
    if (parent.jobKind() == ReconcileJobKind.PLAN_CONNECTOR) {
      tablesScanned = Math.max(tablesScanned, 1L);
    }
    String childState = effectiveProjectedState(childRecord, projected);
    long childFinishedAtMs = logicalFinishedAtMs(childRecord, projected.finishedAtMs());
    return new ChildContribution(
        tablesScanned,
        tablesChanged,
        projected.viewsScanned(),
        projected.viewsChanged(),
        projected.errors(),
        projected.snapshotsProcessed(),
        projected.statsProcessed(),
        projection.indexesProcessed(),
        projection.plannedFileGroups(),
        projection.plannedFiles(),
        projection.completedFileGroups(),
        projection.failedFileGroups(),
        projection.completedFiles(),
        projection.failedFiles(),
        childRecord.startedAtMs,
        isTerminalState(childState) ? childFinishedAtMs : 0L,
        1L,
        "JS_QUEUED".equals(childState) ? 1L : 0L,
        "JS_WAITING".equals(childState) ? 1L : 0L,
        "JS_RUNNING".equals(childState) ? 1L : 0L,
        "JS_CANCELLING".equals(childState) ? 1L : 0L,
        "JS_SUCCEEDED".equals(childState) ? 1L : 0L,
        "JS_FAILED".equals(childState) ? 1L : 0L,
        "JS_CANCELLED".equals(childState) ? 1L : 0L);
  }

  private String effectiveProjectedState(
      StoredReconcileJob childRecord, ProjectedPublicJob projected) {
    String projectedState = blankToEmpty(projected == null ? "" : projected.state());
    if ("JS_WAITING".equals(projectedState)
        && isLogicalSucceededParent(childRecord, projected == null ? "" : projected.message())) {
      return "JS_SUCCEEDED";
    }
    return projectedState;
  }

  private boolean isLogicalSucceededParent(StoredReconcileJob record, String message) {
    if (record == null) {
      return false;
    }
    long expectedChildJobs = Math.max(0L, record.expectedChildJobs);
    long completedChildJobs = Math.max(0L, record.completedChildJobs);
    DirectChildCounts directChildCounts =
        new DirectChildCounts(
            completedChildJobs,
            Math.max(0L, record.failedChildJobs),
            Math.max(0L, record.cancelledChildJobs),
            expectedChildJobs);
    boolean childCompletionSatisfied =
        (expectedChildJobs > 0L && directChildJobsComplete(expectedChildJobs, directChildCounts))
            || (expectedChildJobs <= 0L && completedChildJobs > 0L);
    return childCompletionSatisfied
        && Math.max(0L, record.failedChildJobs) == 0L
        && Math.max(0L, record.cancelledChildJobs) == 0L
        && Math.max(0L, record.queuedChildJobs) == 0L
        && Math.max(0L, record.waitingChildJobs) == 0L
        && Math.max(0L, record.runningChildJobs) == 0L
        && Math.max(0L, record.cancellingChildJobs) == 0L
        && "Succeeded"
            .equals(ReconcileJobProjector.normalizeSucceededMessage(blankToEmpty(message)));
  }

  private long logicalFinishedAtMs(StoredReconcileJob record, long projectedFinishedAtMs) {
    if (record == null) {
      return Math.max(0L, projectedFinishedAtMs);
    }
    return Math.max(
        Math.max(0L, projectedFinishedAtMs),
        Math.max(
            Math.max(0L, record.finishedAtMs), Math.max(0L, record.maxDirectChildFinishedAtMs)));
  }

  private static boolean planTableContributionScanned(
      ProjectedPublicJob projected, JobProjection projection) {
    return projected.snapshotsProcessed() > 0L
        || projected.statsProcessed() > 0L
        || projection.indexesProcessed() > 0L
        || projected.errors() > 0L
        || projection.completedFileGroups() > 0L
        || projection.failedFileGroups() > 0L
        || projection.completedFiles() > 0L
        || projection.failedFiles() > 0L;
  }

  private static boolean planTableContributionChanged(
      ProjectedPublicJob projected, JobProjection projection) {
    return projected.snapshotsProcessed() > 0L
        || projected.statsProcessed() > 0L
        || projection.indexesProcessed() > 0L
        || projection.completedFileGroups() > 0L
        || projection.completedFiles() > 0L;
  }

  private record ChildContribution(
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      long indexesProcessed,
      long plannedFileGroups,
      long plannedFiles,
      long completedFileGroups,
      long failedFileGroups,
      long completedFiles,
      long failedFiles,
      long startedAtMs,
      long finishedAtMs,
      long directChildObserved,
      long queuedChildJobs,
      long waitingChildJobs,
      long runningChildJobs,
      long cancellingChildJobs,
      long completedChildJobs,
      long failedChildJobs,
      long cancelledChildJobs) {
    public static ChildContribution empty() {
      return new ChildContribution(
          0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
          0L, 0L);
    }
  }

  private record ChildView(StoredReconcileJob stored, ProjectedPublicJob projected) {}
}
