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
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.DirectChildCounts;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.JobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.ProjectedPublicJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ReconcileAncestorRollupService {
  @FunctionalInterface
  public interface ReconcileLeaseLiveness {
    boolean hasLiveLease(StoredReconcileJob record, boolean tolerateLeasePointerDrift, long nowMs);
  }

  private ReconcileJobIndexStore jobIndexStore;
  private ReconcileJobProjector projector;
  private ReconcileLeaseLiveness leaseLiveness;

  public void bind(
      ReconcileJobIndexStore jobIndexStore,
      ReconcileJobProjector projector,
      ReconcileLeaseLiveness leaseLiveness) {
    this.jobIndexStore = jobIndexStore;
    this.projector = projector;
    this.leaseLiveness = leaseLiveness;
  }

  public StoredReconcileJobProjection recomputeParentProjection(
      StoredReconcileJob parent, List<StoredReconcileJob> directChildren) {
    return recomputeParentProjection(parent, directChildren, false);
  }

  public StoredReconcileJobProjection recomputeParentProjection(
      StoredReconcileJob parent,
      List<StoredReconcileJob> directChildren,
      boolean ignoreParentLeaseLiveness) {
    if (parent == null || !projector.isParentCapable(parent.jobKind())) {
      return null;
    }
    JobProjection intrinsicProjection = projector.intrinsicProjectionForRollup(parent);
    long selfTablesScanned = Math.max(0L, parent.tablesScanned);
    long selfTablesChanged = Math.max(0L, parent.tablesChanged);
    long selfViewsScanned = Math.max(0L, parent.viewsScanned);
    long selfViewsChanged = Math.max(0L, parent.viewsChanged);
    long selfErrors = Math.max(0L, parent.errors);
    long selfSnapshotsProcessed = Math.max(0L, parent.snapshotsProcessed);
    long selfStatsProcessed = Math.max(0L, parent.statsProcessed);
    long selfIndexesProcessed = Math.max(0L, parent.indexesProcessed);
    long canonicalSnapshotsProcessed = selfSnapshotsProcessed;
    long canonicalStatsProcessed = selfStatsProcessed;
    long canonicalIndexesProcessed = selfIndexesProcessed;
    long selfPlannedFileGroups = Math.max(0L, parent.plannedFileGroups);
    long selfPlannedFiles = Math.max(0L, parent.plannedFiles);
    long selfCompletedFileGroups = Math.max(0L, parent.completedFileGroups);
    long selfFailedFileGroups = Math.max(0L, parent.failedFileGroups);
    long selfCompletedFiles = Math.max(0L, parent.completedFiles);
    long selfFailedFiles = Math.max(0L, parent.failedFiles);
    List<StoredReconcileJob> effectiveDirectChildren =
        dedupeDirectChildrenForRollup(parent, directChildren);
    boolean hasDirectChildren = !effectiveDirectChildren.isEmpty();
    if (parent.jobKind() == ReconcileJobKind.PLAN_CONNECTOR) {
      selfViewsScanned = 0L;
      selfViewsChanged = 0L;
      selfErrors = 0L;
    }
    if (parent.jobKind() == ReconcileJobKind.PLAN_CONNECTOR || hasDirectChildren) {
      selfErrors = 0L;
      selfSnapshotsProcessed = 0L;
      selfStatsProcessed = 0L;
      selfIndexesProcessed = 0L;
      selfPlannedFileGroups = 0L;
      selfPlannedFiles = 0L;
      selfCompletedFileGroups = 0L;
      selfFailedFileGroups = 0L;
      selfCompletedFiles = 0L;
      selfFailedFiles = 0L;
    }
    ChildAggregate aggregate = ChildAggregate.empty();
    StoredReconcileJob representativeChild = null;

    long startedAtMs = Math.max(0L, parent.startedAtMs);
    for (StoredReconcileJob child : effectiveDirectChildren) {
      if (child == null) {
        continue;
      }
      ChildContribution contribution = contributionForParent(parent, child);
      aggregate = aggregate.add(contribution);
      if (contribution.startedAtMs() > 0L) {
        startedAtMs = earliestPositive(startedAtMs, contribution.startedAtMs());
      }
      representativeChild = chooseRepresentativeChild(representativeChild, child);
    }

    long tablesScanned = selfTablesScanned + aggregate.tablesScanned();
    long tablesChanged = selfTablesChanged + aggregate.tablesChanged();
    long viewsScanned = selfViewsScanned + aggregate.viewsScanned();
    long viewsChanged = selfViewsChanged + aggregate.viewsChanged();
    long errors = selfErrors + aggregate.errors();
    long snapshotsProcessed = selfSnapshotsProcessed + aggregate.snapshotsProcessed();
    long statsProcessed = selfStatsProcessed + aggregate.statsProcessed();
    long indexesProcessed = selfIndexesProcessed + aggregate.indexesProcessed();
    long plannedFileGroups =
        Math.max(
            Math.max(intrinsicProjection.plannedFileGroups(), selfPlannedFileGroups),
            aggregate.plannedFileGroups());
    long plannedFiles =
        Math.max(
            Math.max(intrinsicProjection.plannedFiles(), selfPlannedFiles),
            aggregate.plannedFiles());
    long completedFileGroups = selfCompletedFileGroups + aggregate.completedFileGroups();
    long failedFileGroups = selfFailedFileGroups + aggregate.failedFileGroups();
    long completedFiles = selfCompletedFiles + aggregate.completedFiles();
    long failedFiles = selfFailedFiles + aggregate.failedFiles();

    if (parent.jobKind() == ReconcileJobKind.PLAN_TABLE) {
      tablesScanned = Math.max(selfTablesScanned, aggregate.tablesScanned());
      tablesChanged = Math.max(selfTablesChanged, aggregate.tablesChanged());
      snapshotsProcessed =
          tableSnapshotsProcessed(
              parent, canonicalSnapshotsProcessed, aggregate.snapshotsProcessed());
      statsProcessed = Math.max(canonicalStatsProcessed, aggregate.statsProcessed());
      indexesProcessed = Math.max(canonicalIndexesProcessed, aggregate.indexesProcessed());
    }
    if (parent.jobKind() == ReconcileJobKind.PLAN_CONNECTOR) {
      tablesScanned =
          Math.max(
              Math.max(selfTablesScanned, Math.max(0L, aggregate.tablesScanned())),
              Math.max(0L, aggregate.directChildObserved()));
      tablesChanged = Math.max(selfTablesChanged, aggregate.tablesChanged());
    }

    ProjectedParentState parentState =
        projectedParentState(
            parent, representativeChild, aggregate, startedAtMs, ignoreParentLeaseLiveness);
    return new StoredReconcileJobProjection(
        blankToEmpty(parent.accountId),
        blankToEmpty(parent.jobId),
        Math.max(0L, parent.projectionRequestedGeneration),
        parentState.state(),
        parentState.message(),
        parentState.startedAtMs(),
        parentState.finishedAtMs(),
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed,
        indexesProcessed,
        plannedFileGroups,
        plannedFiles,
        completedFileGroups,
        failedFileGroups,
        completedFiles,
        failedFiles,
        parentState.executorId(),
        true);
  }

  private StoredReconcileJob chooseRepresentativeChild(
      StoredReconcileJob current, StoredReconcileJob candidate) {
    if (candidate == null) {
      return current;
    }
    if (current == null) {
      return candidate;
    }
    int currentRank = childStatePriority(current);
    int candidateRank = childStatePriority(candidate);
    if (candidateRank > currentRank) {
      return candidate;
    }
    if (candidateRank < currentRank) {
      return current;
    }
    return candidate.updatedAtMs >= current.updatedAtMs ? candidate : current;
  }

  private List<StoredReconcileJob> dedupeDirectChildrenForRollup(
      StoredReconcileJob parent, List<StoredReconcileJob> directChildren) {
    if (parent == null
        || parent.jobKind() != ReconcileJobKind.PLAN_CONNECTOR
        || directChildren == null
        || directChildren.isEmpty()) {
      return directChildren == null ? List.of() : directChildren;
    }
    Map<String, StoredReconcileJob> byLogicalTarget = new LinkedHashMap<>();
    for (StoredReconcileJob child : directChildren) {
      if (child == null) {
        continue;
      }
      String logicalTarget = connectorDirectChildLogicalTarget(child);
      StoredReconcileJob current = byLogicalTarget.get(logicalTarget);
      byLogicalTarget.put(logicalTarget, chooseRepresentativeChild(current, child));
    }
    return List.copyOf(byLogicalTarget.values());
  }

  private String connectorDirectChildLogicalTarget(StoredReconcileJob child) {
    if (child == null) {
      return "";
    }
    if (child.jobKind() == ReconcileJobKind.PLAN_TABLE && child.definition != null) {
      String destinationTableId = blankToEmpty(child.definition.taskDestinationTableId);
      if (!destinationTableId.isBlank()) {
        return "table|" + destinationTableId;
      }
      String sourceNamespace = blankToEmpty(child.definition.sourceNamespace);
      String sourceTable = blankToEmpty(child.definition.sourceTable);
      if (!sourceNamespace.isBlank() || !sourceTable.isBlank()) {
        return "table-source|" + sourceNamespace + "|" + sourceTable;
      }
    }
    return "job|" + blankToEmpty(child.jobId);
  }

  private int childStatePriority(StoredReconcileJob child) {
    ProjectedPublicJob projected = projector.projectSelfPublicJobForRollup(child);
    return switch (blankToEmpty(projected.state())) {
      case "JS_CANCELLING" -> 7;
      case "JS_RUNNING" -> 6;
      case "JS_FAILED" -> 5;
      case "JS_CANCELLED" -> 4;
      case "JS_WAITING" -> 3;
      case "JS_QUEUED" -> 2;
      case "JS_SUCCEEDED" -> 1;
      default -> 0;
    };
  }

  private ProjectedParentState projectedParentState(
      StoredReconcileJob parent,
      StoredReconcileJob representativeChild,
      ChildAggregate aggregate,
      long startedAtMs,
      boolean ignoreParentLeaseLiveness) {
    DirectChildCounts directChildCounts =
        new DirectChildCounts(
            aggregate.completedChildJobs(),
            aggregate.failedChildJobs(),
            aggregate.cancelledChildJobs(),
            aggregate.directChildObserved());
    long expectedDirectChildJobs =
        Math.max(
            Math.max(0L, parent == null ? 0L : parent.expectedDirectChildren),
            Math.max(0L, aggregate.directChildObserved()));
    boolean childSetFinalized = childSetFinalized(parent);
    boolean allSucceeded =
        childSetFinalized
            && expectedDirectChildJobs > 0L
            && aggregate.completedChildJobs() >= expectedDirectChildJobs
            && aggregate.failedChildJobs() == 0L
            && aggregate.cancelledChildJobs() == 0L
            && aggregate.queuedChildJobs() == 0L
            && aggregate.waitingChildJobs() == 0L
            && aggregate.runningChildJobs() == 0L
            && aggregate.cancellingChildJobs() == 0L;
    ProjectedPublicJob currentProjected =
        representativeChild == null
            ? ProjectedPublicJob.empty()
            : projector.projectSelfPublicJobForRollup(representativeChild);
    String currentChildState = blankToEmpty(currentProjected.state());
    String currentChildMessage = blankToEmpty(currentProjected.message());
    String currentChildExecutorId = blankToEmpty(currentProjected.executorId());
    boolean leaseOwnsCanonicalState =
        !ignoreParentLeaseLiveness
            && leaseLiveness.hasLiveLease(parent, true, System.currentTimeMillis());
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
      } else if ("JS_CANCELLING".equals(parent.state)) {
        if (cancellingParentComplete(
            childSetFinalized, expectedDirectChildJobs, directChildCounts, aggregate)) {
          state = "JS_CANCELLED";
          message = firstNonBlank(parent.message, "Cancelled");
          executorId = blankToEmpty(parent.executorId);
        } else {
          state = "JS_CANCELLING";
          message = firstNonBlank(parent.message, "Cancelling");
          executorId = blankToEmpty(parent.executorId);
        }
      } else if ("JS_WAITING".equals(parent.state)
          && (aggregate.queuedChildJobs() > 0L
              || aggregate.waitingChildJobs() > 0L
              || aggregate.runningChildJobs() > 0L)) {
        state = "JS_WAITING";
        message =
            ReconcileJobProjector.normalizeWaitingStateMessage(
                "JS_WAITING",
                firstNonBlank(parent.message, currentChildMessage, "Waiting on child work"));
        executorId = "";
      } else if (aggregate.cancellingChildJobs() > 0L) {
        state = "JS_CANCELLING";
        message = firstNonBlank(currentChildMessage, message, "Cancelling");
        executorId = firstNonBlank(currentChildExecutorId, executorId);
      } else if (aggregate.runningChildJobs() > 0L) {
        state = "JS_RUNNING";
        message = firstNonBlank(currentChildMessage, message, "Running");
        executorId = firstNonBlank(currentChildExecutorId, executorId);
      } else if (aggregate.failedChildJobs() > 0L) {
        state = "JS_FAILED";
        message =
            "JS_FAILED".equals(currentChildState)
                ? firstNonBlank(currentChildMessage, message, "Failed")
                : firstNonBlank(message, "Failed");
        executorId =
            "JS_FAILED".equals(currentChildState)
                ? firstNonBlank(currentChildExecutorId, executorId)
                : executorId;
      } else if (aggregate.cancelledChildJobs() > 0L) {
        state = "JS_CANCELLED";
        message =
            "JS_CANCELLED".equals(currentChildState)
                ? firstNonBlank(currentChildMessage, message, "Cancelled")
                : firstNonBlank(message, "Cancelled");
        executorId =
            "JS_CANCELLED".equals(currentChildState)
                ? firstNonBlank(currentChildExecutorId, executorId)
                : executorId;
      } else if (aggregate.waitingChildJobs() > 0L) {
        state = "JS_WAITING";
        message =
            ReconcileJobProjector.normalizeWaitingStateMessage(
                "JS_WAITING", firstNonBlank(currentChildMessage, message, "Waiting on child work"));
        executorId = "";
      } else if ("JS_WAITING".equals(parent.state)
          && !directChildJobsComplete(expectedDirectChildJobs, directChildCounts)) {
        state = "JS_WAITING";
        message =
            ReconcileJobProjector.normalizeWaitingStateMessage(
                "JS_WAITING", firstNonBlank(parent.message, "Waiting on child work"));
        executorId = "";
      } else if (childSetFinalized
          && expectedDirectChildJobs > directChildCounts.totalTerminal()
          && aggregate.queuedChildJobs() == 0L
          && aggregate.waitingChildJobs() == 0L
          && aggregate.runningChildJobs() == 0L
          && aggregate.cancellingChildJobs() == 0L) {
        state = "JS_WAITING";
        message =
            ReconcileJobProjector.normalizeWaitingStateMessage(
                "JS_WAITING", firstNonBlank(parent.message, "Waiting on child work"));
        executorId = "";
      } else if (aggregate.queuedChildJobs() > 0L
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
          && directChildJobsComplete(expectedDirectChildJobs, directChildCounts)) {
        state = "JS_SUCCEEDED";
        message = ReconcileJobProjector.normalizeSucceededMessage(message);
      } else if (allSucceeded && expectedDirectChildJobs > 0L) {
        state = "JS_WAITING";
        message = "Waiting on child work";
        executorId = "";
      } else if ("JS_RUNNING".equals(state) && aggregate.directChildObserved() > 0L) {
        state = "JS_WAITING";
        message = "Waiting on child work";
        executorId = "";
      }
    }
    long finishedAtMs =
        leaseOwnsCanonicalState
            ? parent.finishedAtMs
            : (isTerminalState(state) ? maxTerminalFinishedAtMs(parent, aggregate, state) : 0L);
    if (isTerminalState(parent.state) && !blank(parent.executorId)) {
      executorId = parent.executorId;
    }
    return new ProjectedParentState(
        blankToEmpty(state),
        blankToEmpty(message),
        Math.max(0L, startedAtMs),
        Math.max(0L, finishedAtMs),
        blankToEmpty(executorId));
  }

  private static boolean directChildJobsComplete(
      long expectedChildJobs, DirectChildCounts directChildCounts) {
    long expected = Math.max(0L, expectedChildJobs);
    return expected <= 0L || directChildCounts.totalTerminal() >= expected;
  }

  private static boolean cancellingParentComplete(
      boolean childSetFinalized,
      long expectedDirectChildJobs,
      DirectChildCounts directChildCounts,
      ChildAggregate aggregate) {
    return childSetFinalized
        && Math.max(0L, expectedDirectChildJobs) > 0L
        && directChildJobsComplete(expectedDirectChildJobs, directChildCounts)
        && aggregate.queuedChildJobs() == 0L
        && aggregate.waitingChildJobs() == 0L
        && aggregate.runningChildJobs() == 0L
        && aggregate.cancellingChildJobs() == 0L;
  }

  private static boolean childSetFinalized(StoredReconcileJob parent) {
    return parent != null && parent.childrenFinalized;
  }

  private static long tableSnapshotsProcessed(
      StoredReconcileJob parent, long canonicalSnapshotsProcessed, long childSnapshotsProcessed) {
    if (parent == null || parent.jobKind() != ReconcileJobKind.PLAN_TABLE) {
      return Math.max(0L, Math.max(canonicalSnapshotsProcessed, childSnapshotsProcessed));
    }
    long expectedSnapshots = Math.max(0L, parent.expectedDirectChildren);
    if (expectedSnapshots > 0L) {
      return expectedSnapshots;
    }
    if (childSnapshotsProcessed > 0L) {
      return Math.max(0L, childSnapshotsProcessed);
    }
    return Math.max(0L, Math.max(canonicalSnapshotsProcessed, childSnapshotsProcessed));
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

  private static long earliestPositive(long current, long candidate) {
    long left = Math.max(0L, current);
    long right = Math.max(0L, candidate);
    if (left <= 0L) {
      return right;
    }
    if (right <= 0L) {
      return left;
    }
    return Math.min(left, right);
  }

  private static long maxTerminalFinishedAtMs(
      StoredReconcileJob parent, ChildAggregate aggregate, String projectedState) {
    long result = isTerminalState(parent.state) ? Math.max(0L, parent.finishedAtMs) : 0L;
    if (aggregate != null
        && aggregate.maxDirectChildFinishedAtMs() > 0L
        && isTerminalState(projectedState)) {
      result = Math.max(result, aggregate.maxDirectChildFinishedAtMs());
    }
    return result;
  }

  private ChildContribution contributionForParent(
      StoredReconcileJob parent, StoredReconcileJob childRecord) {
    if (parent == null || childRecord == null) {
      return ChildContribution.empty();
    }
    ProjectedPublicJob projected = projector.projectSelfPublicJobForRollup(childRecord);
    JobProjection projection = projected.projection();
    boolean planTableScanned = planTableContributionScanned(projected, projection);
    boolean planTableChanged = planTableContributionChanged(projected, projection);
    long tablesScanned;
    long tablesChanged;
    if (parent.jobKind() == ReconcileJobKind.PLAN_TABLE) {
      tablesScanned = planTableScanned ? 1L : 0L;
      tablesChanged = planTableChanged ? 1L : 0L;
    } else if (parent.jobKind() == ReconcileJobKind.PLAN_CONNECTOR
        && childRecord.jobKind() == ReconcileJobKind.PLAN_TABLE) {
      tablesScanned = 1L;
      tablesChanged = connectorTableContributionChanged(projected, projection) ? 1L : 0L;
    } else {
      tablesScanned = projected.tablesScanned();
      tablesChanged = projected.tablesChanged();
    }
    if (parent.jobKind() == ReconcileJobKind.PLAN_CONNECTOR) {
      tablesScanned = Math.max(tablesScanned, 1L);
    }
    String childState = blankToEmpty(projected.state());
    long childFinishedAtMs = logicalFinishedAtMs(childRecord, projected.finishedAtMs());
    return new ChildContribution(
        tablesScanned,
        tablesChanged,
        projected.viewsScanned(),
        projected.viewsChanged(),
        projected.errors(),
        snapshotsProcessedContribution(parent, childRecord, projected),
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

  private long logicalFinishedAtMs(StoredReconcileJob record, long projectedFinishedAtMs) {
    if (record == null) {
      return Math.max(0L, projectedFinishedAtMs);
    }
    return Math.max(Math.max(0L, projectedFinishedAtMs), Math.max(0L, record.finishedAtMs));
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

  private static boolean connectorTableContributionChanged(
      ProjectedPublicJob projected, JobProjection projection) {
    return projected.tablesChanged() > 0L
        || projected.snapshotsProcessed() > 0L
        || projected.statsProcessed() > 0L
        || projection.indexesProcessed() > 0L
        || projection.completedFileGroups() > 0L
        || projection.completedFiles() > 0L;
  }

  private static long snapshotsProcessedContribution(
      StoredReconcileJob parent, StoredReconcileJob childRecord, ProjectedPublicJob projected) {
    if (parent != null
        && parent.jobKind() == ReconcileJobKind.PLAN_TABLE
        && childRecord != null
        && childRecord.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT) {
      return 1L;
    }
    return projected.snapshotsProcessed();
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

  private record ChildAggregate(
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
      long directChildObserved,
      long queuedChildJobs,
      long waitingChildJobs,
      long runningChildJobs,
      long cancellingChildJobs,
      long completedChildJobs,
      long failedChildJobs,
      long cancelledChildJobs,
      long maxDirectChildFinishedAtMs) {
    public static ChildAggregate empty() {
      return new ChildAggregate(
          0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
          0L);
    }

    public ChildAggregate add(ChildContribution contribution) {
      if (contribution == null) {
        return this;
      }
      return new ChildAggregate(
          tablesScanned + contribution.tablesScanned(),
          tablesChanged + contribution.tablesChanged(),
          viewsScanned + contribution.viewsScanned(),
          viewsChanged + contribution.viewsChanged(),
          errors + contribution.errors(),
          snapshotsProcessed + contribution.snapshotsProcessed(),
          statsProcessed + contribution.statsProcessed(),
          indexesProcessed + contribution.indexesProcessed(),
          plannedFileGroups + contribution.plannedFileGroups(),
          plannedFiles + contribution.plannedFiles(),
          completedFileGroups + contribution.completedFileGroups(),
          failedFileGroups + contribution.failedFileGroups(),
          completedFiles + contribution.completedFiles(),
          failedFiles + contribution.failedFiles(),
          directChildObserved + contribution.directChildObserved(),
          queuedChildJobs + contribution.queuedChildJobs(),
          waitingChildJobs + contribution.waitingChildJobs(),
          runningChildJobs + contribution.runningChildJobs(),
          cancellingChildJobs + contribution.cancellingChildJobs(),
          completedChildJobs + contribution.completedChildJobs(),
          failedChildJobs + contribution.failedChildJobs(),
          cancelledChildJobs + contribution.cancelledChildJobs(),
          Math.max(maxDirectChildFinishedAtMs, contribution.finishedAtMs()));
    }
  }

  private record ProjectedParentState(
      String state, String message, long startedAtMs, long finishedAtMs, String executorId) {}
}
