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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobContribution;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.DirectChildCounts;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.JobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.ProjectedPublicJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileLeaseManager;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileContributionRollupService {
  private static final Logger LOG = Logger.getLogger(ReconcileContributionRollupService.class);

  private PointerStore pointerStore;
  private ReconcilePayloadStore payloadStore;
  private ReconcileJobProjector projector;
  private ReconcileLeaseManager leaseManager;
  private Function<String, Optional<CanonicalEnvelope>> loadByAnyAccount;
  private Function<Pointer, Optional<StoredReconcileJob>> readCurrentRecordFromIndexPointer;
  private BiFunction<String, String, Boolean> upsertReferencePointer;
  private CanonicalMutator mutateByCanonicalPointerReturningRecord;
  private Function<StoredReconcileJob, StoredReconcileJob> copyStoredJob;

  public void bind(
      PointerStore pointerStore,
      ReconcilePayloadStore payloadStore,
      ReconcileJobProjector projector,
      ReconcileLeaseManager leaseManager,
      Function<String, Optional<CanonicalEnvelope>> loadByAnyAccount,
      Function<Pointer, Optional<StoredReconcileJob>> readCurrentRecordFromIndexPointer,
      BiFunction<String, String, Boolean> upsertReferencePointer,
      CanonicalMutator mutateByCanonicalPointerReturningRecord,
      Function<StoredReconcileJob, StoredReconcileJob> copyStoredJob) {
    this.pointerStore = pointerStore;
    this.payloadStore = payloadStore;
    this.projector = projector;
    this.leaseManager = leaseManager;
    this.loadByAnyAccount = loadByAnyAccount;
    this.readCurrentRecordFromIndexPointer = readCurrentRecordFromIndexPointer;
    this.upsertReferencePointer = upsertReferencePointer;
    this.mutateByCanonicalPointerReturningRecord = mutateByCanonicalPointerReturningRecord;
    this.copyStoredJob = copyStoredJob;
  }

  public ProjectedPublicJob projectPublicJob(
      StoredReconcileJob stored,
      boolean includeSelfProjectionPayloads,
      boolean allowContributionRepair) {
    if (stored == null) {
      return ProjectedPublicJob.empty();
    }
    JobProjection selfProjection = projector.inlineSummaryProjection(stored);
    if (!projector.isParentCapable(stored.jobKind())) {
      return projector.projectSelfPublicJob(stored, includeSelfProjectionPayloads);
    }

    List<StoredJobContribution> contributions =
        allowContributionRepair
            ? loadDirectContributionsWithRepair(stored.accountId, stored.jobId)
            : loadDirectContributionsNoRepair(stored.accountId, stored.jobId);
    if (contributions.isEmpty()) {
      return ProjectedPublicJob.self(stored, selfProjection);
    }

    return projectParentPublicJob(stored, selfProjection, contributions);
  }

  public List<StoredJobContribution> loadDirectContributionsNoRepair(
      String accountId, String parentJobId) {
    return loadDirectContributions(accountId, parentJobId, false);
  }

  public List<StoredJobContribution> loadDirectContributionsWithRepair(
      String accountId, String parentJobId) {
    return loadDirectContributions(accountId, parentJobId, true);
  }

  public void refreshContributionChain(StoredReconcileJob record) {
    StoredReconcileJob current = record;
    while (current != null && !blank(current.parentJobId)) {
      upsertContributionForParent(current, true);
      current =
          loadByAnyAccount.apply(current.parentJobId).map(CanonicalEnvelope::record).orElse(null);
    }
  }

  public void refreshAncestorContributionRollups(
      StoredReconcileJob childJob, boolean includeSelfProjectionPayloads) {
    if (childJob == null || blank(childJob.parentJobId)) {
      return;
    }
    StoredReconcileJob currentChild = childJob;
    boolean includeCurrentPayloads = includeSelfProjectionPayloads;
    while (currentChild != null && !blank(currentChild.parentJobId)) {
      upsertContributionForParent(currentChild, includeCurrentPayloads);
      CanonicalEnvelope parentEnvelope =
          loadByAnyAccount.apply(currentChild.parentJobId).orElse(null);
      if (parentEnvelope == null || parentEnvelope.record() == null) {
        return;
      }
      currentChild = refreshCanonicalCountersFromContributions(parentEnvelope);
      includeCurrentPayloads = false;
    }
  }

  private List<StoredJobContribution> loadDirectContributions(
      String accountId, String parentJobId, boolean allowRepair) {
    if (blank(accountId) || blank(parentJobId)) {
      return List.of();
    }
    String prefix = Keys.reconcileJobContributionPointerPrefix(accountId, parentJobId);
    List<StoredJobContribution> out = new ArrayList<>();
    String token = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 256, token, next);
      if (pointers.isEmpty()) {
        break;
      }
      for (Pointer pointer : pointers) {
        readContribution(pointer)
            .filter(
                contribution ->
                    accountId.equals(contribution.accountId)
                        && parentJobId.equals(contribution.parentJobId))
            .ifPresent(out::add);
      }
      token = next.toString();
      if (token.isBlank()) {
        break;
      }
    }
    if (out.isEmpty() && allowRepair && hasDirectChildren(accountId, parentJobId)) {
      rebuildDirectChildContributions(accountId, parentJobId);
      return loadDirectContributions(accountId, parentJobId, false);
    }
    return out;
  }

  private Optional<StoredJobContribution> readContribution(Pointer pointer) {
    if (pointer == null || blank(pointer.getBlobUri())) {
      return Optional.empty();
    }
    return payloadStore.readInlineJobContribution(pointer.getBlobUri());
  }

  private boolean hasDirectChildren(String accountId, String parentJobId) {
    StringBuilder next = new StringBuilder();
    return !pointerStore
        .listPointersByPrefix(
            Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId), 1, "", next)
        .isEmpty();
  }

  private void rebuildDirectChildContributions(String accountId, String parentJobId) {
    if (blank(accountId) || blank(parentJobId)) {
      return;
    }
    String prefix = Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId);
    String token = "";
    Set<String> activeChildIds = new java.util.HashSet<>();
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> childPointers = pointerStore.listPointersByPrefix(prefix, 256, token, next);
      if (childPointers.isEmpty()) {
        break;
      }
      for (Pointer childPointer : childPointers) {
        var child = readCurrentRecordFromIndexPointer.apply(childPointer);
        if (child.isEmpty()) {
          continue;
        }
        activeChildIds.add(child.get().jobId);
        upsertContributionForParent(child.get());
      }
      token = next.toString();
      if (token.isBlank()) {
        break;
      }
    }
    cleanupStaleContributionPointers(accountId, parentJobId, activeChildIds);
  }

  private void cleanupStaleContributionPointers(
      String accountId, String parentJobId, Set<String> activeChildIds) {
    String prefix = Keys.reconcileJobContributionPointerPrefix(accountId, parentJobId);
    String token = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 256, token, next);
      if (pointers.isEmpty()) {
        return;
      }
      for (Pointer pointer : pointers) {
        StoredJobContribution contribution = readContribution(pointer).orElse(null);
        if (contribution == null || !activeChildIds.contains(contribution.childJobId)) {
          pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion());
        }
      }
      token = next.toString();
      if (token.isBlank()) {
        return;
      }
    }
  }

  private StoredReconcileJob refreshCanonicalCountersFromContributions(
      CanonicalEnvelope parentEnvelope) {
    if (parentEnvelope == null
        || parentEnvelope.record() == null
        || !projector.isParentCapable(parentEnvelope.record().jobKind())) {
      return parentEnvelope == null ? null : parentEnvelope.record();
    }
    List<StoredJobContribution> contributions =
        loadDirectContributionsNoRepair(
            parentEnvelope.record().accountId, parentEnvelope.record().jobId);
    DirectChildCounts directChildCounts = projector.countDirectChildStates(contributions);
    ProjectedPublicJob projected = projectPublicJob(parentEnvelope.record(), false, false);
    StoredReconcileJob priorParent = copyStoredJob.apply(parentEnvelope.record());
    Optional<CanonicalEnvelope> updated =
        mutateByCanonicalPointerReturningRecord.apply(
            parentEnvelope.canonicalPointerKey(),
            existing -> {
              if (!projector.isParentCapable(existing.jobKind())) {
                return existing;
              }
              boolean leaseOwnsCanonicalState =
                  leaseManager.hasLiveLease(existing, true, System.currentTimeMillis());
              String nextState =
                  canonicalParentStateFromProjection(existing, projected, directChildCounts);
              String nextMessage =
                  leaseOwnsCanonicalState
                      ? existing.message
                      : canonicalParentMessageFromProjection(existing, projected, nextState);
              String nextExecutorId =
                  leaseOwnsCanonicalState
                      ? existing.executorId
                      : canonicalParentExecutorFromProjection(existing, projected, nextState);
              long nextFinishedAtMs =
                  leaseOwnsCanonicalState
                      ? existing.finishedAtMs
                      : (isTerminalState(nextState)
                          ? Math.max(projected.finishedAtMs(), existing.finishedAtMs)
                          : 0L);
              if (existing.tablesScanned == projected.tablesScanned()
                  && existing.tablesChanged == projected.tablesChanged()
                  && existing.viewsScanned == projected.viewsScanned()
                  && existing.viewsChanged == projected.viewsChanged()
                  && existing.errors == projected.errors()
                  && existing.snapshotsProcessed == projected.snapshotsProcessed()
                  && existing.statsProcessed == projected.statsProcessed()
                  && existing.indexesProcessed == projected.projection().indexesProcessed()
                  && existing.plannedFileGroups == projected.projection().plannedFileGroups()
                  && existing.plannedFiles == projected.projection().plannedFiles()
                  && existing.completedFileGroups == projected.projection().completedFileGroups()
                  && existing.failedFileGroups == projected.projection().failedFileGroups()
                  && existing.completedFiles == projected.projection().completedFiles()
                  && existing.failedFiles == projected.projection().failedFiles()
                  && existing.completedChildJobs == directChildCounts.completed()
                  && existing.failedChildJobs == directChildCounts.failed()
                  && existing.cancelledChildJobs == directChildCounts.cancelled()
                  && blankToEmpty(existing.state)
                      .equals(leaseOwnsCanonicalState ? blankToEmpty(existing.state) : nextState)
                  && blankToEmpty(existing.message).equals(nextMessage)
                  && blankToEmpty(existing.executorId).equals(nextExecutorId)
                  && existing.finishedAtMs == nextFinishedAtMs) {
                return null;
              }
              if (!leaseOwnsCanonicalState) {
                existing.state = nextState;
              }
              existing.message = nextMessage;
              existing.executorId = nextExecutorId;
              existing.finishedAtMs = nextFinishedAtMs;
              if (!leaseOwnsCanonicalState && isTerminalState(nextState)) {
                existing.readyPointerKey = null;
              }
              existing.tablesScanned = projected.tablesScanned();
              existing.tablesChanged = projected.tablesChanged();
              existing.viewsScanned = projected.viewsScanned();
              existing.viewsChanged = projected.viewsChanged();
              existing.errors = projected.errors();
              existing.snapshotsProcessed = projected.snapshotsProcessed();
              existing.statsProcessed = projected.statsProcessed();
              existing.indexesProcessed = projected.projection().indexesProcessed();
              existing.plannedFileGroups = projected.projection().plannedFileGroups();
              existing.plannedFiles = projected.projection().plannedFiles();
              existing.completedFileGroups = projected.projection().completedFileGroups();
              existing.failedFileGroups = projected.projection().failedFileGroups();
              existing.completedFiles = projected.projection().completedFiles();
              existing.failedFiles = projected.projection().failedFiles();
              existing.completedChildJobs = directChildCounts.completed();
              existing.failedChildJobs = directChildCounts.failed();
              existing.cancelledChildJobs = directChildCounts.cancelled();
              return existing;
            });
    if (updated.isPresent()) {
      logAncestorRollupUpdate(
          priorParent, updated.get().record(), projected, directChildCounts, contributions.size());
    } else {
      logAncestorRollupNoop(priorParent, projected, directChildCounts, contributions.size());
    }
    return updated.map(CanonicalEnvelope::record).orElse(parentEnvelope.record());
  }

  private void upsertContributionForParent(StoredReconcileJob childJob) {
    upsertContributionForParent(childJob, true);
  }

  private void upsertContributionForParent(
      StoredReconcileJob childJob, boolean includeSelfProjectionPayloads) {
    if (childJob == null || blank(childJob.parentJobId)) {
      return;
    }
    StoredJobContribution contribution =
        contributionSnapshot(
            childJob.accountId, childJob.parentJobId, childJob, includeSelfProjectionPayloads);
    upsertReferencePointer.apply(
        Keys.reconcileJobContributionPointer(
            contribution.accountId, contribution.parentJobId, contribution.childJobId),
        payloadStore.encodeInlineJobContribution(contribution));
    LOG.debugf(
        "reconcile contribution upsert parentJobId=%s childJobId=%s childKind=%s childState=%s projection=%s includeSelfProjectionPayloads=%s",
        contribution.parentJobId,
        contribution.childJobId,
        childJob.jobKind(),
        blankToEmpty(contribution.state),
        formatContributionProjection(contribution),
        includeSelfProjectionPayloads);
  }

  private StoredJobContribution contributionSnapshot(
      String parentAccountId,
      String parentJobId,
      StoredReconcileJob childJob,
      boolean includeSelfProjectionPayloads) {
    ProjectedPublicJob projected = projectPublicJob(childJob, includeSelfProjectionPayloads, false);
    return StoredJobContribution.of(
        parentAccountId,
        parentJobId,
        childJob.jobId,
        projected.state(),
        projected.message(),
        projected.startedAtMs(),
        projected.finishedAtMs(),
        projected.tablesScanned(),
        projected.tablesChanged(),
        projected.viewsScanned(),
        projected.viewsChanged(),
        projected.errors(),
        projected.snapshotsProcessed(),
        projected.statsProcessed(),
        projected.projection().indexesProcessed(),
        projected.projection().plannedFileGroups(),
        projected.projection().plannedFiles(),
        projected.projection().completedFileGroups(),
        projected.projection().failedFileGroups(),
        projected.projection().completedFiles(),
        projected.projection().failedFiles(),
        projected.executorId(),
        Math.max(childJob.updatedAtMs, System.currentTimeMillis()));
  }

  public ProjectedPublicJob projectParentPublicJob(
      StoredReconcileJob stored,
      JobProjection selfProjection,
      List<StoredJobContribution> contributions) {

    long tablesScanned =
        stored.jobKind() == ReconcileJobKind.PLAN_CONNECTOR
                || stored.jobKind() == ReconcileJobKind.PLAN_TABLE
            ? 0L
            : Math.max(0L, stored.tablesScanned);
    long tablesChanged = 0L;
    long viewsScanned =
        stored.jobKind() == ReconcileJobKind.PLAN_CONNECTOR
            ? 0L
            : Math.max(0L, stored.viewsScanned);
    long viewsChanged = 0L;
    long errors = 0L;
    long snapshotsProcessed = 0L;
    long statsProcessed = 0L;
    long indexesProcessed = 0L;
    long plannedFileGroups = selfProjection.plannedFileGroups();
    long plannedFiles = selfProjection.plannedFiles();
    long completedFileGroups = selfProjection.completedFileGroups();
    long failedFileGroups = selfProjection.failedFileGroups();
    long completedFiles = selfProjection.completedFiles();
    long failedFiles = selfProjection.failedFiles();

    long contributionPlannedFileGroups = 0L;
    long contributionPlannedFiles = 0L;
    long contributionCompletedFileGroups = 0L;
    long contributionFailedFileGroups = 0L;
    long contributionCompletedFiles = 0L;
    long contributionFailedFiles = 0L;

    long startedAtMs =
        firstNonZeroMin(
            stored.startedAtMs, contributions, contribution -> contribution.startedAtMs);
    String state = stored.state;
    String message = blankToEmpty(stored.message);
    String executorId = blankToEmpty(stored.executorId);

    StoredJobContribution cancellingChild = null;
    StoredJobContribution runningChild = null;
    StoredJobContribution failedChild = null;
    StoredJobContribution cancelledChild = null;
    StoredJobContribution waitingChild = null;
    StoredJobContribution queuedChild = null;
    DirectChildCounts directChildCounts = DirectChildCounts.empty();
    boolean allSucceeded = true;

    for (StoredJobContribution contribution : contributions) {
      tablesScanned += contribution.tablesScanned;
      tablesChanged += contribution.tablesChanged;
      viewsScanned += contribution.viewsScanned;
      viewsChanged += contribution.viewsChanged;
      errors += contribution.errors;
      snapshotsProcessed += contribution.snapshotsProcessed;
      statsProcessed += contribution.statsProcessed;
      indexesProcessed += contribution.indexesProcessed;
      contributionPlannedFileGroups += contribution.plannedFileGroups;
      contributionPlannedFiles += contribution.plannedFiles;
      contributionCompletedFileGroups += contribution.completedFileGroups;
      contributionFailedFileGroups += contribution.failedFileGroups;
      contributionCompletedFiles += contribution.completedFiles;
      contributionFailedFiles += contribution.failedFiles;

      String childState = blankToEmpty(contribution.state);
      directChildCounts = directChildCounts.incrementedBy(childState);
      if (!"JS_SUCCEEDED".equals(childState)) {
        allSucceeded = false;
      }
      if ("JS_CANCELLING".equals(childState) && cancellingChild == null) {
        cancellingChild = contribution;
      } else if ("JS_RUNNING".equals(childState) && runningChild == null) {
        runningChild = contribution;
      } else if ("JS_FAILED".equals(childState) && failedChild == null) {
        failedChild = contribution;
      } else if ("JS_CANCELLED".equals(childState) && cancelledChild == null) {
        cancelledChild = contribution;
      } else if ("JS_WAITING".equals(childState) && waitingChild == null) {
        waitingChild = contribution;
      } else if ("JS_QUEUED".equals(childState) && queuedChild == null) {
        queuedChild = contribution;
      }
    }

    if (stored.jobKind() == ReconcileJobKind.PLAN_TABLE) {
      tablesScanned =
          contributions.stream()
                  .anyMatch(ReconcileContributionRollupService::planTableDescendantScanned)
              ? 1L
              : 0L;
      tablesChanged =
          contributions.stream()
                  .anyMatch(ReconcileContributionRollupService::planTableDescendantChanged)
              ? 1L
              : 0L;
    } else if (stored.jobKind() == ReconcileJobKind.PLAN_CONNECTOR) {
      tablesScanned =
          Math.max(
              Math.max(Math.max(0L, stored.tablesScanned), tablesScanned),
              maxExpectedChildJobs(stored, directChildCounts.totalObserved()));
    }

    if ("JS_FAILED".equals(stored.state)) {
      state = "JS_FAILED";
      message = blankToEmpty(stored.message);
    } else if ("JS_CANCELLED".equals(stored.state)) {
      state = "JS_CANCELLED";
      message = blankToEmpty(stored.message);
    } else if ("JS_WAITING".equals(stored.state)
        && !directChildJobsComplete(stored, directChildCounts)) {
      state = "JS_WAITING";
      message = firstNonBlank(stored.message, "Waiting on child work");
      executorId = "";
    } else if (cancellingChild != null) {
      state = "JS_CANCELLING";
      message = firstNonBlank(cancellingChild.message, message, "Cancelling");
      executorId = firstNonBlank(cancellingChild.executorId, executorId);
    } else if (runningChild != null) {
      state = "JS_RUNNING";
      message = firstNonBlank(runningChild.message, message, "Running");
      executorId = firstNonBlank(runningChild.executorId, executorId);
    } else if (failedChild != null) {
      state = "JS_FAILED";
      message = firstNonBlank(failedChild.message, message, "Failed");
      executorId = firstNonBlank(failedChild.executorId, executorId);
    } else if (cancelledChild != null) {
      state = "JS_CANCELLED";
      message = firstNonBlank(cancelledChild.message, message, "Cancelled");
      executorId = firstNonBlank(cancelledChild.executorId, executorId);
    } else if (waitingChild != null) {
      state = "JS_WAITING";
      message = firstNonBlank(waitingChild.message, message, "Waiting on child work");
      executorId = "";
    } else if (queuedChild != null
        && ("JS_RUNNING".equals(stored.state)
            || "JS_WAITING".equals(stored.state)
            || "JS_SUCCEEDED".equals(stored.state))) {
      state = "JS_WAITING";
      message = firstNonBlank(queuedChild.message, "Waiting on child work");
      executorId = "";
    } else if (allSucceeded && directChildJobsComplete(stored, directChildCounts)) {
      state = "JS_SUCCEEDED";
      message = ReconcileJobProjector.normalizeSucceededMessage(message);
    } else if (allSucceeded && hasExpectedDirectChildJobs(stored)) {
      state = "JS_WAITING";
      message = "Waiting on child work";
      executorId = "";
    } else if ("JS_RUNNING".equals(state)
        && !leaseManager.hasLiveLease(stored, true, System.currentTimeMillis())
        && hasExpectedDirectChildJobs(stored)) {
      state = "JS_WAITING";
      message = "Waiting on child work";
      executorId = "";
    }

    long finishedAtMs =
        isTerminalState(state) ? maxTerminalFinishedAtMs(stored, contributions, state) : 0L;
    if (isTerminalState(stored.state) && !blank(stored.executorId)) {
      executorId = stored.executorId;
    }
    plannedFileGroups = Math.max(plannedFileGroups, contributionPlannedFileGroups);
    plannedFiles = Math.max(plannedFiles, contributionPlannedFiles);
    if (contributionCompletedFileGroups > 0L) {
      completedFileGroups = contributionCompletedFileGroups;
    }
    if (contributionFailedFileGroups > 0L) {
      failedFileGroups = contributionFailedFileGroups;
    }
    if (contributionCompletedFiles > 0L) {
      completedFiles = contributionCompletedFiles;
    }
    if (contributionFailedFiles > 0L) {
      failedFiles = contributionFailedFiles;
    }

    return new ProjectedPublicJob(
        state,
        firstNonBlank(message, stored.message),
        startedAtMs,
        finishedAtMs,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed,
        firstNonBlank(executorId, stored.executorId),
        new JobProjection(
            indexesProcessed,
            plannedFileGroups,
            plannedFiles,
            completedFileGroups,
            failedFileGroups,
            completedFiles,
            failedFiles));
  }

  private void logAncestorRollupUpdate(
      StoredReconcileJob previous,
      StoredReconcileJob current,
      ProjectedPublicJob projected,
      DirectChildCounts directChildCounts,
      int contributionCount) {
    LOG.debugf(
        "reconcile ancestor rollup updated parentJobId=%s kind=%s oldState=%s newState=%s childCounts=%s contributionCount=%d oldProjection=%s newProjection=%s projectedState=%s projectedMessage=%s leaseOwnsState=%s",
        current.jobId,
        current.jobKind(),
        blankToEmpty(previous.state),
        blankToEmpty(current.state),
        formatDirectChildCounts(directChildCounts),
        contributionCount,
        formatStoredProjection(previous),
        formatStoredProjection(current),
        blankToEmpty(projected.state()),
        blankToEmpty(projected.message()),
        leaseManager.hasLiveLease(current, true, System.currentTimeMillis()));
  }

  private void logAncestorRollupNoop(
      StoredReconcileJob parent,
      ProjectedPublicJob projected,
      DirectChildCounts directChildCounts,
      int contributionCount) {
    LOG.debugf(
        "reconcile ancestor rollup noop parentJobId=%s kind=%s state=%s childCounts=%s contributionCount=%d projection=%s projectedState=%s projectedMessage=%s",
        parent.jobId,
        parent.jobKind(),
        blankToEmpty(parent.state),
        formatDirectChildCounts(directChildCounts),
        contributionCount,
        formatStoredProjection(parent),
        blankToEmpty(projected.state()),
        blankToEmpty(projected.message()));
  }

  private static String canonicalParentStateFromProjection(
      StoredReconcileJob existing,
      ProjectedPublicJob projected,
      DirectChildCounts directChildCounts) {
    String projectedState = blankToEmpty(projected.state());
    return projectedState.isBlank() ? blankToEmpty(existing.state) : projectedState;
  }

  private static String canonicalParentMessageFromProjection(
      StoredReconcileJob existing, ProjectedPublicJob projected, String nextState) {
    if ("JS_SUCCEEDED".equals(nextState)) {
      return ReconcileJobProjector.normalizeSucceededMessage(
          firstNonBlank(projected.message(), existing.message, "Succeeded"));
    }
    return firstNonBlank(projected.message(), existing.message, nextState);
  }

  private static String canonicalParentExecutorFromProjection(
      StoredReconcileJob existing, ProjectedPublicJob projected, String nextState) {
    if ("JS_WAITING".equals(nextState) || "JS_SUCCEEDED".equals(nextState)) {
      return "";
    }
    return firstNonBlank(projected.executorId(), existing.executorId);
  }

  private static String formatDirectChildCounts(DirectChildCounts counts) {
    if (counts == null) {
      return "completed=0 failed=0 cancelled=0";
    }
    return String.format(
        "completed=%d failed=%d cancelled=%d",
        counts.completed(), counts.failed(), counts.cancelled());
  }

  private static String formatStoredProjection(StoredReconcileJob stored) {
    if (stored == null) {
      return "plannedGroups=0 completedGroups=0 failedGroups=0 plannedFiles=0 completedFiles=0 failedFiles=0";
    }
    return String.format(
        "plannedGroups=%d completedGroups=%d failedGroups=%d plannedFiles=%d completedFiles=%d failedFiles=%d",
        stored.plannedFileGroups,
        stored.completedFileGroups,
        stored.failedFileGroups,
        stored.plannedFiles,
        stored.completedFiles,
        stored.failedFiles);
  }

  private static String formatContributionProjection(StoredJobContribution contribution) {
    if (contribution == null) {
      return "plannedGroups=0 completedGroups=0 failedGroups=0 plannedFiles=0 completedFiles=0 failedFiles=0";
    }
    return String.format(
        "plannedGroups=%d completedGroups=%d failedGroups=%d plannedFiles=%d completedFiles=%d failedFiles=%d",
        contribution.plannedFileGroups,
        contribution.completedFileGroups,
        contribution.failedFileGroups,
        contribution.plannedFiles,
        contribution.completedFiles,
        contribution.failedFiles);
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

  private static boolean isTerminalState(String state) {
    if (state == null) {
      return false;
    }
    return switch (state) {
      case "JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED" -> true;
      default -> false;
    };
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static long firstNonZeroMin(
      long seed,
      List<StoredJobContribution> contributions,
      java.util.function.ToLongFunction<StoredJobContribution> extractor) {
    long result = seed > 0L ? seed : 0L;
    for (StoredJobContribution contribution : contributions) {
      long value = extractor.applyAsLong(contribution);
      if (value <= 0L) {
        continue;
      }
      result = result <= 0L ? value : Math.min(result, value);
    }
    return result;
  }

  private long maxTerminalFinishedAtMs(
      StoredReconcileJob stored, List<StoredJobContribution> contributions, String projectedState) {
    long result = isTerminalState(stored.state) ? Math.max(0L, stored.finishedAtMs) : 0L;
    for (StoredJobContribution contribution : contributions) {
      if (!isTerminalState(contribution.state) || contribution.finishedAtMs <= 0L) {
        continue;
      }
      if ("JS_SUCCEEDED".equals(projectedState) && !"JS_SUCCEEDED".equals(contribution.state)) {
        continue;
      }
      result = Math.max(result, contribution.finishedAtMs);
    }
    return result;
  }

  private static boolean hasExpectedDirectChildJobs(StoredReconcileJob stored) {
    return stored != null && Math.max(0L, stored.expectedChildJobs) > 0L;
  }

  private static boolean directChildJobsComplete(
      StoredReconcileJob stored, DirectChildCounts directChildCounts) {
    if (stored == null) {
      return true;
    }
    long expected = maxExpectedChildJobs(stored, directChildCounts.totalObserved());
    if (expected <= 0L) {
      return true;
    }
    return directChildCounts.totalTerminal() >= expected;
  }

  private static long maxExpectedChildJobs(StoredReconcileJob stored, long directChildJobs) {
    if (stored == null) {
      return Math.max(0L, directChildJobs);
    }
    return Math.max(Math.max(0L, stored.expectedChildJobs), Math.max(0L, directChildJobs));
  }

  private static boolean planTableDescendantScanned(StoredJobContribution contribution) {
    return contribution != null
        && (contribution.snapshotsProcessed > 0L
            || contribution.statsProcessed > 0L
            || contribution.indexesProcessed > 0L
            || contribution.errors > 0L
            || contribution.completedFileGroups > 0L
            || contribution.failedFileGroups > 0L
            || contribution.completedFiles > 0L
            || contribution.failedFiles > 0L);
  }

  private static boolean planTableDescendantChanged(StoredJobContribution contribution) {
    return contribution != null
        && (contribution.snapshotsProcessed > 0L
            || contribution.statsProcessed > 0L
            || contribution.indexesProcessed > 0L
            || contribution.completedFileGroups > 0L
            || contribution.completedFiles > 0L);
  }

  public record CanonicalEnvelope(String canonicalPointerKey, StoredReconcileJob record) {}

  @FunctionalInterface
  public interface CanonicalMutator {
    Optional<CanonicalEnvelope> apply(
        String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator);
  }
}
