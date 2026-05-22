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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.CompletionKind;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.IntToLongFunction;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobCompleter {
  private static final Logger LOG = Logger.getLogger(ReconcileJobCompleter.class);

  @FunctionalInterface
  public interface JobMutator {
    Optional<CanonicalEnvelope> apply(String jobId, UnaryOperator<StoredReconcileJob> mutator);
  }

  @FunctionalInterface
  public interface ClearExecutionLeases {
    void clear(CanonicalEnvelope env, String jobId, String leaseEpoch);
  }

  public record CanonicalEnvelope(String canonicalPointerKey, StoredReconcileJob record) {}

  private ReconcileLeaseManager leaseManager;
  private JobMutator mutateByJobIdReturningRecord;
  private BiConsumer<StoredReconcileJob, Boolean> refreshAncestorContributionRollups;
  private BiFunction<String, String, Long> countDirectChildJobs;
  private IntToLongFunction backoffMs;
  private BiFunction<StoredReconcileJob, Long, String> readyPointerKeyFor;
  private ClearExecutionLeases clearExecutionLeasesIfOwned;
  private int maxAttempts;
  private long baseBackoffMs;

  public void bind(
      ReconcileLeaseManager leaseManager,
      JobMutator mutateByJobIdReturningRecord,
      BiConsumer<StoredReconcileJob, Boolean> refreshAncestorContributionRollups,
      BiFunction<String, String, Long> countDirectChildJobs,
      IntToLongFunction backoffMs,
      BiFunction<StoredReconcileJob, Long, String> readyPointerKeyFor,
      ClearExecutionLeases clearExecutionLeasesIfOwned,
      int maxAttempts,
      long baseBackoffMs) {
    this.leaseManager = leaseManager;
    this.mutateByJobIdReturningRecord = mutateByJobIdReturningRecord;
    this.refreshAncestorContributionRollups = refreshAncestorContributionRollups;
    this.countDirectChildJobs = countDirectChildJobs;
    this.backoffMs = backoffMs;
    this.readyPointerKeyFor = readyPointerKeyFor;
    this.clearExecutionLeasesIfOwned = clearExecutionLeasesIfOwned;
    this.maxAttempts = maxAttempts;
    this.baseBackoffMs = baseBackoffMs;
  }

  public void markRunning(String jobId, String leaseEpoch, long startedAtMs, String executorId) {
    mutateByJobIdReturningRecord
        .apply(
            jobId,
            existing -> {
              if (!leaseManager.hasActiveLease(
                  jobId, leaseEpoch, existing, "markRunning", false, true, false)) {
                return null;
              }
              boolean cancelling = "JS_CANCELLING".equals(existing.state);
              if (!cancelling) {
                existing.state = "JS_RUNNING";
                existing.message = "Running";
              }
              if (existing.startedAtMs <= 0L) {
                existing.startedAtMs = startedAtMs;
              }
              existing.executorId = executorId == null ? "" : executorId;
              return existing;
            })
        .ifPresent(env -> refreshAncestorContributionRollups.accept(env.record(), false));
  }

  public void markProgress(
      String jobId,
      String leaseEpoch,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message) {
    boolean[] changedMaterially = new boolean[] {false};
    mutateByJobIdReturningRecord
        .apply(
            jobId,
            existing -> {
              if (!leaseManager.hasActiveLease(
                  jobId, leaseEpoch, existing, "markProgress", false, true, false)) {
                return null;
              }
              if (isTerminalState(existing.state)) {
                return null;
              }
              changedMaterially[0] =
                  existing.tablesScanned != tablesScanned
                      || existing.tablesChanged != tablesChanged
                      || existing.viewsScanned != viewsScanned
                      || existing.viewsChanged != viewsChanged
                      || existing.errors != errors
                      || existing.snapshotsProcessed != snapshotsProcessed
                      || existing.statsProcessed != statsProcessed
                      || (message != null
                          && !message.isBlank()
                          && !message.equals(blankToEmpty(existing.message)));
              existing.tablesScanned = tablesScanned;
              existing.tablesChanged = tablesChanged;
              existing.viewsScanned = viewsScanned;
              existing.viewsChanged = viewsChanged;
              existing.errors = errors;
              existing.snapshotsProcessed = snapshotsProcessed;
              existing.statsProcessed = statsProcessed;
              if (message != null && !message.isBlank()) {
                existing.message = message;
              }
              return existing;
            })
        .ifPresent(
            env -> {
              if (changedMaterially[0]) {
                refreshAncestorContributionRollups.accept(env.record(), false);
              }
            });
  }

  public boolean applyLeaseOutcome(
      String jobId,
      String leaseEpoch,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    long operationStartedAtMs = System.currentTimeMillis();
    var updated =
        mutateByJobIdReturningRecord.apply(
            jobId,
            existing -> {
              String op = operationName(completionKind);
              boolean allowExpiredWithinGrace =
                  completionKind == CompletionKind.SUCCEEDED
                      || completionKind == CompletionKind.FAILED_TERMINAL
                      || completionKind == CompletionKind.CANCELLED;
              if (!leaseManager.hasActiveLease(
                  jobId, leaseEpoch, existing, op, true, true, allowExpiredWithinGrace)) {
                return null;
              }
              if (isTerminalState(existing.state)) {
                return null;
              }
              if ("JS_CANCELLING".equals(existing.state)
                  && completionKind != CompletionKind.CANCELLED) {
                existing.state = "JS_CANCELLED";
                existing.message =
                    blank(existing.message) ? "Cancelled" : blankToEmpty(existing.message);
                if (existing.startedAtMs <= 0L) {
                  existing.startedAtMs = finishedAtMs;
                }
                existing.finishedAtMs = finishedAtMs;
                existing.tablesScanned = tablesScanned;
                existing.tablesChanged = tablesChanged;
                existing.viewsScanned = viewsScanned;
                existing.viewsChanged = viewsChanged;
                existing.errors = errors;
                existing.snapshotsProcessed = snapshotsProcessed;
                existing.statsProcessed = statsProcessed;
                existing.readyPointerKey = null;
                return existing;
              }
              return switch (completionKind) {
                case SUCCEEDED ->
                    applySucceeded(
                        existing,
                        finishedAtMs,
                        message,
                        tablesScanned,
                        tablesChanged,
                        viewsScanned,
                        viewsChanged,
                        snapshotsProcessed,
                        statsProcessed);
                case FAILED_RETRYABLE ->
                    applyFailedRetryable(
                        existing,
                        finishedAtMs,
                        message,
                        tablesScanned,
                        tablesChanged,
                        viewsScanned,
                        viewsChanged,
                        errors,
                        snapshotsProcessed,
                        statsProcessed);
                case FAILED_WAITING ->
                    applyFailedWaiting(
                        existing,
                        message,
                        tablesScanned,
                        tablesChanged,
                        viewsScanned,
                        viewsChanged,
                        errors,
                        snapshotsProcessed,
                        statsProcessed);
                case FAILED_TERMINAL ->
                    applyFailedTerminal(
                        existing,
                        finishedAtMs,
                        message,
                        tablesScanned,
                        tablesChanged,
                        viewsScanned,
                        viewsChanged,
                        errors,
                        snapshotsProcessed,
                        statsProcessed);
                case CANCELLED ->
                    applyCancelled(
                        existing,
                        finishedAtMs,
                        message,
                        tablesScanned,
                        tablesChanged,
                        viewsScanned,
                        viewsChanged,
                        errors,
                        snapshotsProcessed,
                        statsProcessed);
              };
            });
    updated.ifPresent(
        env -> {
          long mutationElapsedMs = System.currentTimeMillis() - operationStartedAtMs;
          clearExecutionLeasesIfOwned.clear(env, jobId, leaseEpoch);
          long refreshStartedAtMs = System.currentTimeMillis();
          refreshAncestorContributionRollups.accept(
              env.record(),
              isTerminalState(env.record().state)
                  || env.record().jobKind() == ReconcileJobKind.PLAN_SNAPSHOT);
          LOG.debugf(
              "applyLeaseOutcome mutation_ms=%d contribution_refresh_ms=%d jobId=%s completionKind=%s",
              mutationElapsedMs,
              System.currentTimeMillis() - refreshStartedAtMs,
              jobId,
              completionKind);
        });
    return updated.isPresent();
  }

  private StoredReconcileJob applySucceeded(
      StoredReconcileJob existing,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.tablesScanned = tablesScanned;
    existing.tablesChanged = tablesChanged;
    existing.viewsScanned = viewsScanned;
    existing.viewsChanged = viewsChanged;
    existing.errors = 0L;
    existing.snapshotsProcessed = snapshotsProcessed;
    existing.statsProcessed = statsProcessed;
    existing.lastError = "";
    long directChildJobs = countDirectChildJobs.apply(existing.accountId, existing.jobId);
    if (hasIncompleteDirectChildJobs(existing)
        || (directChildJobs > 0L
            && !directChildJobsComplete(
                maxExpectedChildJobs(existing, directChildJobs),
                existing.completedChildJobs,
                existing.failedChildJobs,
                existing.cancelledChildJobs))) {
      if (existing.expectedChildJobs <= 0L) {
        existing.expectedChildJobs = directChildJobs;
      }
      existing.state = "JS_WAITING";
      existing.message = blank(message) ? "Waiting on child work" : message;
      existing.finishedAtMs = 0L;
      existing.executorId = "";
    } else {
      existing.state = "JS_SUCCEEDED";
      existing.message = "Succeeded";
      existing.finishedAtMs = finishedAtMs;
    }
    if (existing.startedAtMs <= 0L) {
      existing.startedAtMs = finishedAtMs;
    }
    existing.readyPointerKey = null;
    return existing;
  }

  private StoredReconcileJob applyFailedRetryable(
      StoredReconcileJob existing,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.attempt = Math.max(0, existing.attempt) + 1;
    existing.tablesScanned = tablesScanned;
    existing.tablesChanged = tablesChanged;
    existing.viewsScanned = viewsScanned;
    existing.viewsChanged = viewsChanged;
    existing.errors = errors;
    existing.snapshotsProcessed = snapshotsProcessed;
    existing.statsProcessed = statsProcessed;
    existing.lastError = message == null ? "Failed" : message;

    if (existing.attempt >= maxAttempts) {
      existing.state = "JS_FAILED";
      existing.message = message == null ? "Failed" : message;
      if (existing.startedAtMs <= 0L) {
        existing.startedAtMs = finishedAtMs;
      }
      existing.finishedAtMs = finishedAtMs;
      existing.readyPointerKey = null;
      return existing;
    }

    long now = System.currentTimeMillis();
    existing.state = "JS_QUEUED";
    existing.message = message == null ? "Retrying" : message;
    existing.executorId = "";
    existing.nextAttemptAtMs = now + backoffMs.applyAsLong(existing.attempt);
    existing.finishedAtMs = 0L;
    existing.readyPointerKey = readyPointerKeyFor.apply(existing, existing.nextAttemptAtMs);
    return existing;
  }

  private StoredReconcileJob applyFailedWaiting(
      StoredReconcileJob existing,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.tablesScanned = tablesScanned;
    existing.tablesChanged = tablesChanged;
    existing.viewsScanned = viewsScanned;
    existing.viewsChanged = viewsChanged;
    existing.errors = errors;
    existing.snapshotsProcessed = snapshotsProcessed;
    existing.statsProcessed = statsProcessed;
    existing.lastError = message == null ? "Waiting on dependency" : message;
    existing.state = "JS_QUEUED";
    existing.message = message == null ? "Waiting on dependency" : message;
    existing.executorId = "";
    existing.nextAttemptAtMs = System.currentTimeMillis() + baseBackoffMs;
    existing.finishedAtMs = 0L;
    existing.readyPointerKey = readyPointerKeyFor.apply(existing, existing.nextAttemptAtMs);
    return existing;
  }

  private StoredReconcileJob applyFailedTerminal(
      StoredReconcileJob existing,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.attempt = Math.max(0, existing.attempt) + 1;
    existing.tablesScanned = tablesScanned;
    existing.tablesChanged = tablesChanged;
    existing.viewsScanned = viewsScanned;
    existing.viewsChanged = viewsChanged;
    existing.errors = errors;
    existing.snapshotsProcessed = snapshotsProcessed;
    existing.statsProcessed = statsProcessed;
    existing.lastError = message == null ? "Failed" : message;
    existing.state = "JS_FAILED";
    existing.message = message == null ? "Failed" : message;
    if (existing.startedAtMs <= 0L) {
      existing.startedAtMs = finishedAtMs;
    }
    existing.finishedAtMs = finishedAtMs;
    existing.readyPointerKey = null;
    return existing;
  }

  private StoredReconcileJob applyCancelled(
      StoredReconcileJob existing,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.state = "JS_CANCELLED";
    existing.message = message == null || message.isBlank() ? "Cancelled" : message;
    if (existing.startedAtMs <= 0L) {
      existing.startedAtMs = finishedAtMs;
    }
    existing.finishedAtMs = finishedAtMs;
    existing.tablesScanned = tablesScanned;
    existing.tablesChanged = tablesChanged;
    existing.viewsScanned = viewsScanned;
    existing.viewsChanged = viewsChanged;
    existing.errors = errors;
    existing.snapshotsProcessed = snapshotsProcessed;
    existing.statsProcessed = statsProcessed;
    existing.readyPointerKey = null;
    return existing;
  }

  private static String operationName(CompletionKind completionKind) {
    return switch (completionKind) {
      case SUCCEEDED -> "markSucceeded";
      case FAILED_RETRYABLE -> "markFailed";
      case FAILED_WAITING -> "markWaiting";
      case FAILED_TERMINAL -> "markFailedTerminal";
      case CANCELLED -> "markCancelled";
    };
  }

  private static boolean hasExpectedDirectChildJobs(StoredReconcileJob stored) {
    return stored != null && Math.max(0L, stored.expectedChildJobs) > 0L;
  }

  private static boolean hasIncompleteDirectChildJobs(StoredReconcileJob stored) {
    return hasExpectedDirectChildJobs(stored) && !directChildJobsComplete(stored);
  }

  private static boolean directChildJobsComplete(StoredReconcileJob stored) {
    if (stored == null) {
      return true;
    }
    return directChildJobsComplete(
        Math.max(0L, stored.expectedChildJobs),
        stored.completedChildJobs,
        stored.failedChildJobs,
        stored.cancelledChildJobs);
  }

  private static boolean directChildJobsComplete(
      long expectedChildJobs,
      long completedChildJobs,
      long failedChildJobs,
      long cancelledChildJobs) {
    long expected = Math.max(0L, expectedChildJobs);
    if (expected <= 0L) {
      return true;
    }
    long terminalChildren =
        Math.max(0L, completedChildJobs)
            + Math.max(0L, failedChildJobs)
            + Math.max(0L, cancelledChildJobs);
    return terminalChildren >= expected;
  }

  private static long maxExpectedChildJobs(StoredReconcileJob stored, long directChildJobs) {
    if (stored == null) {
      return Math.max(0L, directChildJobs);
    }
    return Math.max(Math.max(0L, stored.expectedChildJobs), Math.max(0L, directChildJobs));
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

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
