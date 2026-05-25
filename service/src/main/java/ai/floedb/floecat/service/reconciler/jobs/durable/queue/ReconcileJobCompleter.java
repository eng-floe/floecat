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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.CompletionKind;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
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

  private ReconcileLeaseStore leaseManager;
  private JobMutator mutateByJobIdReturningRecord;
  private IntToLongFunction backoffMs;
  private BiFunction<StoredReconcileJob, Long, String> readyPointerKeyFor;
  private ClearExecutionLeases clearExecutionLeasesIfOwned;
  private int maxAttempts;
  private long baseBackoffMs;

  public void bind(
      ReconcileLeaseStore leaseManager,
      JobMutator mutateByJobIdReturningRecord,
      IntToLongFunction backoffMs,
      BiFunction<StoredReconcileJob, Long, String> readyPointerKeyFor,
      ClearExecutionLeases clearExecutionLeasesIfOwned,
      int maxAttempts,
      long baseBackoffMs) {
    this.leaseManager = leaseManager;
    this.mutateByJobIdReturningRecord = mutateByJobIdReturningRecord;
    this.backoffMs = backoffMs;
    this.readyPointerKeyFor = readyPointerKeyFor;
    this.clearExecutionLeasesIfOwned = clearExecutionLeasesIfOwned;
    this.maxAttempts = maxAttempts;
    this.baseBackoffMs = baseBackoffMs;
  }

  public void markRunning(String jobId, String leaseEpoch, long startedAtMs, String executorId) {
    mutateByJobIdReturningRecord.apply(
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
        });
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
              existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
              existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
              existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
              existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
              existing.errors = errors;
              existing.snapshotsProcessed =
                  Math.max(existing.snapshotsProcessed, snapshotsProcessed);
              existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
              if (message != null && !message.isBlank()) {
                existing.message = message;
              }
              return existing;
            })
        .ifPresent(env -> {});
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
                      || completionKind == CompletionKind.SUCCEEDED_WAITING
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
                existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
                existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
                existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
                existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
                existing.errors = errors;
                existing.snapshotsProcessed =
                    Math.max(existing.snapshotsProcessed, snapshotsProcessed);
                existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
                existing.readyPointerKey = null;
                return existing;
              }
              return switch (completionKind) {
                case SUCCEEDED ->
                    applySucceeded(
                        existing,
                        finishedAtMs,
                        "JS_SUCCEEDED",
                        "Succeeded",
                        finishedAtMs,
                        blankToEmpty(existing.executorId),
                        tablesScanned,
                        tablesChanged,
                        viewsScanned,
                        viewsChanged,
                        snapshotsProcessed,
                        statsProcessed);
                case SUCCEEDED_WAITING ->
                    applySucceeded(
                        existing,
                        finishedAtMs,
                        "JS_WAITING",
                        blank(message) ? "Waiting on child work" : message,
                        0L,
                        "",
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
          LOG.debugf(
              "applyLeaseOutcome mutation_ms=%d jobId=%s completionKind=%s",
              mutationElapsedMs, jobId, completionKind);
        });
    return updated.isPresent();
  }

  private StoredReconcileJob applySucceeded(
      StoredReconcileJob existing,
      long finishedAtMs,
      String nextState,
      String nextMessage,
      long nextFinishedAtMs,
      String nextExecutorId,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = 0L;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.lastError = "";
    existing.state = nextState;
    existing.message = nextMessage;
    existing.finishedAtMs = nextFinishedAtMs;
    existing.executorId = nextExecutorId;
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
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
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
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
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
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
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
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.readyPointerKey = null;
    return existing;
  }

  private static String operationName(CompletionKind completionKind) {
    return switch (completionKind) {
      case SUCCEEDED -> "markSucceeded";
      case SUCCEEDED_WAITING -> "markSucceededWaiting";
      case FAILED_RETRYABLE -> "markFailed";
      case FAILED_WAITING -> "markWaiting";
      case FAILED_TERMINAL -> "markFailedTerminal";
      case CANCELLED -> "markCancelled";
    };
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
