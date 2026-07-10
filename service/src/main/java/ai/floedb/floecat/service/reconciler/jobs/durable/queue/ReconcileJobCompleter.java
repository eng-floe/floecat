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

import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
import java.util.function.UnaryOperator;

@ApplicationScoped
public class ReconcileJobCompleter {
  @FunctionalInterface
  public interface JobMutator {
    Optional<CanonicalEnvelope> apply(String jobId, UnaryOperator<StoredReconcileJob> mutator);
  }

  public record CanonicalEnvelope(String canonicalPointerKey, StoredReconcileJob record) {}

  private ReconcileLeaseStore leaseManager;
  private JobMutator mutateByJobIdReturningRecord;

  public void bind(ReconcileLeaseStore leaseManager, JobMutator mutateByJobIdReturningRecord) {
    this.leaseManager = leaseManager;
    this.mutateByJobIdReturningRecord = mutateByJobIdReturningRecord;
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
          existing.childrenFinalized = false;
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

  private static boolean isTerminalState(String state) {
    if (state == null) {
      return false;
    }
    return switch (state) {
      case "JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED" -> true;
      default -> false;
    };
  }
}
