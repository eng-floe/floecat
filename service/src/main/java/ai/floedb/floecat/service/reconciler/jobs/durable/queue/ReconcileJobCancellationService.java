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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobCancellationService {
  private static final Logger LOG = Logger.getLogger(ReconcileJobCancellationService.class);

  @FunctionalInterface
  public interface CanonicalMutator {
    Optional<CanonicalEnvelope> apply(
        String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator);
  }

  @FunctionalInterface
  public interface ClearExecutionLeases {
    void clear(CanonicalEnvelope env, String jobId, String leaseEpoch);
  }

  public record CanonicalEnvelope(String canonicalPointerKey, StoredReconcileJob record) {}

  private ReconcileLeaseStore leaseManager;
  private Function<String, Optional<CanonicalEnvelope>> loadByAnyAccount;
  private CanonicalMutator mutateByCanonicalPointerReturningRecord;
  private GetJob getJob;
  private ClearExecutionLeases clearExecutionLeasesIfOwned;
  private long cancelPokeMaxDelayMs;

  @FunctionalInterface
  public interface GetJob {
    Optional<ReconcileJob> get(String accountId, String jobId);
  }

  public void bind(
      ReconcileLeaseStore leaseManager,
      Function<String, Optional<CanonicalEnvelope>> loadByAnyAccount,
      CanonicalMutator mutateByCanonicalPointerReturningRecord,
      GetJob getJob,
      ClearExecutionLeases clearExecutionLeasesIfOwned,
      long cancelPokeMaxDelayMs) {
    this.leaseManager = leaseManager;
    this.loadByAnyAccount = loadByAnyAccount;
    this.mutateByCanonicalPointerReturningRecord = mutateByCanonicalPointerReturningRecord;
    this.getJob = getJob;
    this.clearExecutionLeasesIfOwned = clearExecutionLeasesIfOwned;
    this.cancelPokeMaxDelayMs = cancelPokeMaxDelayMs;
  }

  public Optional<ReconcileJob> cancel(String accountId, String jobId, String reason) {
    var loaded = loadByAnyAccount.apply(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    if (accountId != null
        && !accountId.isBlank()
        && !accountId.equals(loaded.get().record().accountId)) {
      return Optional.empty();
    }
    String priorLeaseEpoch =
        leaseManager.loadLease(loaded.get().record()).map(lease -> lease.epoch).orElse("");
    var updated =
        mutateByCanonicalPointerReturningRecord.apply(
            loaded.get().canonicalPointerKey(),
            existing -> {
              if (isTerminalState(existing.state) || "JS_CANCELLING".equals(existing.state)) {
                return null;
              }
              if ("JS_RUNNING".equals(existing.state) && existing.snapshotFinalizeCommitStarted) {
                return null;
              }
              if ("JS_RUNNING".equals(existing.state)) {
                long now = System.currentTimeMillis();
                existing.state = "JS_CANCELLING";
                existing.message = (reason == null || reason.isBlank()) ? "Cancelling" : reason;
                existing.nextAttemptAtMs = now;
                existing.readyPointerKey = null;
                existing.updatedAtMs = now;
                return existing;
              }
              existing.state = "JS_CANCELLED";
              existing.message = (reason == null || reason.isBlank()) ? "Cancelled" : reason;
              long now = System.currentTimeMillis();
              if (existing.startedAtMs <= 0L) {
                existing.startedAtMs = now;
              }
              existing.finishedAtMs = now;
              existing.readyPointerKey = null;
              return existing;
            });
    if (updated.isPresent() && priorLeaseEpoch != null && !priorLeaseEpoch.isBlank()) {
      boolean shortened = false;
      for (int i = 0; i < 3 && !shortened; i++) {
        shortened =
            leaseManager
                .mutateLease(
                    loaded.get().record().accountId,
                    jobId,
                    current -> {
                      if (!priorLeaseEpoch.equals(current.epoch)) {
                        return null;
                      }
                      long now = System.currentTimeMillis();
                      current.expiresAtMs =
                          Math.min(Math.max(now, current.expiresAtMs), now + cancelPokeMaxDelayMs);
                      return current;
                    })
                .isPresent();
      }
      if (!shortened) {
        LOG.warnf(
            "Unable to shorten reconcile lease after cancellation accountId=%s jobId=%s epoch=%s",
            loaded.get().record().accountId, jobId, priorLeaseEpoch);
      }
    }
    var post = getJob.get(accountId, jobId);
    if (post.isPresent()
        && ("JS_CANCELLED".equals(post.get().state) || "JS_CANCELLING".equals(post.get().state))) {
      if (updated.isPresent()
          && "JS_CANCELLED".equals(post.get().state)
          && priorLeaseEpoch != null
          && !priorLeaseEpoch.isBlank()) {
        clearExecutionLeasesIfOwned.clear(updated.get(), jobId, priorLeaseEpoch);
      } else if (updated.isPresent() && "JS_CANCELLED".equals(post.get().state)) {
        leaseManager.clearLaneLeaseIfOwned(
            updated.get().record(), updated.get().canonicalPointerKey());
        leaseManager.clearSnapshotLeaseIfOwned(
            updated.get().record(), updated.get().canonicalPointerKey());
      }
      return post;
    }
    return Optional.empty();
  }

  public boolean isCancellationRequested(String jobId) {
    var job = getJob.get(null, jobId);
    if (job.isEmpty()) {
      return false;
    }
    String state = job.get().state;
    return "JS_CANCELLING".equals(state) || "JS_CANCELLED".equals(state);
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
