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

package ai.floedb.floecat.service.reconciler.jobs;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

@ApplicationScoped
@IfBuildProperty(name = "floecat.reconciler.job-store", stringValue = "durable")
public class DurableReconcileJobStore implements ReconcileJobStore {
  private static final Logger LOG = Logger.getLogger(DurableReconcileJobStore.class);

  private static final int DEFAULT_MAX_ATTEMPTS = 8;
  private static final long DEFAULT_BASE_BACKOFF_MS = 500L;
  private static final long DEFAULT_MAX_BACKOFF_MS = 30_000L;
  private static final long DEFAULT_LEASE_MS = 30_000L;
  private static final long DEFAULT_RECLAIM_INTERVAL_MS = 5_000L;
  private static final long DEFAULT_LEASE_RENEW_GRACE_MS = 5_000L;
  private static final long CANCEL_POKE_MAX_DELAY_MS = 1_000L;
  private static final int DEFAULT_READY_SCAN_LIMIT = 128;
  private static final int FALLBACK_SCAN_MAX_PAGES = 200;
  private static final int CAS_MAX = 16;
  private static final String LIST_TOKEN_V1_PREFIX = "v1:";

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject ObjectMapper mapper;
  @Inject Config config;

  private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
  private long baseBackoffMs = DEFAULT_BASE_BACKOFF_MS;
  private long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;
  private long leaseMs = DEFAULT_LEASE_MS;
  private long reclaimIntervalMs = DEFAULT_RECLAIM_INTERVAL_MS;
  private long leaseRenewGraceMs = DEFAULT_LEASE_RENEW_GRACE_MS;
  private int readyScanLimit = DEFAULT_READY_SCAN_LIMIT;

  private final String leaseOwner = "reconcile-store-" + UUID.randomUUID();
  private volatile long lastReclaimAtMs;
  private final ReentrantLock reclaimLock = new ReentrantLock();

  @PostConstruct
  void init() {
    maxAttempts =
        Math.max(
            1,
            config
                .getOptionalValue("floecat.reconciler.job-store.max-attempts", Integer.class)
                .orElse(DEFAULT_MAX_ATTEMPTS));
    baseBackoffMs =
        Math.max(
            100L,
            config
                .getOptionalValue("floecat.reconciler.job-store.base-backoff-ms", Long.class)
                .orElse(DEFAULT_BASE_BACKOFF_MS));
    maxBackoffMs =
        Math.max(
            baseBackoffMs,
            config
                .getOptionalValue("floecat.reconciler.job-store.max-backoff-ms", Long.class)
                .orElse(DEFAULT_MAX_BACKOFF_MS));
    leaseMs =
        Math.max(
            1_000L,
            config
                .getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class)
                .orElse(DEFAULT_LEASE_MS));
    reclaimIntervalMs =
        Math.max(
            1_000L,
            config
                .getOptionalValue("floecat.reconciler.job-store.reclaim-interval-ms", Long.class)
                .orElse(DEFAULT_RECLAIM_INTERVAL_MS));
    leaseRenewGraceMs =
        Math.max(
            0L,
            config
                .getOptionalValue("floecat.reconciler.job-store.lease-renew-grace-ms", Long.class)
                .orElse(DEFAULT_LEASE_RENEW_GRACE_MS));
    readyScanLimit =
        Math.max(
            1,
            config
                .getOptionalValue("floecat.reconciler.job-store.ready-scan-limit", Integer.class)
                .orElse(DEFAULT_READY_SCAN_LIMIT));
  }

  @Override
  public String enqueue(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope incomingScope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    ReconcileJobKind effectiveJobKind = jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind;
    ReconcileTableTask effectiveTableTask =
        tableTask == null ? ReconcileTableTask.empty() : tableTask;
    ReconcileViewTask effectiveViewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
    ReconcileScope scope =
        normalizeScopeForJobKind(
            incomingScope == null ? ReconcileScope.empty() : incomingScope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask);
    ReconcileExecutionPolicy policy =
        executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
    String laneKey =
        laneKey(connectorId, scope, effectiveJobKind, effectiveTableTask, effectiveViewTask);
    String dedupeKey =
        dedupeKey(
            accountId,
            connectorId,
            fullRescan,
            captureMode,
            scope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask,
            policy,
            parentJobId,
            pinnedExecutorId);
    String dedupePointerKey = Keys.reconcileDedupePointer(accountId, hashValue(dedupeKey));

    for (int attempt = 0; attempt < CAS_MAX; attempt++) {
      var existing = loadActiveFromDedupe(dedupePointerKey);
      if (existing.isPresent()) {
        return existing.get().jobId;
      }

      String jobId = UUID.randomUUID().toString();
      long now = System.currentTimeMillis();
      String canonicalKey = Keys.reconcileJobPointerById(accountId, jobId);
      String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
      String parentKey = parentPointerKey(accountId, parentJobId, jobId);
      String readyKey = Keys.reconcileReadyPointerByDue(now, accountId, laneKey, jobId);
      String blobUri =
          Keys.reconcileJobBlobUri(accountId, jobId, "create-" + now + "-" + UUID.randomUUID());

      Pointer dedupeReserve =
          Pointer.newBuilder().setKey(dedupePointerKey).setBlobUri(blobUri).setVersion(1L).build();
      if (!pointerStore.compareAndSet(dedupePointerKey, 0L, dedupeReserve)) {
        continue;
      }

      StoredReconcileJob record =
          StoredReconcileJob.queued(
              jobId,
              accountId,
              connectorId,
              fullRescan,
              captureMode,
              scope,
              effectiveJobKind,
              effectiveTableTask,
              effectiveViewTask,
              policy,
              parentJobId,
              pinnedExecutorId,
              laneKey,
              dedupeKey,
              now,
              readyKey);

      try {
        blobStore.put(
            blobUri,
            mapper.writeValueAsBytes(record),
            "application/json; charset=" + StandardCharsets.UTF_8.name());
      } catch (Exception e) {
        clearDedupeReservation(dedupePointerKey, blobUri);
        throw new IllegalStateException("Failed to persist reconcile job payload", e);
      }

      Pointer canonical =
          Pointer.newBuilder().setKey(canonicalKey).setBlobUri(blobUri).setVersion(1L).build();
      if (!pointerStore.compareAndSet(canonicalKey, 0L, canonical)) {
        clearDedupeReservation(dedupePointerKey, blobUri);
        blobStore.delete(blobUri);
        continue;
      }

      Pointer lookup =
          Pointer.newBuilder().setKey(lookupKey).setBlobUri(blobUri).setVersion(1L).build();
      if (!pointerStore.compareAndSet(lookupKey, 0L, lookup)) {
        clearPointerIfMatches(canonicalKey, blobUri);
        clearDedupeReservation(dedupePointerKey, blobUri);
        blobStore.delete(blobUri);
        continue;
      }

      if (!parentKey.isBlank()) {
        Pointer parent =
            Pointer.newBuilder().setKey(parentKey).setBlobUri(blobUri).setVersion(1L).build();
        if (!pointerStore.compareAndSet(parentKey, 0L, parent)) {
          clearPointerIfMatches(lookupKey, blobUri);
          clearPointerIfMatches(canonicalKey, blobUri);
          clearDedupeReservation(dedupePointerKey, blobUri);
          blobStore.delete(blobUri);
          continue;
        }
      }

      Pointer ready =
          Pointer.newBuilder().setKey(readyKey).setBlobUri(blobUri).setVersion(1L).build();
      if (!pointerStore.compareAndSet(readyKey, 0L, ready)) {
        clearPointerIfMatches(parentKey, blobUri);
        clearPointerIfMatches(lookupKey, blobUri);
        clearPointerIfMatches(canonicalKey, blobUri);
        clearDedupeReservation(dedupePointerKey, blobUri);
        blobStore.delete(blobUri);
        continue;
      }

      return jobId;
    }

    throw new IllegalStateException("Unable to enqueue reconcile job after CAS retries");
  }

  @Override
  public Optional<ReconcileJob> get(String accountId, String jobId) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    if (accountId != null
        && !accountId.isBlank()
        && !accountId.equals(loaded.get().record.accountId)) {
      return Optional.empty();
    }
    return Optional.of(toPublicJob(loaded.get().record));
  }

  @Override
  public ReconcileJobPage list(
      String accountId,
      int pageSize,
      String pageToken,
      String connectorId,
      java.util.Set<String> states) {
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    List<ReconcileJob> out = new java.util.ArrayList<>(limit);
    String nextToken = "";
    while (out.size() < limit) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileJobPointerByIdPrefix(accountId), Math.max(limit * 2, 64), token, next);
      if (pointers.isEmpty()) {
        break;
      }
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        Pointer ptr = pointers.get(i);
        var rec = readRecord(ptr);
        if (rec.isEmpty()) {
          continue;
        }
        var job = toPublicJob(rec.get());
        if (connectorId != null && !connectorId.isBlank() && !connectorId.equals(job.connectorId)) {
          continue;
        }
        if (states != null && !states.isEmpty() && !states.contains(job.state)) {
          continue;
        }
        out.add(job);
        if (out.size() >= limit) {
          boolean hasMore = i + 1 < pointers.size() || next.length() > 0;
          if (!hasMore) {
            nextToken = "";
          } else if (i + 1 < pointers.size()) {
            nextToken = encodeListCursor(token, i + 1);
          } else {
            nextToken = next.toString();
          }
          break;
        }
      }
      if (out.size() >= limit) {
        break;
      }
      nextToken = next.toString();
      if (nextToken.isBlank()) {
        break;
      }
      token = nextToken;
    }
    return new ReconcileJobPage(out, nextToken);
  }

  @Override
  public List<ReconcileJob> childJobs(String accountId, String parentJobId) {
    if (accountId == null || accountId.isBlank() || parentJobId == null || parentJobId.isBlank()) {
      return List.of();
    }
    List<ReconcileJob> out = new ArrayList<>();
    String token = "";
    String prefix = Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId);
    do {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 200, token, next);
      for (Pointer ptr : pointers) {
        var rec = readRecordByBlobUri(ptr.getBlobUri());
        rec.ifPresent(stored -> out.add(toPublicJob(stored)));
      }
      token = next.toString();
    } while (token != null && !token.isBlank());
    return List.copyOf(out);
  }

  @Override
  public QueueStats queueStats() {
    long queued = 0L;
    long running = 0L;
    long cancelling = 0L;
    long oldestQueued = 0L;
    String token = "";
    int pages = 0;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileJobLookupPointerByIdPrefix(), 256, token, next);
      if (pointers.isEmpty()) {
        break;
      }
      for (Pointer ptr : pointers) {
        var rec = readRecordByBlobUri(ptr.getBlobUri());
        if (rec.isEmpty() || rec.get().state == null) {
          continue;
        }
        switch (rec.get().state) {
          case "JS_QUEUED" -> {
            queued++;
            long created = Math.max(0L, rec.get().createdAtMs);
            if (created > 0L && (oldestQueued == 0L || created < oldestQueued)) {
              oldestQueued = created;
            }
          }
          case "JS_RUNNING" -> running++;
          case "JS_CANCELLING" -> cancelling++;
          default -> {}
        }
      }
      token = next.toString();
      pages++;
      if (token.isBlank() || pages >= 10_000) {
        break;
      }
    }
    return new QueueStats(queued, running, cancelling, oldestQueued);
  }

  @Override
  public Optional<LeasedJob> leaseNext(LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    long now = System.currentTimeMillis();
    var leased = leaseReadyDue(now, effective);
    if (leased.isPresent()) {
      return leased;
    }

    // Reclaim is best-effort recovery work; do not block ready leasing on potentially expensive
    // scans.
    reclaimExpiredLeasesIfDue(now);
    return leaseReadyDue(System.currentTimeMillis(), effective);
  }

  @Override
  public boolean renewLease(String jobId, String leaseEpoch) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return false;
    }
    return renewLeaseByCanonicalPointer(loaded.get().canonicalPointerKey, jobId, leaseEpoch);
  }

  @Override
  public void markRunning(String jobId, String leaseEpoch, long startedAtMs, String executorId) {
    mutateByJobId(
        jobId,
        existing -> {
          if (!hasActiveLease(jobId, leaseEpoch, existing, "markRunning", false, true)) {
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

  @Override
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
    mutateByJobId(
        jobId,
        existing -> {
          if (!hasActiveLease(jobId, leaseEpoch, existing, "markProgress", false, true)) {
            return null;
          }
          if (isTerminalState(existing.state)) {
            return existing;
          }
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
        });
  }

  @Override
  public void markSucceeded(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long snapshotsProcessed,
      long statsProcessed) {
    mutateByJobId(
        jobId,
        existing -> {
          if (!hasActiveLease(jobId, leaseEpoch, existing, "markSucceeded", false, true)) {
            return null;
          }
          if (isTerminalState(existing.state) || "JS_CANCELLING".equals(existing.state)) {
            return existing;
          }
          existing.state = "JS_SUCCEEDED";
          existing.message = "Succeeded";
          if (existing.startedAtMs <= 0L) {
            existing.startedAtMs = finishedAtMs;
          }
          existing.finishedAtMs = finishedAtMs;
          existing.tablesScanned = tablesScanned;
          existing.tablesChanged = tablesChanged;
          existing.viewsScanned = viewsScanned;
          existing.viewsChanged = viewsChanged;
          existing.snapshotsProcessed = snapshotsProcessed;
          existing.statsProcessed = statsProcessed;
          clearLaneLeaseIfOwned(existing, existing.currentBlobUri);
          existing.leaseOwner = null;
          existing.leaseEpoch = null;
          existing.leaseExpiresAtMs = 0L;
          clearReadyPointer(existing.readyPointerKey);
          existing.readyPointerKey = null;
          clearDedupeIfOwned(existing);
          return existing;
        });
  }

  @Override
  public void markFailed(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    mutateByJobId(
        jobId,
        existing -> {
          if (!hasActiveLease(jobId, leaseEpoch, existing, "markFailed", false, true)) {
            return null;
          }
          if (isTerminalState(existing.state) || "JS_CANCELLING".equals(existing.state)) {
            return existing;
          }
          existing.attempt = Math.max(0, existing.attempt) + 1;
          existing.tablesScanned = tablesScanned;
          existing.tablesChanged = tablesChanged;
          existing.viewsScanned = viewsScanned;
          existing.viewsChanged = viewsChanged;
          existing.errors = errors;
          existing.snapshotsProcessed = snapshotsProcessed;
          existing.statsProcessed = statsProcessed;
          existing.lastError = message == null ? "Failed" : message;
          clearLaneLeaseIfOwned(existing, existing.currentBlobUri);
          existing.leaseOwner = null;
          existing.leaseEpoch = null;
          existing.leaseExpiresAtMs = 0L;

          if (existing.attempt >= maxAttempts) {
            existing.state = "JS_FAILED";
            existing.message = message == null ? "Failed" : message;
            if (existing.startedAtMs <= 0L) {
              existing.startedAtMs = finishedAtMs;
            }
            existing.finishedAtMs = finishedAtMs;
            clearReadyPointer(existing.readyPointerKey);
            existing.readyPointerKey = null;
            clearDedupeIfOwned(existing);
            return existing;
          }

          long now = System.currentTimeMillis();
          existing.state = "JS_QUEUED";
          existing.message = message == null ? "Retrying" : message;
          existing.executorId = "";
          existing.nextAttemptAtMs = now + backoffMs(existing.attempt);
          existing.finishedAtMs = 0L;
          String readyKey =
              Keys.reconcileReadyPointerByDue(
                  existing.nextAttemptAtMs, existing.accountId, existing.laneKey, existing.jobId);
          clearReadyPointer(existing.readyPointerKey);
          if (upsertReadyPointer(readyKey, existing.currentBlobUri)) {
            existing.readyPointerKey = readyKey;
          } else {
            LOG.warnf("Failed to requeue reconcile job %s after failure", existing.jobId);
            existing.state = "JS_FAILED";
            existing.finishedAtMs = finishedAtMs;
            clearDedupeIfOwned(existing);
          }
          return existing;
        });
  }

  @Override
  public Optional<ReconcileJob> cancel(String accountId, String jobId, String reason) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    if (accountId != null
        && !accountId.isBlank()
        && !accountId.equals(loaded.get().record.accountId)) {
      return Optional.empty();
    }
    mutateByCanonicalPointer(
        loaded.get().canonicalPointerKey,
        existing -> {
          if (isTerminalState(existing.state) || "JS_CANCELLING".equals(existing.state)) {
            return existing;
          }
          if ("JS_RUNNING".equals(existing.state)) {
            long now = System.currentTimeMillis();
            existing.state = "JS_CANCELLING";
            existing.message = (reason == null || reason.isBlank()) ? "Cancelling" : reason;
            long cancelPokeExpiry = now + CANCEL_POKE_MAX_DELAY_MS;
            if (existing.leaseExpiresAtMs <= 0L) {
              existing.leaseExpiresAtMs = cancelPokeExpiry;
            } else {
              existing.leaseExpiresAtMs = Math.min(existing.leaseExpiresAtMs, cancelPokeExpiry);
            }
            String readyKey =
                Keys.reconcileReadyPointerByDue(
                    now, existing.accountId, existing.laneKey, existing.jobId);
            clearReadyPointer(existing.readyPointerKey);
            if (upsertReadyPointer(readyKey, existing.currentBlobUri)) {
              existing.readyPointerKey = readyKey;
            } else {
              LOG.warnf(
                  "Failed to poke cancelling reconcile job %s for near-term pickup",
                  existing.jobId);
            }
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
          clearLaneLeaseIfOwned(existing, existing.currentBlobUri);
          existing.leaseOwner = null;
          existing.leaseEpoch = null;
          existing.leaseExpiresAtMs = 0L;
          clearReadyPointer(existing.readyPointerKey);
          existing.readyPointerKey = null;
          clearDedupeIfOwned(existing);
          return existing;
        });
    var post = get(accountId, jobId);
    if (post.isPresent()
        && ("JS_CANCELLED".equals(post.get().state) || "JS_CANCELLING".equals(post.get().state))) {
      return post;
    }
    return Optional.empty();
  }

  @Override
  public boolean isCancellationRequested(String jobId) {
    var job = get(null, jobId);
    if (job.isEmpty()) {
      return false;
    }
    String state = job.get().state;
    return "JS_CANCELLING".equals(state) || "JS_CANCELLED".equals(state);
  }

  @Override
  public void markCancelled(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    mutateByJobId(
        jobId,
        existing -> {
          if (!hasActiveLease(jobId, leaseEpoch, existing, "markCancelled", true, true)) {
            return null;
          }
          if (isTerminalState(existing.state)) {
            return existing;
          }
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
          clearLaneLeaseIfOwned(existing, existing.currentBlobUri);
          existing.leaseOwner = null;
          existing.leaseEpoch = null;
          existing.leaseExpiresAtMs = 0L;
          clearReadyPointer(existing.readyPointerKey);
          existing.readyPointerKey = null;
          clearDedupeIfOwned(existing);
          return existing;
        });
  }

  private Optional<StoredEnvelope> loadByAnyAccount(String jobId) {
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    Pointer lookup = pointerStore.get(lookupKey).orElse(null);
    if (lookup != null) {
      var rec = readRecordByBlobUri(lookup.getBlobUri());
      if (rec.isPresent()
          && jobId.equals(rec.get().jobId)
          && !rec.get().accountId.isBlank()
          && pointerStore
              .get(Keys.reconcileJobPointerById(rec.get().accountId, rec.get().jobId))
              .isPresent()) {
        return Optional.of(
            new StoredEnvelope(
                Keys.reconcileJobPointerById(rec.get().accountId, rec.get().jobId), rec.get()));
      } else {
        pointerStore.compareAndDelete(lookupKey, lookup.getVersion());
      }
    }

    String token = "";
    int pages = 0;
    LOG.warnf(
        "Reconcile lookup fallback scan engaged for jobId=%s; scanning broad /accounts/ pointer"
            + " prefix",
        jobId);
    while (true) {
      StringBuilder next = new StringBuilder();
      var pointers = pointerStore.listPointersByPrefix("/accounts/", 256, token, next);
      for (Pointer ptr : pointers) {
        if (!ptr.getKey().contains("/reconcile/jobs/by-id/")) {
          continue;
        }
        if (!ptr.getKey().endsWith("/" + urlEncode(jobId))) {
          continue;
        }
        var rec = readRecord(ptr);
        if (rec.isPresent()) {
          return Optional.of(new StoredEnvelope(ptr.getKey(), rec.get()));
        }
      }
      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        return Optional.empty();
      }
      if (nextToken.equals(token)) {
        LOG.warnf(
            "Reconcile lookup fallback pagination token did not advance; aborting fallback scan to"
                + " avoid livelock jobId=%s",
            jobId);
        return Optional.empty();
      }
      token = nextToken;
      pages++;
      if (pages >= FALLBACK_SCAN_MAX_PAGES) {
        LOG.warnf(
            "Reconcile lookup fallback hit safety page cap (%d); aborting scan jobId=%s",
            FALLBACK_SCAN_MAX_PAGES, jobId);
        return Optional.empty();
      }
    }
  }

  private void mutateByJobId(String jobId, UnaryOperator<StoredReconcileJob> mutator) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return;
    }
    mutateByCanonicalPointer(loaded.get().canonicalPointerKey, mutator);
  }

  private void mutateByCanonicalPointer(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(canonicalPointerKey).orElse(null);
      if (currentPointer == null) {
        return;
      }
      var currentOpt = readRecord(currentPointer);
      if (currentOpt.isEmpty()) {
        return;
      }

      StoredReconcileJob current = currentOpt.get();
      current.currentBlobUri = currentPointer.getBlobUri();
      StoredReconcileJob nextRecord = mutator.apply(current);
      if (nextRecord == null) {
        return;
      }

      nextRecord.updatedAtMs = System.currentTimeMillis();
      nextRecord.canonicalPointerKey = canonicalPointerKey;
      String nextBlobUri =
          Keys.reconcileJobBlobUri(
              nextRecord.accountId,
              nextRecord.jobId,
              "v" + (currentPointer.getVersion() + 1) + "-" + UUID.randomUUID());

      try {
        blobStore.put(
            nextBlobUri,
            mapper.writeValueAsBytes(nextRecord),
            "application/json; charset=" + StandardCharsets.UTF_8.name());
      } catch (Exception e) {
        throw new IllegalStateException("Failed to write reconcile job payload", e);
      }

      Pointer nextPointer =
          Pointer.newBuilder()
              .setKey(canonicalPointerKey)
              .setBlobUri(nextBlobUri)
              .setVersion(currentPointer.getVersion() + 1)
              .build();

      if (pointerStore.compareAndSet(
          canonicalPointerKey, currentPointer.getVersion(), nextPointer)) {
        syncIndexPointers(nextRecord, currentPointer.getBlobUri(), nextBlobUri);
        if (!currentPointer.getBlobUri().equals(nextBlobUri)) {
          // The pointer graph is the durable source of truth. Readers are expected to resolve the
          // latest canonical/secondary pointers before dereferencing blobs, so old blob URIs are
          // not stable externally-cacheable addresses once the pointer swap has committed.
          blobStore.delete(currentPointer.getBlobUri());
        }
        return;
      }

      blobStore.delete(nextBlobUri);
    }
  }

  private boolean renewLeaseByCanonicalPointer(
      String canonicalPointerKey, String jobId, String leaseEpoch) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(canonicalPointerKey).orElse(null);
      if (currentPointer == null) {
        return false;
      }
      var currentOpt = readRecord(currentPointer);
      if (currentOpt.isEmpty()) {
        return false;
      }

      StoredReconcileJob current = currentOpt.get();
      if (!hasActiveLease(jobId, leaseEpoch, current, "renewLease", true, false)) {
        return false;
      }
      long now = System.currentTimeMillis();
      long expiry = current.leaseExpiresAtMs;
      if (expiry > 0L && (expiry - now) > (leaseMs / 2L)) {
        return true;
      }
      if (expiry > 0L && now - expiry > leaseRenewGraceMs) {
        LOG.warnf(
            "Skipping renewLease for reconcile job %s due to lease expiry beyond grace now=%d"
                + " expiry=%d graceMs=%d",
            jobId, now, expiry, leaseRenewGraceMs);
        return false;
      }

      current.updatedAtMs = now;
      current.leaseExpiresAtMs = now + leaseMs;
      current.canonicalPointerKey = canonicalPointerKey;

      String nextBlobUri =
          Keys.reconcileJobBlobUri(
              current.accountId,
              current.jobId,
              "renew-" + (currentPointer.getVersion() + 1) + "-" + UUID.randomUUID());
      try {
        blobStore.put(
            nextBlobUri,
            mapper.writeValueAsBytes(current),
            "application/json; charset=" + StandardCharsets.UTF_8.name());
      } catch (Exception e) {
        throw new IllegalStateException("Failed to persist renewed reconcile lease", e);
      }

      Pointer nextPointer =
          Pointer.newBuilder()
              .setKey(canonicalPointerKey)
              .setBlobUri(nextBlobUri)
              .setVersion(currentPointer.getVersion() + 1)
              .build();
      if (pointerStore.compareAndSet(
          canonicalPointerKey, currentPointer.getVersion(), nextPointer)) {
        syncIndexPointers(current, currentPointer.getBlobUri(), nextBlobUri);
        // The pointer graph is the durable source of truth. Readers are expected to resolve the
        // latest canonical/secondary pointers before dereferencing blobs, so old blob URIs are
        // not stable externally-cacheable addresses once the pointer swap has committed.
        blobStore.delete(currentPointer.getBlobUri());
        return true;
      }

      blobStore.delete(nextBlobUri);
    }
    return false;
  }

  private Optional<LeasedJob> leaseCanonical(
      String canonicalPointerKey, String readyPointerKey, long now) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(canonicalPointerKey).orElse(null);
      if (currentPointer == null) {
        return Optional.empty();
      }

      var recordOpt = readRecord(currentPointer);
      if (recordOpt.isEmpty()) {
        return Optional.empty();
      }
      StoredReconcileJob current = recordOpt.get();

      if (isTerminalState(current.state)) {
        clearDedupeIfOwned(current);
        return Optional.empty();
      }

      if ("JS_RUNNING".equals(current.state)
          && current.leaseExpiresAtMs > now
          && current.leaseOwner != null
          && !current.leaseOwner.isBlank()) {
        return Optional.empty();
      }

      boolean cancelling = "JS_CANCELLING".equals(current.state);
      if (!cancelling) {
        current.state = "JS_RUNNING";
        current.message = "Leased";
      }
      if (current.startedAtMs <= 0L) {
        current.startedAtMs = now;
      }
      current.leaseOwner = leaseOwner;
      current.leaseEpoch = UUID.randomUUID().toString();
      current.leaseExpiresAtMs = now + leaseMs;
      if (readyPointerKey != null && readyPointerKey.equals(current.readyPointerKey)) {
        current.readyPointerKey = null;
      }

      current.updatedAtMs = now;
      current.canonicalPointerKey = canonicalPointerKey;

      String nextBlobUri =
          Keys.reconcileJobBlobUri(
              current.accountId,
              current.jobId,
              "lease-" + (currentPointer.getVersion() + 1) + "-" + UUID.randomUUID());
      try {
        blobStore.put(
            nextBlobUri,
            mapper.writeValueAsBytes(current),
            "application/json; charset=" + StandardCharsets.UTF_8.name());
      } catch (Exception e) {
        throw new IllegalStateException("Failed to persist leased reconcile job", e);
      }

      Pointer nextPointer =
          Pointer.newBuilder()
              .setKey(canonicalPointerKey)
              .setBlobUri(nextBlobUri)
              .setVersion(currentPointer.getVersion() + 1)
              .build();

      if (pointerStore.compareAndSet(
          canonicalPointerKey, currentPointer.getVersion(), nextPointer)) {
        syncIndexPointers(current, currentPointer.getBlobUri(), nextBlobUri);
        // The pointer graph is the durable source of truth. Readers are expected to resolve the
        // latest canonical/secondary pointers before dereferencing blobs, so old blob URIs are
        // not stable externally-cacheable addresses once the pointer swap has committed.
        blobStore.delete(currentPointer.getBlobUri());
        return Optional.of(
            new LeasedJob(
                current.jobId,
                current.accountId,
                current.connectorId,
                current.fullRescan,
                current.captureMode(),
                current.toScope(),
                current.executionPolicy(),
                current.leaseEpoch,
                current.pinnedExecutorId(),
                current.executorId(),
                current.jobKind(),
                current.tableTask(),
                current.viewTask(),
                current.parentJobId()));
      }

      blobStore.delete(nextBlobUri);
    }

    return Optional.empty();
  }

  private boolean tryAcquireLaneLease(StoredReconcileJob record, String blobUri, long now) {
    if (record == null
        || record.accountId == null
        || record.accountId.isBlank()
        || record.laneKey == null
        || record.laneKey.isBlank()
        || blobUri == null
        || blobUri.isBlank()) {
      return false;
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer existing = pointerStore.get(lanePointerKey).orElse(null);
      if (existing == null) {
        Pointer created =
            Pointer.newBuilder().setKey(lanePointerKey).setBlobUri(blobUri).setVersion(1L).build();
        if (pointerStore.compareAndSet(lanePointerKey, 0L, created)) {
          return true;
        }
        continue;
      }
      if (blobUri.equals(existing.getBlobUri())) {
        return true;
      }

      var owner = readRecordByBlobUri(existing.getBlobUri());
      if (owner.isPresent() && hasActiveLaneLease(owner.get(), now)) {
        return false;
      }
      if (pointerStore.compareAndDelete(lanePointerKey, existing.getVersion())) {
        continue;
      }
    }
    return false;
  }

  private boolean hasActiveLaneLease(StoredReconcileJob record, long now) {
    if (record == null) {
      return false;
    }
    if (!"JS_RUNNING".equals(record.state) && !"JS_CANCELLING".equals(record.state)) {
      return false;
    }
    if (record.leaseOwner == null || record.leaseOwner.isBlank()) {
      return false;
    }
    return record.leaseExpiresAtMs > now;
  }

  private void clearLaneLeaseIfOwned(StoredReconcileJob record, String expectedBlobUri) {
    if (record == null
        || record.accountId == null
        || record.accountId.isBlank()
        || record.laneKey == null
        || record.laneKey.isBlank()
        || expectedBlobUri == null
        || expectedBlobUri.isBlank()) {
      return;
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    clearPointerIfMatches(lanePointerKey, expectedBlobUri);
  }

  private Optional<StoredReconcileJob> loadActiveFromDedupe(String dedupePointerKey) {
    Pointer dedupePointer = pointerStore.get(dedupePointerKey).orElse(null);
    if (dedupePointer == null) {
      return Optional.empty();
    }

    var recordOpt = readRecordByBlobUri(dedupePointer.getBlobUri());
    if (recordOpt.isEmpty()) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    StoredReconcileJob record = recordOpt.get();
    if (record.accountId == null
        || record.accountId.isBlank()
        || record.jobId == null
        || record.jobId.isBlank()) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    if (pointerStore.get(Keys.reconcileJobPointerById(record.accountId, record.jobId)).isEmpty()) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    if (isTerminalState(record.state)) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    return Optional.of(record);
  }

  private void reclaimExpiredLeasesIfDue(long nowMs) {
    if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
      return;
    }
    if (!reclaimLock.tryLock()) {
      return;
    }
    try {
      if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
        return;
      }
      lastReclaimAtMs = nowMs;

      String token = "";
      int pages = 0;
      while (true) {
        StringBuilder next = new StringBuilder();
        List<Pointer> lookups =
            pointerStore.listPointersByPrefix(
                Keys.reconcileJobLookupPointerByIdPrefix(), 256, token, next);
        for (Pointer lookup : lookups) {
          var recordOpt = readRecordByBlobUri(lookup.getBlobUri());
          if (recordOpt.isEmpty()) {
            pointerStore.compareAndDelete(lookup.getKey(), lookup.getVersion());
            continue;
          }
          StoredReconcileJob existing = recordOpt.get();
          if (existing.accountId == null
              || existing.accountId.isBlank()
              || existing.jobId == null
              || existing.jobId.isBlank()) {
            pointerStore.compareAndDelete(lookup.getKey(), lookup.getVersion());
            continue;
          }
          String canonicalKey = Keys.reconcileJobPointerById(existing.accountId, existing.jobId);

          mutateByCanonicalPointer(
              canonicalKey,
              record -> {
                if (!"JS_RUNNING".equals(record.state) && !"JS_CANCELLING".equals(record.state)) {
                  return null;
                }
                if (record.leaseExpiresAtMs <= 0L || record.leaseExpiresAtMs > nowMs) {
                  return null;
                }

                boolean wasCancelling = "JS_CANCELLING".equals(record.state);
                clearLaneLeaseIfOwned(record, record.currentBlobUri);
                record.state = wasCancelling ? "JS_CANCELLING" : "JS_QUEUED";
                record.message =
                    wasCancelling ? "Lease expired while cancelling" : "Lease expired; requeued";
                if (!wasCancelling) {
                  record.executorId = "";
                }
                record.leaseOwner = null;
                record.leaseEpoch = null;
                record.leaseExpiresAtMs = 0L;
                record.nextAttemptAtMs = nowMs;

                String readyKey =
                    Keys.reconcileReadyPointerByDue(
                        nowMs, record.accountId, record.laneKey, record.jobId);
                clearReadyPointer(record.readyPointerKey);
                if (upsertReadyPointer(readyKey, record.currentBlobUri)) {
                  record.readyPointerKey = readyKey;
                }
                return record;
              });
        }

        String nextToken = next.toString();
        if (nextToken.isBlank()) {
          return;
        }
        if (nextToken.equals(token)) {
          LOG.warn(
              "Reconcile lease reclaim pagination token did not advance; aborting reclaim scan to"
                  + " avoid livelock");
          return;
        }
        token = nextToken;
        pages++;
        if (pages >= 10_000) {
          LOG.warn("Reconcile lease reclaim pagination hit safety page cap; aborting scan");
          return;
        }
      }
    } finally {
      reclaimLock.unlock();
    }
  }

  private Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request) {
    String token = "";
    int pages = 0;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> ready =
          pointerStore.listPointersByPrefix(
              Keys.reconcileReadyPointerPrefix(), readyScanLimit, token, next);
      if (ready.isEmpty()) {
        return Optional.empty();
      }

      for (Pointer candidate : ready) {
        long dueAt = parseDueMillis(candidate.getKey());
        if (dueAt > nowMs) {
          continue;
        }

        var readyTarget = decodeReadyPointerTarget(candidate.getKey());
        if (readyTarget == null) {
          continue;
        }
        if (!matchesLeaseRequest(readyTarget.canonicalPointerKey(), request)) {
          continue;
        }
        Pointer canonicalPointer = pointerStore.get(readyTarget.canonicalPointerKey()).orElse(null);
        if (canonicalPointer == null) {
          continue;
        }
        var recordOpt = readRecord(canonicalPointer);
        if (recordOpt.isEmpty()) {
          continue;
        }
        if (!tryAcquireLaneLease(recordOpt.get(), canonicalPointer.getBlobUri(), nowMs)) {
          continue;
        }
        if (!pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion())) {
          clearLaneLeaseIfOwned(recordOpt.get(), canonicalPointer.getBlobUri());
          continue;
        }
        var leased = leaseCanonical(readyTarget.canonicalPointerKey(), candidate.getKey(), nowMs);
        if (leased.isPresent()) {
          return leased;
        }
        clearLaneLeaseIfOwned(recordOpt.get(), canonicalPointer.getBlobUri());
      }

      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        return Optional.empty();
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile ready pagination token did not advance; aborting ready scan to avoid"
                + " livelock");
        return Optional.empty();
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile ready pagination hit safety page cap; aborting scan");
        return Optional.empty();
      }
    }
  }

  private Optional<StoredReconcileJob> readRecord(Pointer canonicalPointer) {
    var record = readRecordByBlobUri(canonicalPointer.getBlobUri());
    if (record.isEmpty()) {
      return Optional.empty();
    }
    record.get().canonicalPointerKey = canonicalPointer.getKey();
    return record;
  }

  private boolean matchesLeaseRequest(String canonicalPointerKey, LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      return false;
    }
    var record = readRecord(canonicalPointer);
    return record.isPresent()
        && effective.matches(
            record.get().executionPolicy(),
            record.get().pinnedExecutorId(),
            record.get().jobKind());
  }

  private ReconcileJob toPublicJob(StoredReconcileJob stored) {
    return new ReconcileJob(
        stored.jobId,
        stored.accountId,
        stored.connectorId,
        stored.state,
        stored.message == null ? "" : stored.message,
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
        stored.toScope(),
        stored.executionPolicy(),
        stored.executorId(),
        stored.jobKind(),
        stored.tableTask(),
        stored.viewTask(),
        stored.parentJobId());
  }

  private boolean upsertReadyPointer(String readyPointerKey, String blobUri) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer existing = pointerStore.get(readyPointerKey).orElse(null);
      if (existing == null) {
        Pointer created =
            Pointer.newBuilder().setKey(readyPointerKey).setBlobUri(blobUri).setVersion(1L).build();
        if (pointerStore.compareAndSet(readyPointerKey, 0L, created)) {
          return true;
        }
        continue;
      }

      Pointer next =
          Pointer.newBuilder()
              .setKey(readyPointerKey)
              .setBlobUri(blobUri)
              .setVersion(existing.getVersion() + 1)
              .build();
      if (pointerStore.compareAndSet(readyPointerKey, existing.getVersion(), next)) {
        return true;
      }
    }

    return false;
  }

  private void clearReadyPointer(String readyPointerKey) {
    if (readyPointerKey == null || readyPointerKey.isBlank()) {
      return;
    }
    Pointer existing = pointerStore.get(readyPointerKey).orElse(null);
    if (existing != null) {
      pointerStore.compareAndDelete(readyPointerKey, existing.getVersion());
    }
  }

  private void clearDedupeIfOwned(StoredReconcileJob record) {
    if (record.dedupeKey == null || record.dedupeKey.isBlank()) {
      return;
    }
    String dedupeKey = Keys.reconcileDedupePointer(record.accountId, hashValue(record.dedupeKey));
    Pointer existing = pointerStore.get(dedupeKey).orElse(null);
    if (existing == null) {
      return;
    }
    var owner = readRecordByBlobUri(existing.getBlobUri());
    if (owner.isPresent()
        && record.jobId.equals(owner.get().jobId)
        && record.accountId.equals(owner.get().accountId)) {
      pointerStore.compareAndDelete(dedupeKey, existing.getVersion());
    }
  }

  private void clearDedupeReservation(String dedupePointerKey, String expectedBlobUri) {
    clearPointerIfMatches(dedupePointerKey, expectedBlobUri);
  }

  private void clearPointerIfMatches(String pointerKey, String expectedBlobUri) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return;
    }
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (expectedBlobUri != null
        && !expectedBlobUri.isBlank()
        && !expectedBlobUri.equals(existing.getBlobUri())) {
      return;
    }
    pointerStore.compareAndDelete(pointerKey, existing.getVersion());
  }

  private Optional<StoredReconcileJob> readRecordByBlobUri(String blobUri) {
    byte[] payload;
    try {
      payload = blobStore.get(blobUri);
    } catch (StorageNotFoundException e) {
      LOG.warnf(
          e, "Reconcile job blob missing during reclaim, treating as absent blob=%s", blobUri);
      return Optional.empty();
    }
    if (payload == null || payload.length == 0) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(mapper.readValue(payload, StoredReconcileJob.class));
    } catch (Exception e) {
      LOG.warnf(e, "Failed to decode reconcile job payload blob=%s", blobUri);
      return Optional.empty();
    }
  }

  private void syncIndexPointers(StoredReconcileJob record, String oldBlobUri, String newBlobUri) {
    if (record == null
        || oldBlobUri == null
        || oldBlobUri.isBlank()
        || newBlobUri == null
        || newBlobUri.isBlank()
        || oldBlobUri.equals(newBlobUri)) {
      return;
    }
    replacePointerBlobUriIfMatch(
        Keys.reconcileJobLookupPointerById(record.jobId), oldBlobUri, newBlobUri);
    String parentPointerKey =
        parentPointerKey(record.accountId, record.parentJobId(), record.jobId);
    if (!parentPointerKey.isBlank()) {
      replacePointerBlobUriIfMatch(parentPointerKey, oldBlobUri, newBlobUri);
    }
    if (record.readyPointerKey != null && !record.readyPointerKey.isBlank()) {
      replacePointerBlobUriIfMatch(record.readyPointerKey, oldBlobUri, newBlobUri);
    }
    if (hasActiveLaneLease(record, System.currentTimeMillis())) {
      replacePointerBlobUriIfMatch(
          Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey), oldBlobUri, newBlobUri);
    }
    if (record.dedupeKey != null && !record.dedupeKey.isBlank() && !isTerminalState(record.state)) {
      String dedupePointer =
          Keys.reconcileDedupePointer(record.accountId, hashValue(record.dedupeKey));
      replacePointerBlobUriIfMatch(dedupePointer, oldBlobUri, newBlobUri);
    }
  }

  private void replacePointerBlobUriIfMatch(
      String pointerKey, String expectedBlobUri, String nextBlobUri) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer existing = pointerStore.get(pointerKey).orElse(null);
      if (existing == null) {
        return;
      }
      if (nextBlobUri.equals(existing.getBlobUri())) {
        return;
      }
      if (!expectedBlobUri.equals(existing.getBlobUri())) {
        return;
      }
      Pointer next =
          Pointer.newBuilder()
              .setKey(pointerKey)
              .setBlobUri(nextBlobUri)
              .setVersion(existing.getVersion() + 1L)
              .build();
      if (pointerStore.compareAndSet(pointerKey, existing.getVersion(), next)) {
        return;
      }
    }
  }

  private ReadyPointerTarget decodeReadyPointerTarget(String readyPointerKey) {
    String normalizedKey = normalizePointerKey(readyPointerKey);
    String prefix = normalizePointerKey(Keys.reconcileReadyPointerPrefix());
    if (!normalizedKey.startsWith(prefix)) {
      return null;
    }
    String[] parts = normalizedKey.substring(prefix.length()).split("/");
    if (parts.length < 4) {
      return null;
    }
    try {
      String accountId = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
      String jobId = URLDecoder.decode(parts[3], StandardCharsets.UTF_8);
      return new ReadyPointerTarget(Keys.reconcileJobPointerById(accountId, jobId));
    } catch (Exception e) {
      return null;
    }
  }

  private long parseDueMillis(String readyPointerKey) {
    if (readyPointerKey == null || readyPointerKey.isBlank()) {
      return Long.MAX_VALUE;
    }
    String normalizedKey = normalizePointerKey(readyPointerKey);
    String prefix = normalizePointerKey(Keys.reconcileReadyPointerPrefix());
    if (!normalizedKey.startsWith(prefix)) {
      return Long.MAX_VALUE;
    }
    int slash = normalizedKey.indexOf('/', prefix.length());
    if (slash < 0) {
      return Long.MAX_VALUE;
    }

    String token = normalizedKey.substring(prefix.length(), slash);
    try {
      return Long.parseLong(token);
    } catch (NumberFormatException nfe) {
      return Long.MAX_VALUE;
    }
  }

  private static String normalizePointerKey(String key) {
    if (key == null || key.isBlank()) {
      return "/";
    }
    return key.startsWith("/") ? key : "/" + key;
  }

  private long backoffMs(int attempts) {
    long base = baseBackoffMs * (1L << Math.min(8, Math.max(0, attempts - 1)));
    return Math.min(maxBackoffMs, base);
  }

  private boolean hasActiveLease(
      String jobId,
      String leaseEpoch,
      StoredReconcileJob existing,
      String op,
      boolean allowCancelling,
      boolean requireUnexpiredLease) {
    if (leaseEpoch == null || leaseEpoch.isBlank()) {
      logLeaseSkip(op, "Skipping %s for reconcile job %s due to missing lease epoch", op, jobId);
      return false;
    }
    boolean stateAllowed =
        "JS_RUNNING".equals(existing.state)
            || (allowCancelling && "JS_CANCELLING".equals(existing.state));
    if (!stateAllowed) {
      logLeaseSkip(
          op,
          "Skipping %s for reconcile job %s due to non-running state=%s",
          op,
          jobId,
          existing.state);
      return false;
    }
    long now = System.currentTimeMillis();
    if (requireUnexpiredLease && existing.leaseExpiresAtMs <= now) {
      logLeaseSkip(
          op,
          "Skipping %s for reconcile job %s due to expired lease expiresAtMs=%d now=%d",
          op,
          jobId,
          existing.leaseExpiresAtMs,
          now);
      return false;
    }
    if (!leaseOwner.equals(existing.leaseOwner) || !leaseEpoch.equals(existing.leaseEpoch)) {
      logLeaseSkip(
          op,
          "Skipping %s for reconcile job %s due to stale lease (owner=%s epoch=%s)",
          op,
          jobId,
          existing.leaseOwner,
          existing.leaseEpoch);
      return false;
    }
    return true;
  }

  private void logLeaseSkip(String op, String format, Object... args) {
    if ("markProgress".equals(op)) {
      LOG.debugf(format, args);
    } else {
      LOG.warnf(format, args);
    }
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

  private static ReconcileScope normalizeScopeForJobKind(
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask) {
    ReconcileScope effectiveScope = scope == null ? ReconcileScope.empty() : scope;
    if (jobKind == ReconcileJobKind.EXEC_TABLE
        && tableTask != null
        && tableTask.strict()
        && !blank(tableTask.destinationTableId())) {
      if (effectiveScope.hasTableFilter()
          && !tableTask.destinationTableId().equals(effectiveScope.destinationTableId())) {
        throw new IllegalArgumentException(
            "table task destinationTableId does not match scope destinationTableId");
      }
      if (effectiveScope.hasViewFilter() || effectiveScope.hasNamespaceFilter()) {
        throw new IllegalArgumentException(
            "table task destinationTableId cannot be combined with namespace or view scope");
      }
      return effectiveScope.hasTableFilter()
          ? effectiveScope
          : ReconcileScope.of(
              List.of(), tableTask.destinationTableId(), effectiveScope.destinationStatsRequests());
    }
    if (jobKind == ReconcileJobKind.EXEC_VIEW
        && viewTask != null
        && viewTask.strict()
        && !blank(viewTask.destinationViewId())) {
      if (effectiveScope.hasViewFilter()
          && !viewTask.destinationViewId().equals(effectiveScope.destinationViewId())) {
        throw new IllegalArgumentException(
            "view task destinationViewId does not match scope destinationViewId");
      }
      if (effectiveScope.hasNamespaceFilter()
          && !effectiveScope
              .destinationNamespaceIds()
              .contains(viewTask.destinationNamespaceId())) {
        throw new IllegalArgumentException(
            "view task destinationNamespaceId does not match scope destinationNamespaceIds");
      }
      if (effectiveScope.hasTableFilter() || effectiveScope.hasStatsRequestFilter()) {
        throw new IllegalArgumentException(
            "view task destinationViewId cannot be combined with table or stats scope");
      }
      return effectiveScope.hasViewFilter()
          ? effectiveScope
          : ReconcileScope.ofView(List.of(), viewTask.destinationViewId());
    }
    return effectiveScope;
  }

  private static String laneKey(
      String connectorId,
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask) {
    String namespaces =
        scope.destinationNamespaceIds().stream().sorted().reduce((a, b) -> a + "," + b).orElse("*");
    if (jobKind == ReconcileJobKind.EXEC_TABLE && tableTask != null) {
      return scope.destinationTableId() == null || scope.destinationTableId().isBlank()
          ? "tables|" + namespaces
          : "table|" + scope.destinationTableId();
    }
    if (jobKind == ReconcileJobKind.EXEC_VIEW && viewTask != null) {
      return scope.destinationViewId() == null || scope.destinationViewId().isBlank()
          ? "views|" + namespaces
          : "view|" + scope.destinationViewId();
    }
    String resource =
        scope.destinationTableId() != null
            ? scope.destinationTableId()
            : (scope.destinationViewId() == null ? "*" : scope.destinationViewId());
    return namespaces + "|" + resource;
  }

  private static String dedupeKey(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    String namespaces =
        scope.destinationNamespaceIds().stream().sorted().reduce((a, b) -> a + "," + b).orElse("*");
    String table = scope.destinationTableId() == null ? "*" : scope.destinationTableId();
    String statsRequests =
        scope.destinationStatsRequests().stream()
            .map(DurableReconcileJobStore::canonicalStatsRequest)
            .sorted()
            .reduce((a, b) -> a + "," + b)
            .orElse("");
    String canonicalTableDisplayName =
        tableTask != null && tableTask.strict() && !blank(tableTask.destinationTableId())
            ? ""
            : (tableTask == null ? "" : tableTask.destinationTableDisplayName());
    String canonicalViewDisplayName =
        viewTask != null && viewTask.strict() && !blank(viewTask.destinationViewId())
            ? ""
            : (viewTask == null ? "" : viewTask.destinationViewDisplayName());
    ReconcileExecutionPolicy policy =
        executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
    Canonicalizer canonicalizer = new Canonicalizer();
    canonicalizer
        .scalar("account_id", accountId)
        .scalar("connector_id", connectorId)
        .scalar(
            "job_kind", (jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR.name() : jobKind.name()))
        .scalar("full_rescan", fullRescan)
        .scalar(
            "capture_mode",
            captureMode == null ? CaptureMode.METADATA_AND_STATS.name() : captureMode.name())
        .scalar("table_task.source_namespace", tableTask == null ? "" : tableTask.sourceNamespace())
        .scalar("table_task.source_table", tableTask == null ? "" : tableTask.sourceTable())
        .scalar(
            "table_task.destination_table_id",
            tableTask == null ? "" : blankToEmpty(tableTask.destinationTableId()))
        .scalar(
            "table_task.destination_namespace_id",
            tableTask == null ? "" : tableTask.destinationNamespaceId())
        .scalar("table_task.destination_table_display_name", canonicalTableDisplayName)
        .scalar("table_task.mode", tableTask == null ? "" : tableTask.mode().name())
        .scalar("view_task.source_namespace", viewTask == null ? "" : viewTask.sourceNamespace())
        .scalar("view_task.source_view", viewTask == null ? "" : viewTask.sourceView())
        .scalar(
            "view_task.destination_namespace_id",
            viewTask == null ? "" : viewTask.destinationNamespaceId())
        .scalar(
            "view_task.destination_view_id",
            viewTask == null ? "" : blankToEmpty(viewTask.destinationViewId()))
        .scalar("view_task.destination_view_display_name", canonicalViewDisplayName)
        .scalar("view_task.mode", viewTask == null ? "" : viewTask.mode().name())
        .scalar("scope.namespaces", namespaces)
        .scalar("scope.table", table)
        .scalar("scope.view", scope.destinationViewId() == null ? "" : scope.destinationViewId())
        .scalar("scope.stats_requests", statsRequests)
        .scalar("policy.execution_class", policy.executionClass().name())
        .scalar("policy.lane", policy.lane())
        .map("policy.attributes", policy.attributes())
        .scalar("parent_job_id", parentJobId == null ? "" : parentJobId.trim())
        .scalar("pinned_executor_id", pinnedExecutorId == null ? "" : pinnedExecutorId.trim());
    return new String(canonicalizer.bytes(), StandardCharsets.UTF_8);
  }

  private static ListCursor decodeListCursor(String pageToken) {
    if (pageToken == null || pageToken.isBlank()) {
      return new ListCursor("", 0);
    }
    if (!pageToken.startsWith(LIST_TOKEN_V1_PREFIX)) {
      return new ListCursor(pageToken, 0);
    }
    try {
      String decoded =
          new String(
              Base64.getUrlDecoder().decode(pageToken.substring(LIST_TOKEN_V1_PREFIX.length())),
              StandardCharsets.UTF_8);
      int separator = decoded.indexOf('\n');
      if (separator < 0) {
        return new ListCursor(decoded, 0);
      }
      String storeToken = decoded.substring(0, separator);
      int skip = Integer.parseInt(decoded.substring(separator + 1));
      return new ListCursor(storeToken, Math.max(0, skip));
    } catch (RuntimeException e) {
      return new ListCursor(pageToken, 0);
    }
  }

  private static String encodeListCursor(String storeToken, int skip) {
    if (skip <= 0) {
      return storeToken == null ? "" : storeToken;
    }
    String payload = (storeToken == null ? "" : storeToken) + "\n" + skip;
    return LIST_TOKEN_V1_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }

  private static String hashValue(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] payload = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest(payload));
    } catch (Exception e) {
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
    }
  }

  private static String canonicalStatsRequest(ReconcileScope.ScopedStatsRequest request) {
    if (request == null) {
      return "";
    }
    String selectors =
        request.columnSelectors().stream().sorted().reduce((a, b) -> a + "," + b).orElse("");
    return request.tableId()
        + "|"
        + request.snapshotId()
        + "|"
        + request.targetSpec()
        + "|"
        + selectors;
  }

  private static String urlEncode(String value) {
    if (value == null || value.isBlank()) {
      return "";
    }
    return Keys.encodeSegment(value);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static String parentPointerKey(String accountId, String parentJobId, String jobId) {
    if (accountId == null
        || accountId.isBlank()
        || parentJobId == null
        || parentJobId.isBlank()
        || jobId == null
        || jobId.isBlank()) {
      return "";
    }
    return Keys.reconcileJobByParentPointer(accountId, parentJobId, jobId);
  }

  private static final class StoredEnvelope {
    final String canonicalPointerKey;
    final StoredReconcileJob record;

    private StoredEnvelope(String canonicalPointerKey, StoredReconcileJob record) {
      this.canonicalPointerKey = canonicalPointerKey;
      this.record = record;
    }
  }

  private record ReadyPointerTarget(String canonicalPointerKey) {}

  private record ListCursor(String storeToken, int skip) {}

  static final class StoredReconcileJob {
    public String jobId;
    public String accountId;
    public String connectorId;
    public String jobKind;
    public String parentJobId;
    public boolean fullRescan;
    public String captureMode;
    public String executionClass;
    public String executionLane;
    public java.util.Map<String, String> executionAttributes = java.util.Map.of();
    public String pinnedExecutorId;
    public String executorId;
    public String sourceNamespace;
    public String sourceTable;
    public String taskMode;
    public String taskDestinationNamespaceId;
    public String taskDestinationTableId;
    public String taskDestinationTableDisplayName;
    public String sourceView;
    public String taskDestinationViewId;
    public String taskDestinationViewDisplayName;
    public String destinationTableId;
    public String destinationViewId;
    public List<String> destinationNamespaceIds = List.of();
    public List<ReconcileScope.ScopedStatsRequest> destinationStatsRequests = List.of();
    public String state;
    public String message;
    public long startedAtMs;
    public long finishedAtMs;
    public long tablesScanned;
    public long tablesChanged;
    public long viewsScanned;
    public long viewsChanged;
    public long errors;
    public long snapshotsProcessed;
    public long statsProcessed;

    public int attempt;
    public long nextAttemptAtMs;
    public String leaseOwner;
    public String leaseEpoch;
    public long leaseExpiresAtMs;
    public String lastError;

    public String laneKey;
    public String dedupeKey;
    public String readyPointerKey;
    public String canonicalPointerKey;
    public String currentBlobUri;

    public long createdAtMs;
    public long updatedAtMs;

    static StoredReconcileJob queued(
        String jobId,
        String accountId,
        String connectorId,
        boolean fullRescan,
        CaptureMode captureMode,
        ReconcileScope scope,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        ReconcileExecutionPolicy executionPolicy,
        String parentJobId,
        String pinnedExecutorId,
        String laneKey,
        String dedupeKey,
        long now,
        String readyPointerKey) {
      StoredReconcileJob rec = new StoredReconcileJob();
      rec.jobId = jobId;
      rec.accountId = accountId;
      rec.connectorId = connectorId;
      rec.jobKind = (jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind).name();
      rec.parentJobId = parentJobId == null ? "" : parentJobId.trim();
      rec.fullRescan = fullRescan;
      rec.captureMode = (captureMode == null ? CaptureMode.METADATA_AND_STATS : captureMode).name();
      ReconcileExecutionPolicy policy =
          executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
      rec.executionClass = policy.executionClass().name();
      rec.executionLane = policy.lane();
      rec.executionAttributes = policy.attributes();
      rec.pinnedExecutorId = pinnedExecutorId == null ? "" : pinnedExecutorId.trim();
      rec.executorId = "";
      ReconcileTableTask effectiveTask = tableTask == null ? ReconcileTableTask.empty() : tableTask;
      ReconcileViewTask effectiveViewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
      rec.sourceNamespace =
          jobKind == ReconcileJobKind.EXEC_VIEW
              ? effectiveViewTask.sourceNamespace()
              : effectiveTask.sourceNamespace();
      rec.sourceTable = effectiveTask.sourceTable();
      rec.taskMode =
          jobKind == ReconcileJobKind.EXEC_VIEW
              ? effectiveViewTask.mode().name()
              : effectiveTask.mode().name();
      rec.taskDestinationNamespaceId =
          jobKind == ReconcileJobKind.EXEC_VIEW
              ? effectiveViewTask.destinationNamespaceId()
              : effectiveTask.destinationNamespaceId();
      rec.taskDestinationTableId = blankToEmpty(effectiveTask.destinationTableId());
      rec.taskDestinationTableDisplayName = effectiveTask.destinationTableDisplayName();
      rec.sourceView = effectiveViewTask.sourceView();
      rec.taskDestinationViewId = blankToEmpty(effectiveViewTask.destinationViewId());
      rec.taskDestinationViewDisplayName = effectiveViewTask.destinationViewDisplayName();
      rec.destinationNamespaceIds = scope.destinationNamespaceIds();
      rec.destinationTableId = scope.destinationTableId();
      rec.destinationViewId = scope.destinationViewId();
      rec.destinationStatsRequests = scope.destinationStatsRequests();
      rec.state = "JS_QUEUED";
      rec.message = fullRescan ? "Queued (full)" : "Queued";
      rec.nextAttemptAtMs = now;
      rec.attempt = 0;
      rec.laneKey = laneKey;
      rec.dedupeKey = dedupeKey;
      rec.readyPointerKey = readyPointerKey;
      rec.createdAtMs = now;
      rec.updatedAtMs = now;
      return rec;
    }

    CaptureMode captureMode() {
      if (captureMode == null || captureMode.isBlank()) {
        return CaptureMode.METADATA_AND_STATS;
      }
      try {
        return CaptureMode.valueOf(captureMode);
      } catch (IllegalArgumentException ignored) {
        return CaptureMode.METADATA_AND_STATS;
      }
    }

    ReconcileScope toScope() {
      return ReconcileScope.of(
          destinationNamespaceIds, destinationTableId, destinationViewId, destinationStatsRequests);
    }

    ReconcileJobKind jobKind() {
      return ReconcileJobKind.fromString(jobKind);
    }

    ReconcileTableTask tableTask() {
      if (ReconcileTableTask.Mode.DISCOVERY.name().equals(taskMode)) {
        return ReconcileTableTask.discovery(
            sourceNamespace,
            sourceTable,
            taskDestinationNamespaceId,
            blankToNull(taskDestinationTableId),
            taskDestinationTableDisplayName);
      }
      return ReconcileTableTask.of(
          sourceNamespace,
          sourceTable,
          taskDestinationNamespaceId,
          taskDestinationTableId,
          taskDestinationTableDisplayName);
    }

    ReconcileViewTask viewTask() {
      if (ReconcileViewTask.Mode.DISCOVERY.name().equals(taskMode)) {
        return ReconcileViewTask.discovery(
            sourceNamespace,
            sourceView,
            taskDestinationNamespaceId,
            blankToNull(taskDestinationViewId),
            taskDestinationViewDisplayName);
      }
      return ReconcileViewTask.of(
          sourceNamespace,
          sourceView,
          taskDestinationNamespaceId,
          taskDestinationViewId,
          taskDestinationViewDisplayName);
    }

    ReconcileExecutionPolicy executionPolicy() {
      return ReconcileExecutionPolicy.of(
          ReconcileExecutionClass.fromString(executionClass), executionLane, executionAttributes);
    }

    String pinnedExecutorId() {
      return pinnedExecutorId == null ? "" : pinnedExecutorId;
    }

    String executorId() {
      return executorId == null ? "" : executorId;
    }

    String parentJobId() {
      return parentJobId == null ? "" : parentJobId;
    }
  }
}
