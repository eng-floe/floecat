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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.repo.model.Keys;
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
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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
  private static final int DEFAULT_READY_SCAN_LIMIT = 128;
  private static final int CAS_MAX = 16;

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject ObjectMapper mapper;
  @Inject Config config;

  private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
  private long baseBackoffMs = DEFAULT_BASE_BACKOFF_MS;
  private long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;
  private long leaseMs = DEFAULT_LEASE_MS;
  private long reclaimIntervalMs = DEFAULT_RECLAIM_INTERVAL_MS;
  private int readyScanLimit = DEFAULT_READY_SCAN_LIMIT;

  private final String leaseOwner = "reconcile-store-" + UUID.randomUUID();
  private volatile long lastReclaimAtMs;

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
    readyScanLimit =
        Math.max(
            1,
            config
                .getOptionalValue("floecat.reconciler.job-store.ready-scan-limit", Integer.class)
                .orElse(DEFAULT_READY_SCAN_LIMIT));
  }

  @Override
  public String enqueue(
      String accountId, String connectorId, boolean fullRescan, ReconcileScope incomingScope) {
    ReconcileScope scope = incomingScope == null ? ReconcileScope.empty() : incomingScope;
    String laneKey = laneKey(connectorId, scope);
    String dedupeKey = dedupeKey(accountId, connectorId, fullRescan, scope);
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
              jobId, accountId, connectorId, fullRescan, scope, laneKey, dedupeKey, now, readyKey);

      try {
        blobStore.put(
            blobUri,
            mapper.writeValueAsBytes(record),
            "application/json; charset=" + StandardCharsets.UTF_8.name());
      } catch (Exception e) {
        pointerStore.compareAndDelete(dedupePointerKey, 1L);
        throw new IllegalStateException("Failed to persist reconcile job payload", e);
      }

      Pointer canonical =
          Pointer.newBuilder().setKey(canonicalKey).setBlobUri(blobUri).setVersion(1L).build();
      if (!pointerStore.compareAndSet(canonicalKey, 0L, canonical)) {
        pointerStore.compareAndDelete(dedupePointerKey, 1L);
        blobStore.delete(blobUri);
        continue;
      }

      Pointer lookup =
          Pointer.newBuilder().setKey(lookupKey).setBlobUri(blobUri).setVersion(1L).build();
      if (!pointerStore.compareAndSet(lookupKey, 0L, lookup)) {
        pointerStore.compareAndDelete(canonicalKey, 1L);
        pointerStore.compareAndDelete(dedupePointerKey, 1L);
        blobStore.delete(blobUri);
        continue;
      }

      Pointer ready =
          Pointer.newBuilder().setKey(readyKey).setBlobUri(blobUri).setVersion(1L).build();
      if (!pointerStore.compareAndSet(readyKey, 0L, ready)) {
        pointerStore.compareAndDelete(lookupKey, 1L);
        pointerStore.compareAndDelete(canonicalKey, 1L);
        pointerStore.compareAndDelete(dedupePointerKey, 1L);
        blobStore.delete(blobUri);
        continue;
      }

      return jobId;
    }

    throw new IllegalStateException("Unable to enqueue reconcile job after CAS retries");
  }

  @Override
  public Optional<ReconcileJob> get(String jobId) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(toPublicJob(loaded.get().record));
  }

  @Override
  public Optional<LeasedJob> leaseNext() {
    long now = System.currentTimeMillis();
    var leased = leaseReadyDue(now);
    if (leased.isPresent()) {
      return leased;
    }

    // Reclaim is best-effort recovery work; do not block ready leasing on potentially expensive
    // scans.
    reclaimExpiredLeasesIfDue(now);
    return leaseReadyDue(System.currentTimeMillis());
  }

  @Override
  public void markRunning(String jobId, long startedAtMs) {
    mutateByJobId(
        jobId,
        existing -> {
          existing.state = "JS_RUNNING";
          existing.message = "Running";
          if (existing.startedAtMs <= 0L) {
            existing.startedAtMs = startedAtMs;
          }
          return existing;
        });
  }

  @Override
  public void markProgress(String jobId, long scanned, long changed, long errors, String message) {
    mutateByJobId(
        jobId,
        existing -> {
          existing.tablesScanned = scanned;
          existing.tablesChanged = changed;
          existing.errors = errors;
          if (message != null && !message.isBlank()) {
            existing.message = message;
          }
          return existing;
        });
  }

  @Override
  public void markSucceeded(String jobId, long finishedAtMs, long scanned, long changed) {
    mutateByJobId(
        jobId,
        existing -> {
          existing.state = "JS_SUCCEEDED";
          existing.message = "Succeeded";
          if (existing.startedAtMs <= 0L) {
            existing.startedAtMs = finishedAtMs;
          }
          existing.finishedAtMs = finishedAtMs;
          existing.tablesScanned = scanned;
          existing.tablesChanged = changed;
          existing.leaseOwner = null;
          existing.leaseExpiresAtMs = 0L;
          clearReadyPointer(existing.readyPointerKey);
          existing.readyPointerKey = null;
          clearDedupeIfOwned(existing);
          return existing;
        });
  }

  @Override
  public void markFailed(
      String jobId, long finishedAtMs, String message, long scanned, long changed, long errors) {
    mutateByJobId(
        jobId,
        existing -> {
          existing.attempt = Math.max(0, existing.attempt) + 1;
          existing.tablesScanned = scanned;
          existing.tablesChanged = changed;
          existing.errors = errors;
          existing.lastError = message == null ? "Failed" : message;
          existing.leaseOwner = null;
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
    while (true) {
      StringBuilder next = new StringBuilder();
      var pointers = pointerStore.listPointersByPrefix("/accounts/", 256, token, next);
      for (Pointer ptr : pointers) {
        if (!ptr.getKey().contains("/reconcile/jobs/by-id/")) {
          continue;
        }
        if (!ptr.getKey().endsWith("/" + urlEncode(jobId))
            && !ptr.getKey().endsWith(urlEncode(jobId))) {
          continue;
        }
        var rec = readRecord(ptr);
        if (rec.isPresent()) {
          return Optional.of(new StoredEnvelope(ptr.getKey(), rec.get()));
        }
      }
      token = next.toString();
      if (token.isBlank()) {
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
          blobStore.delete(currentPointer.getBlobUri());
        }
        return;
      }

      blobStore.delete(nextBlobUri);
    }
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

      current.state = "JS_RUNNING";
      current.message = "Leased";
      current.leaseOwner = leaseOwner;
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
        blobStore.delete(currentPointer.getBlobUri());
        return Optional.of(
            new LeasedJob(
                current.jobId,
                current.accountId,
                current.connectorId,
                current.fullRescan,
                current.toScope()));
      }

      blobStore.delete(nextBlobUri);
    }

    return Optional.empty();
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
              if (!"JS_RUNNING".equals(record.state)) {
                return null;
              }
              if (record.leaseExpiresAtMs <= 0L || record.leaseExpiresAtMs > nowMs) {
                return null;
              }

              record.state = "JS_QUEUED";
              record.message = "Lease expired; requeued";
              record.leaseOwner = null;
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
            "Reconcile lease reclaim pagination token did not advance; aborting reclaim scan to avoid livelock");
        return;
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile lease reclaim pagination hit safety page cap; aborting scan");
        return;
      }
    }
  }

  private Optional<LeasedJob> leaseReadyDue(long nowMs) {
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
          return Optional.empty();
        }

        if (!pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion())) {
          continue;
        }

        var readyTarget = decodeReadyPointerTarget(candidate.getKey());
        if (readyTarget == null) {
          continue;
        }
        var leased = leaseCanonical(readyTarget.canonicalPointerKey(), candidate.getKey(), nowMs);
        if (leased.isPresent()) {
          return leased;
        }
      }

      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        return Optional.empty();
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile ready pagination token did not advance; aborting ready scan to avoid livelock");
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
        stored.errors,
        stored.fullRescan,
        stored.toScope());
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

  private Optional<StoredReconcileJob> readRecordByBlobUri(String blobUri) {
    byte[] payload = blobStore.get(blobUri);
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
    if (record.readyPointerKey != null && !record.readyPointerKey.isBlank()) {
      replacePointerBlobUriIfMatch(record.readyPointerKey, oldBlobUri, newBlobUri);
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

  private static boolean isTerminalState(String state) {
    if (state == null) {
      return false;
    }
    return switch (state) {
      case "JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED" -> true;
      default -> false;
    };
  }

  private static String laneKey(String connectorId, ReconcileScope scope) {
    String namespace =
        scope.destinationNamespacePaths().isEmpty()
            ? "*"
            : String.join(".", scope.destinationNamespacePaths().get(0));
    String table =
        scope.destinationTableDisplayName() == null ? "*" : scope.destinationTableDisplayName();
    return connectorId + "|" + namespace + "|" + table;
  }

  private static String dedupeKey(
      String accountId, String connectorId, boolean fullRescan, ReconcileScope scope) {
    String namespaces =
        scope.destinationNamespacePaths().stream()
            .map(path -> String.join(".", path))
            .sorted()
            .reduce((a, b) -> a + "," + b)
            .orElse("*");
    String table =
        scope.destinationTableDisplayName() == null ? "*" : scope.destinationTableDisplayName();
    String columns =
        scope.destinationTableColumns().stream().sorted().reduce((a, b) -> a + "," + b).orElse("");
    return accountId
        + "|"
        + connectorId
        + "|"
        + (fullRescan ? "full" : "incr")
        + "|"
        + namespaces
        + "|"
        + table
        + "|"
        + columns;
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

  private static String urlEncode(String value) {
    if (value == null || value.isBlank()) {
      return "";
    }
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
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

  static final class StoredReconcileJob {
    public String jobId;
    public String accountId;
    public String connectorId;
    public boolean fullRescan;

    public List<List<String>> destinationNamespacePaths = List.of();
    public String destinationTableDisplayName;
    public List<String> destinationTableColumns = List.of();

    public String state;
    public String message;
    public long startedAtMs;
    public long finishedAtMs;
    public long tablesScanned;
    public long tablesChanged;
    public long errors;

    public int attempt;
    public long nextAttemptAtMs;
    public String leaseOwner;
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
        ReconcileScope scope,
        String laneKey,
        String dedupeKey,
        long now,
        String readyPointerKey) {
      StoredReconcileJob rec = new StoredReconcileJob();
      rec.jobId = jobId;
      rec.accountId = accountId;
      rec.connectorId = connectorId;
      rec.fullRescan = fullRescan;
      rec.destinationNamespacePaths = scope.destinationNamespacePaths();
      rec.destinationTableDisplayName = scope.destinationTableDisplayName();
      rec.destinationTableColumns = scope.destinationTableColumns();
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

    ReconcileScope toScope() {
      return ReconcileScope.of(
          destinationNamespacePaths, destinationTableDisplayName, destinationTableColumns);
    }
  }
}
