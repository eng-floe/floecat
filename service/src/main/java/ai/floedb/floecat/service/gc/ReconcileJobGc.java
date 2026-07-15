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

package ai.floedb.floecat.service.gc;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.reconciler.jobs.ReconcilerSettingsStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobListSummary;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexEntrySnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueBackend;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class ReconcileJobGc {

  private static final String INLINE_JOB_STATE_PREFIX = "inline:reconcile-job:";
  private static final String INLINE_JOB_LIST_SUMMARY_PREFIX = "inline:reconcile-job-list-summary:";
  private static final Set<String> TERMINAL_STATES =
      Set.of("JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED");
  private static final long INVALID_ORDERED_POINTER_MS = Long.MIN_VALUE;

  @Inject BlobStore blobStore;
  @Inject ObjectMapper mapper;
  @Inject ReconcilerSettingsStore settings;
  @Inject ReconcileJobIndexBackend jobIndexBackend;
  @Inject ReconcileReadyQueueBackend readyQueueBackend;
  @Inject ReconcileJobIndexes jobIndexes;
  @Inject PointerStore pointerStore;

  public record AccountResult(
      int scanned,
      int expired,
      int ptrDeleted,
      int blobDeleted,
      int dedupeDeleted,
      int readyDeleted,
      int canonicalQuarantined,
      int dedupeQuarantined,
      int rootSummaryQuarantined,
      String nextJobToken,
      String nextDedupeToken,
      String nextRootSummaryToken) {}

  public record GlobalResult(int scanned, int deleted, int quarantined, String nextToken) {}

  private enum RootSummaryReadStatus {
    READABLE,
    UNREADABLE
  }

  private record RootSummaryReadResult(
      RootSummaryReadStatus status, StoredReconcileJobListSummary summary) {}

  private record RootSummaryDeleteResult(int deleted, int quarantined) {}

  public AccountResult runAccountSlice(String accountId, String jobTokenIn, String dedupeTokenIn) {
    return runAccountSlice(accountId, jobTokenIn, dedupeTokenIn, "");
  }

  public AccountResult runAccountSlice(
      String accountId, String jobTokenIn, String dedupeTokenIn, String rootSummaryTokenIn) {
    var cfg = ConfigProvider.getConfig();
    final int pageSize =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.page-size", Integer.class).orElse(200);
    final int batchLimit =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.batch-limit", Integer.class).orElse(1000);
    final long sliceMillis =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.slice-millis", Long.class).orElse(4000L);
    final long retentionMs =
        Math.max(
            1L,
            settings != null
                ? settings.finishedJobRetentionMs()
                : cfg.getOptionalValue("floecat.gc.reconcile-jobs.retention-ms", Long.class)
                    .orElse(24L * 60L * 60L * 1000L));

    final long nowMs = System.currentTimeMillis();
    final long deadline = nowMs + sliceMillis;

    String jobToken = jobTokenIn == null ? "" : jobTokenIn;
    String dedupeToken = dedupeTokenIn == null ? "" : dedupeTokenIn;
    String rootSummaryToken = rootSummaryTokenIn == null ? "" : rootSummaryTokenIn;

    int scanned = 0;
    int expired = 0;
    int ptrDeleted = 0;
    int blobDeleted = 0;
    int dedupeDeleted = 0;
    int readyDeleted = 0;
    int canonicalQuarantined = 0;
    int dedupeQuarantined = 0;
    int rootSummaryQuarantined = 0;

    String jobPrefix = Keys.reconcileJobPointerByIdPrefix(accountId);
    while (scanned < batchLimit && System.currentTimeMillis() < deadline) {
      var page = jobIndexBackend.listCanonicalEntries(accountId, pageSize, jobToken);
      var pointers = page.entries();
      jobToken = page.nextPageToken();
      if (pointers.isEmpty()) {
        break;
      }

      for (var canonical : pointers) {
        if (scanned >= batchLimit || System.currentTimeMillis() >= deadline) {
          break;
        }
        scanned++;

        JsonNode record = readRecordByReference(canonical.blobUri());
        String jobId = decodeJobId(jobPrefix, canonical.pointerKey());
        if (record == null) {
          canonicalQuarantined++;
          continue;
        }

        String state = text(record, "state");
        long updatedAt =
            longValue(
                record,
                "updatedAtMs",
                longValue(record, "finishedAtMs", longValue(record, "createdAtMs", nowMs)));

        if (TERMINAL_STATES.contains(state)) {
          // Terminal jobs must never hold queue/dedupe references.
          StoredReconcileJob stored = storedJob(record);
          String dedupePointerKey = stored == null ? "" : jobIndexes.dedupePointerKey(stored);
          if (!dedupePointerKey.isBlank()
              && deleteJobIndexPointerIfOwned(dedupePointerKey, canonical.pointerKey())) {
            dedupeDeleted++;
          }
          for (String readyKey : readyPointerKeysForCleanup(record)) {
            if (!readyKey.isBlank() && readyQueueBackend.deleteReadyEntry(readyKey)) {
              readyDeleted++;
            }
          }

          if (updatedAt <= nowMs - retentionMs) {
            if (deleteCanonicalFootprint(accountId, jobId, canonical, record)) {
              expired++;
              ptrDeleted++;
              if (jobId != null) {
                blobDeleted += deleteJobBlobs(accountId, jobId);
              }
            }
          }
        }
      }

      if (jobToken.isBlank()) {
        break;
      }
    }

    String dedupePrefix = Keys.reconcileDedupePointerPrefix(accountId);
    while (scanned < batchLimit && System.currentTimeMillis() < deadline) {
      var dedupePage = jobIndexBackend.listDedupeEntries(accountId, pageSize, dedupeToken);
      var dedupePointers = dedupePage.entries();
      dedupeToken = dedupePage.nextPageToken();
      if (dedupePointers.isEmpty()) {
        break;
      }

      for (var dedupe : dedupePointers) {
        if (scanned >= batchLimit || System.currentTimeMillis() >= deadline) {
          break;
        }
        scanned++;

        var canonical = jobIndexBackend.loadIndexEntry(dedupe.blobUri()).orElse(null);
        if (canonical == null) {
          if (deleteJobIndexPointerIfPresent(dedupe.pointerKey())) {
            dedupeDeleted++;
          }
          continue;
        }
        JsonNode record = readRecordByReference(canonical.blobUri());
        if (record == null) {
          dedupeQuarantined++;
          continue;
        }

        if (TERMINAL_STATES.contains(text(record, "state"))) {
          if (deleteJobIndexPointerIfPresent(dedupe.pointerKey())) {
            dedupeDeleted++;
          }
        }
      }

      if (dedupeToken.isBlank()) {
        break;
      }
    }

    String rootSummaryPrefix = Keys.reconcileRootJobSummaryByAccountPointerPrefix(accountId);
    while (scanned < batchLimit && System.currentTimeMillis() < deadline && pointerStore != null) {
      StringBuilder next = new StringBuilder();
      var summaries =
          pointerStore.listPointersByPrefix(rootSummaryPrefix, pageSize, rootSummaryToken, next);
      rootSummaryToken = next.toString();
      if (summaries.isEmpty()) {
        break;
      }

      for (Pointer summaryPointer : summaries) {
        if (scanned >= batchLimit || System.currentTimeMillis() >= deadline) {
          break;
        }
        scanned++;
        RootSummaryDeleteResult result = deleteRootSummaryIfOrphan(accountId, summaryPointer);
        ptrDeleted += result.deleted();
        rootSummaryQuarantined += result.quarantined();
      }

      if (rootSummaryToken.isBlank()) {
        break;
      }
    }

    return new AccountResult(
        scanned,
        expired,
        ptrDeleted,
        blobDeleted,
        dedupeDeleted,
        readyDeleted,
        canonicalQuarantined,
        dedupeQuarantined,
        rootSummaryQuarantined,
        jobToken,
        dedupeToken,
        rootSummaryToken);
  }

  public GlobalResult runReadySlice(String pageTokenIn) {
    var cfg = ConfigProvider.getConfig();
    final int pageSize =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.page-size", Integer.class).orElse(200);
    final int batchLimit =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.global-ready-batch-limit", Integer.class)
            .orElse(1000);
    final long staleReadyGraceMs =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.ready-stale-grace-ms", Long.class)
            .orElse(60_000L);
    final long nowMs = System.currentTimeMillis();

    int scanned = 0;
    int deleted = 0;
    int quarantined = 0;
    String token = pageTokenIn == null ? "" : pageTokenIn;

    while (scanned < batchLimit) {
      var readyPage = readyQueueBackend.scanAllReadyEntries(pageSize, token);
      token = readyPage.nextPageToken();
      if (readyPage.entries().isEmpty()) {
        break;
      }

      for (var ready : readyPage.entries()) {
        if (scanned >= batchLimit) {
          break;
        }
        scanned++;

        var canonical = jobIndexBackend.loadIndexEntry(ready.canonicalPointerKey()).orElse(null);
        if (canonical == null) {
          if (ready.dueAtMs() <= nowMs - staleReadyGraceMs
              && readyQueueBackend.deleteReadyEntry(ready.readyPointerKey())) {
            deleted++;
          }
          continue;
        }
        JsonNode record = readRecordByReference(canonical.blobUri());
        if (record == null) {
          quarantined++;
          continue;
        }

        String state = text(record, "state");
        if (!"JS_QUEUED".equals(state)
            && ready.dueAtMs() <= nowMs - staleReadyGraceMs
            && readyQueueBackend.deleteReadyEntry(ready.readyPointerKey())) {
          deleted++;
        }
      }

      if (token.isBlank()) {
        break;
      }
    }

    return new GlobalResult(scanned, deleted, quarantined, token);
  }

  private JsonNode readRecordByCanonicalKey(String canonicalPointerKey) {
    if (canonicalPointerKey == null || canonicalPointerKey.isBlank()) {
      return null;
    }
    var canonical = jobIndexBackend.loadIndexEntry(canonicalPointerKey).orElse(null);
    return canonical == null ? null : readRecordByReference(canonical.blobUri());
  }

  private JsonNode readRecordByReference(String reference) {
    if (reference == null || reference.isBlank()) {
      return null;
    }
    if (reference.startsWith(INLINE_JOB_STATE_PREFIX)) {
      try {
        byte[] payload =
            Base64.getUrlDecoder().decode(reference.substring(INLINE_JOB_STATE_PREFIX.length()));
        return mapper.readTree(payload);
      } catch (Exception ignored) {
        return null;
      }
    }
    return null;
  }

  private static String text(JsonNode node, String field) {
    if (node == null || field == null) {
      return "";
    }
    JsonNode child = node.get(field);
    if (child == null || child.isNull()) {
      return "";
    }
    String value = child.asText("");
    return value == null ? "" : value;
  }

  private static long longValue(JsonNode node, String field, long defaultValue) {
    if (node == null || field == null) {
      return defaultValue;
    }
    JsonNode child = node.get(field);
    if (child == null || child.isNull()) {
      return defaultValue;
    }
    return child.asLong(defaultValue);
  }

  private RootSummaryDeleteResult deleteRootSummaryIfOrphan(
      String accountId, Pointer summaryPointer) {
    if (summaryPointer == null
        || summaryPointer.getKey() == null
        || summaryPointer.getKey().isBlank()
        || pointerStore == null) {
      return new RootSummaryDeleteResult(0, 0);
    }
    RootSummaryReadResult read = readRootSummary(summaryPointer.getBlobUri());
    if (read.status() != RootSummaryReadStatus.READABLE) {
      return new RootSummaryDeleteResult(0, 1);
    }
    StoredReconcileJobListSummary summary = read.summary();
    if (summary == null || summary.jobId() == null || summary.jobId().isBlank()) {
      return new RootSummaryDeleteResult(0, 1);
    }
    if (jobIndexBackend
        .loadIndexEntry(Keys.reconcileJobPointerById(accountId, summary.jobId()))
        .isPresent()) {
      return new RootSummaryDeleteResult(0, 0);
    }
    var deletes = new java.util.ArrayList<PointerStore.CasOp>();
    deletes.add(new PointerStore.CasDelete(summaryPointer.getKey(), summaryPointer.getVersion()));
    appendPointerDeleteIfPresent(
        deletes, Keys.reconcileJobProjectionPointer(accountId, summary.jobId()));
    if (summary.connectorId() != null && !summary.connectorId().isBlank()) {
      String connectorSummaryKey =
          Keys.reconcileRootJobSummaryByConnectorPointer(
              accountId,
              summary.connectorId(),
              rootSummarySortableJobToken(summary.createdAtMs(), summary.jobId()));
      appendPointerDeleteIfPresent(deletes, connectorSummaryKey);
    }
    return new RootSummaryDeleteResult(
        pointerStore.compareAndSetBatch(deletes) ? deletes.size() : 0, 0);
  }

  private RootSummaryReadResult readRootSummary(String reference) {
    if (reference == null || !reference.startsWith(INLINE_JOB_LIST_SUMMARY_PREFIX)) {
      return new RootSummaryReadResult(RootSummaryReadStatus.UNREADABLE, null);
    }
    try {
      byte[] payload =
          Base64.getUrlDecoder()
              .decode(reference.substring(INLINE_JOB_LIST_SUMMARY_PREFIX.length()));
      return new RootSummaryReadResult(
          RootSummaryReadStatus.READABLE,
          mapper.readValue(payload, StoredReconcileJobListSummary.class));
    } catch (Exception ignored) {
      return new RootSummaryReadResult(RootSummaryReadStatus.UNREADABLE, null);
    }
  }

  private static String decodeJobId(String prefix, String canonicalKey) {
    if (canonicalKey == null || prefix == null || !canonicalKey.startsWith(prefix)) {
      return null;
    }
    String suffix = canonicalKey.substring(prefix.length());
    if (suffix.isBlank()) {
      return null;
    }
    return URLDecoder.decode(suffix, StandardCharsets.UTF_8);
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

  private boolean deleteCanonicalFootprint(
      String accountId, String jobId, JobIndexEntrySnapshot canonical, JsonNode record) {
    if (canonical == null || jobId == null || jobId.isBlank()) {
      return false;
    }
    var deletes = new java.util.ArrayList<ReconcileJobIndexStore.JobIndexWriteOp>();
    var pointerDeletes = new java.util.ArrayList<PointerStore.CasOp>();
    deletes.add(
        new ReconcileJobIndexStore.JobIndexDelete(canonical.pointerKey(), canonical.version()));
    appendDeleteIfPresent(deletes, Keys.reconcileJobLookupPointerById(jobId));

    if (record != null) {
      String parentJobId = text(record, "parentJobId");
      if (!parentJobId.isBlank()) {
        appendDeleteIfPresent(deletes, jobIndexes.parentPointerKey(accountId, parentJobId, jobId));
      }
      StoredReconcileJob stored = storedJob(record);
      appendPointerDeleteIfPresent(
          pointerDeletes, Keys.reconcileJobProjectionPointer(accountId, jobId));
      appendDeleteIfPresent(
          deletes,
          jobIndexes.connectorIndexPointerKey(
              accountId, text(record, "connectorId"), longValue(record, "createdAtMs", 0L), jobId));
      if (text(record, "parentJobId").isBlank()) {
        appendPointerDeleteIfPresent(
            pointerDeletes,
            Keys.reconcileRootJobSummaryByAccountPointer(
                accountId,
                rootSummarySortableJobToken(longValue(record, "createdAtMs", 0L), jobId)));
        String connectorId = text(record, "connectorId");
        if (!connectorId.isBlank()) {
          appendPointerDeleteIfPresent(
              pointerDeletes,
              Keys.reconcileRootJobSummaryByConnectorPointer(
                  accountId,
                  connectorId,
                  rootSummarySortableJobToken(longValue(record, "createdAtMs", 0L), jobId)));
        }
      }
      for (String stateKey : jobIndexes.statePointerKeys(stored)) {
        appendDeleteIfPresent(deletes, stateKey);
      }
      String dedupeKey = jobIndexes.dedupePointerKey(stored);
      if (!dedupeKey.isBlank()) {
        appendOwnedDeleteIfPresent(deletes, dedupeKey, canonical.pointerKey());
      }
    }

    java.util.LinkedHashSet<String> readyDeletes = new java.util.LinkedHashSet<>();
    if (record != null) {
      String preferredReadyKey = text(record, "readyPointerKey");
      if (!preferredReadyKey.isBlank()) {
        readyDeletes.add(preferredReadyKey);
      }
      for (String readyKey : readyPointerKeysForCleanup(record)) {
        if (!readyKey.isBlank()) {
          readyDeletes.add(readyKey);
        }
      }
    }

    return jobIndexBackend.compareAndSetBatch(
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            deletes,
            new ReconcileJobIndexStore.ReadyQueueMutation(
                List.of(), new java.util.ArrayList<>(readyDeletes))),
        pointerDeletes);
  }

  private void appendDeleteIfPresent(
      java.util.List<ReconcileJobIndexStore.JobIndexWriteOp> deletes, String pointerKey) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return;
    }
    var existing = jobIndexBackend.loadIndexEntry(pointerKey).orElse(null);
    if (existing != null) {
      deletes.add(
          new ReconcileJobIndexStore.JobIndexDelete(existing.pointerKey(), existing.version()));
    }
  }

  private static String rootSummarySortableJobToken(long createdAtMs, String jobId) {
    long created = Math.max(0L, createdAtMs);
    long reversedCreated = Long.MAX_VALUE - created;
    return String.format("%019d-%s", reversedCreated, jobId);
  }

  private void appendPointerDeleteIfPresent(
      java.util.List<PointerStore.CasOp> deletes, String pointerKey) {
    if (pointerKey == null || pointerKey.isBlank() || pointerStore == null) {
      return;
    }
    var existing = pointerStore.get(pointerKey).orElse(null);
    if (existing != null) {
      deletes.add(new PointerStore.CasDelete(existing.getKey(), existing.getVersion()));
    }
  }

  private void appendOwnedDeleteIfPresent(
      java.util.List<ReconcileJobIndexStore.JobIndexWriteOp> deletes,
      String pointerKey,
      String expectedReference) {
    if (pointerKey == null || pointerKey.isBlank() || expectedReference == null) {
      return;
    }
    var existing = jobIndexBackend.loadIndexEntry(pointerKey).orElse(null);
    if (existing != null && expectedReference.equals(existing.blobUri())) {
      deletes.add(
          new ReconcileJobIndexStore.JobIndexDelete(existing.pointerKey(), existing.version()));
    }
  }

  private boolean deleteJobIndexPointerIfPresent(String pointerKey) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return false;
    }
    var existing = jobIndexBackend.loadIndexEntry(pointerKey).orElse(null);
    if (existing == null) {
      return false;
    }
    return jobIndexBackend.compareAndSetBatch(
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.of(
                new ReconcileJobIndexStore.JobIndexDelete(
                    existing.pointerKey(), existing.version())),
            ReconcileJobIndexStore.ReadyQueueMutation.empty()));
  }

  private boolean deleteJobIndexPointerIfOwned(String pointerKey, String expectedReference) {
    if (pointerKey == null
        || pointerKey.isBlank()
        || expectedReference == null
        || expectedReference.isBlank()) {
      return false;
    }
    var existing = jobIndexBackend.loadIndexEntry(pointerKey).orElse(null);
    if (existing == null || !expectedReference.equals(existing.blobUri())) {
      return false;
    }
    return jobIndexBackend.compareAndSetBatch(
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.of(
                new ReconcileJobIndexStore.JobIndexDelete(
                    existing.pointerKey(), existing.version())),
            ReconcileJobIndexStore.ReadyQueueMutation.empty()));
  }

  private int deleteJobBlobs(String accountId, String jobId) {
    String prefix = Keys.reconcileJobBlobPrefix(accountId, jobId);
    boolean hadBlob = !blobStore.list(prefix, 1, "").keys().isEmpty();
    blobStore.deletePrefix(prefix);
    return hadBlob ? 1 : 0;
  }

  private StoredReconcileJob storedJob(JsonNode record) {
    if (record == null) {
      return null;
    }
    StoredReconcileJob stored = new StoredReconcileJob();
    stored.jobId = text(record, "jobId");
    stored.accountId = text(record, "accountId");
    stored.connectorId = text(record, "connectorId");
    stored.parentJobId = text(record, "parentJobId");
    stored.state = text(record, "state");
    stored.fileGroupResultBlobUri = text(record, "fileGroupResultBlobUri");
    stored.createdAtMs = longValue(record, "createdAtMs", 0L);
    stored.updatedAtMs = longValue(record, "updatedAtMs", 0L);
    stored.nextAttemptAtMs = longValue(record, "nextAttemptAtMs", 0L);
    stored.laneKey = text(record, "laneKey");
    stored.executionClass = text(record, "executionClass");
    stored.executionLane = text(record, "executionLane");
    stored.pinnedExecutorId = text(record, "pinnedExecutorId");
    stored.jobKind = text(record, "jobKind");
    stored.readyPointerKey = text(record, "readyPointerKey");
    stored.dedupeKeyHash = text(record, "dedupeKeyHash");
    return stored;
  }

  private List<String> currentReadyPointerKeys(JsonNode record) {
    if (record == null) {
      return List.of();
    }
    StoredReconcileJob stored = storedJob(record);
    if (stored == null || !"JS_QUEUED".equals(stored.state)) {
      return List.of();
    }
    if (stored.accountId == null
        || stored.accountId.isBlank()
        || stored.jobId == null
        || stored.jobId.isBlank()
        || stored.laneKey == null
        || stored.laneKey.isBlank()) {
      return List.of();
    }
    long dueAt = readyPointerDueAt(stored);
    if (dueAt == INVALID_ORDERED_POINTER_MS || dueAt <= 0L) {
      dueAt = System.currentTimeMillis();
    }
    java.util.ArrayList<String> keys = new java.util.ArrayList<>();
    keys.add(
        Keys.reconcileReadyPointerByDue(dueAt, stored.accountId, stored.laneKey, stored.jobId));
    if (stored.executionClass != null && !stored.executionClass.isBlank()) {
      keys.add(
          Keys.reconcileReadyByExecutionClassPointerByDue(
              dueAt, stored.executionClass, stored.accountId, stored.jobId));
    }
    String executionLane = stored.executionPolicy().lane();
    if (executionLane != null && !executionLane.isBlank()) {
      keys.add(
          Keys.reconcileReadyByExecutionLanePointerByDue(
              dueAt, executionLane, stored.accountId, stored.jobId));
    }
    if (stored.pinnedExecutorId != null && !stored.pinnedExecutorId.isBlank()) {
      keys.add(
          Keys.reconcileReadyByPinnedExecutorPointerByDue(
              dueAt, stored.pinnedExecutorId, stored.accountId, stored.jobId));
    }
    if (stored.jobKind != null && !stored.jobKind.isBlank()) {
      keys.add(
          Keys.reconcileReadyByJobKindPointerByDue(
              dueAt, stored.jobKind, stored.accountId, stored.jobId));
    }
    return keys;
  }

  private List<String> readyPointerKeysForCleanup(JsonNode record) {
    if (record == null) {
      return List.of();
    }
    StoredReconcileJob stored = storedJob(record);
    if (stored == null) {
      return List.of();
    }
    java.util.LinkedHashSet<String> readyKeys =
        new java.util.LinkedHashSet<>(currentReadyPointerKeys(record));
    boolean hasStoredReadyPointer =
        stored.readyPointerKey != null && !stored.readyPointerKey.isBlank();
    if (hasStoredReadyPointer) {
      readyKeys.add(stored.readyPointerKey);
    }
    boolean shouldReconstructHistoricalReadyKeys =
        hasStoredReadyPointer || !TERMINAL_STATES.contains(stored.state);
    if (shouldReconstructHistoricalReadyKeys) {
      long dueAt = readyPointerDueAt(stored);
      if (dueAt != INVALID_ORDERED_POINTER_MS && dueAt > 0L) {
        if (stored.accountId != null
            && !stored.accountId.isBlank()
            && stored.jobId != null
            && !stored.jobId.isBlank()
            && stored.laneKey != null
            && !stored.laneKey.isBlank()) {
          readyKeys.add(
              Keys.reconcileReadyPointerByDue(
                  dueAt, stored.accountId, stored.laneKey, stored.jobId));
        }
        if (stored.executionClass != null && !stored.executionClass.isBlank()) {
          readyKeys.add(
              Keys.reconcileReadyByExecutionClassPointerByDue(
                  dueAt, stored.executionClass, stored.accountId, stored.jobId));
        }
        String executionLane = stored.executionPolicy().lane();
        if (executionLane != null && !executionLane.isBlank()) {
          readyKeys.add(
              Keys.reconcileReadyByExecutionLanePointerByDue(
                  dueAt, executionLane, stored.accountId, stored.jobId));
        }
        if (stored.pinnedExecutorId != null && !stored.pinnedExecutorId.isBlank()) {
          readyKeys.add(
              Keys.reconcileReadyByPinnedExecutorPointerByDue(
                  dueAt, stored.pinnedExecutorId, stored.accountId, stored.jobId));
        }
        if (stored.jobKind != null && !stored.jobKind.isBlank()) {
          readyKeys.add(
              Keys.reconcileReadyByJobKindPointerByDue(
                  dueAt, stored.jobKind, stored.accountId, stored.jobId));
        }
      }
    }
    readyKeys.removeIf(readyKey -> readyKey == null || readyKey.isBlank());
    return List.copyOf(readyKeys);
  }

  private long readyPointerDueAt(StoredReconcileJob stored) {
    if (stored == null) {
      return INVALID_ORDERED_POINTER_MS;
    }
    if (stored.nextAttemptAtMs > 0L) {
      return stored.nextAttemptAtMs;
    }
    return parseDueMillis(stored.readyPointerKey);
  }

  private long parseDueMillis(String readyPointerKey) {
    return parseTimestampFromOrderedPointer(readyPointerKey, Keys.reconcileReadyPointerPrefix());
  }

  private long parseTimestampFromOrderedPointer(String pointerKey, String prefix) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return INVALID_ORDERED_POINTER_MS;
    }
    String normalizedKey = normalizePointerKey(pointerKey);
    String normalizedPrefix = normalizePointerKey(prefix);
    if (!normalizedKey.startsWith(normalizedPrefix)) {
      return INVALID_ORDERED_POINTER_MS;
    }
    int slash = normalizedKey.indexOf('/', normalizedPrefix.length());
    if (slash < 0) {
      return INVALID_ORDERED_POINTER_MS;
    }
    String token = normalizedKey.substring(normalizedPrefix.length(), slash);
    try {
      return Long.parseLong(token);
    } catch (NumberFormatException nfe) {
      return INVALID_ORDERED_POINTER_MS;
    }
  }

  private static String normalizePointerKey(String key) {
    if (key == null || key.isBlank()) {
      return "/";
    }
    return key.startsWith("/") ? key : "/" + key;
  }
}
