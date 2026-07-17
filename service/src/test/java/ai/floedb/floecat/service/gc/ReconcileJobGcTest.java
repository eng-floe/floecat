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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobListSummary;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexEntrySnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.NativeReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexCleanupManifest;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconcileJobGcTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String CONNECTOR_ID = "conn-1";
  private static final String INLINE_JOB_STATE_PREFIX = "inline:reconcile-job:";

  private PointerStore pointers;
  private BlobStore blobs;
  private ObjectMapper mapper;
  private ReconcileJobGc gc;
  private MemoryReconcileJobIndexBackend jobIndexBackend;
  private MemoryReconcileReadyQueueBackend readyQueueBackend;

  @BeforeEach
  void setUp() {
    pointers = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    mapper = new ObjectMapper();

    gc = new ReconcileJobGc();
    gc.blobStore = blobs;
    gc.mapper = mapper;
    gc.jobIndexBackend = jobIndexBackend = new MemoryReconcileJobIndexBackend();
    jobIndexBackend.bind(pointers);
    gc.readyQueueBackend = readyQueueBackend = new MemoryReconcileReadyQueueBackend();
    readyQueueBackend.bind(pointers);
    gc.pointerStore = pointers;
    gc.jobIndexes = new ReconcileJobIndexes();
    gc.jobIndexes.bind(pointers, ignored -> false, ignored -> java.util.List.of());
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(blobs, pointers, mapper);
    gc.jobIndexStore = new NativeReconcileJobIndexStore();
    gc.jobIndexStore.bind(
        jobIndexBackend,
        payloadStore,
        gc.jobIndexes,
        16,
        (previous, current) -> {},
        (previous, current, operation) -> {});
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("floecat.gc.reconcile-jobs.page-size");
    System.clearProperty("floecat.gc.reconcile-jobs.batch-limit");
    System.clearProperty("floecat.gc.reconcile-jobs.slice-millis");
    System.clearProperty("floecat.gc.reconcile-jobs.retention-ms");
    System.clearProperty("floecat.gc.reconcile-jobs.global-ready-batch-limit");
    System.clearProperty("floecat.gc.reconcile-jobs.ready-stale-grace-ms");
    System.clearProperty("floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms");
  }

  @Test
  void accountSliceRemovesExpiredTerminalJobs() {
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "0");
    String jobId = "job-expired";
    String dedupeHash = hashValue(ACCOUNT_ID + "|" + CONNECTOR_ID + "|incr|*|*|");
    String dedupePointer = Keys.reconcileDedupePointer(ACCOUNT_ID, dedupeHash);
    String readyPointer =
        Keys.reconcileReadyPointerByDue(
            System.currentTimeMillis() - 1_000L, ACCOUNT_ID, "lane", jobId);
    StoredReconcileJob record =
        storedJob(
            jobId,
            "JS_SUCCEEDED",
            System.currentTimeMillis() - 10_000L,
            "",
            dedupeHash,
            readyPointer);
    putNativeJobIndexRows(record);
    String historyBlob = Keys.reconcileJobBlobUri(ACCOUNT_ID, jobId, "history");
    blobs.put(
        historyBlob,
        "{\"old\":true}".getBytes(StandardCharsets.UTF_8),
        "application/json; charset=UTF-8");
    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.expired() >= 1);
    assertTrue(result.blobDeleted() >= 1);
    assertTrue(pointers.get(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId)).isEmpty());
    assertTrue(pointers.get(Keys.reconcileJobLookupPointerById(jobId)).isEmpty());
    assertTrue(pointers.get(dedupePointer).isEmpty());
    assertTrue(pointers.get(readyPointer).isEmpty());
    assertFalse(blobs.head(historyBlob).isPresent());
  }

  @Test
  void accountSliceBatchesExpiredTerminalJobCleanup() {
    CountingMemoryJobIndexBackend countingBackend = useCountingJobIndexBackend(false);
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "0");
    long updatedAtMs = System.currentTimeMillis() - 10_000L;
    putNativeJobIndexRows(
        storedJob("job-batch-a", "JS_SUCCEEDED", updatedAtMs, "", "hash-batch-a", ""));
    putNativeJobIndexRows(
        storedJob("job-batch-b", "JS_SUCCEEDED", updatedAtMs, "", "hash-batch-b", ""));

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(2, result.expired());
    assertEquals(1, countingBackend.batchedDeleteCalls);
    assertEquals(0, countingBackend.singleDeleteCalls);
  }

  @Test
  void accountSliceFallsBackToIndividualCleanupAfterBatchRace() {
    CountingMemoryJobIndexBackend countingBackend = useCountingJobIndexBackend(true);
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "0");
    long updatedAtMs = System.currentTimeMillis() - 10_000L;
    putNativeJobIndexRows(
        storedJob("job-fallback-a", "JS_SUCCEEDED", updatedAtMs, "", "hash-fallback-a", ""));
    putNativeJobIndexRows(
        storedJob("job-fallback-b", "JS_SUCCEEDED", updatedAtMs, "", "hash-fallback-b", ""));

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(2, result.expired());
    assertEquals(1, countingBackend.batchedDeleteCalls);
    assertEquals(2, countingBackend.singleDeleteCalls);
  }

  @Test
  void accountSliceKeepsUnreadableCanonicalPointers() {
    String jobId = "job-missing-inline";
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    putPointer(canonicalKey, "inline:reconcile-job:not-valid");
    putPointer(Keys.reconcileJobLookupPointerById(jobId), canonicalKey);

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, result.ptrDeleted());
    assertEquals(1, result.canonicalQuarantined());
    assertTrue(pointers.get(canonicalKey).isPresent());
    assertTrue(pointers.get(Keys.reconcileJobLookupPointerById(jobId)).isPresent());
  }

  @Test
  void accountSliceClearsQuarantineMarkerWhenCanonicalBecomesReadable() {
    String jobId = "job-corrupt-then-readable";
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    putPointer(canonicalKey, "inline:reconcile-job:not-valid");
    putPointer(Keys.reconcileJobLookupPointerById(jobId), canonicalKey);

    var first = gc.runAccountSlice(ACCOUNT_ID, "", "");

    String markerKey =
        Keys.reconcileCanonicalQuarantinePointer(ACCOUNT_ID, hashValue(canonicalKey));
    assertEquals(1, first.canonicalQuarantined());
    assertTrue(pointers.get(markerKey).isPresent());

    Pointer corrupt = pointers.get(canonicalKey).orElseThrow();
    StoredReconcileJob readable =
        storedJob(jobId, "JS_RUNNING", System.currentTimeMillis(), "", "", "");
    String readableReference = inlineJobReference(readable);
    assertTrue(
        pointers.compareAndSet(
            canonicalKey,
            corrupt.getVersion(),
            PointerReferences.inlineJsonPointer(
                canonicalKey, readableReference, corrupt.getVersion() + 1L)));
    System.setProperty("floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms", "0");

    var second = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, second.ptrDeleted());
    assertEquals(0, second.canonicalQuarantined());
    assertTrue(pointers.get(canonicalKey).isPresent());
    assertTrue(pointers.get(markerKey).isEmpty());
  }

  @Test
  void accountSliceContinuesCanonicalQuarantineMarkerCleanupAfterPartialPage() {
    System.setProperty("floecat.gc.reconcile-jobs.page-size", "2");
    System.setProperty("floecat.gc.reconcile-jobs.batch-limit", "3");
    java.util.ArrayList<String> markerKeys = new java.util.ArrayList<>();
    java.util.HashMap<String, String> canonicalByMarker = new java.util.HashMap<>();
    for (int i = 0; i < 4; i++) {
      String jobId = "job-quarantine-marker-page-" + i;
      String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
      String markerKey =
          Keys.reconcileCanonicalQuarantinePointer(ACCOUNT_ID, hashValue(canonicalKey));
      markerKeys.add(markerKey);
      canonicalByMarker.put(markerKey, canonicalKey);
    }
    java.util.Collections.sort(markerKeys);

    for (int i = 0; i < markerKeys.size(); i++) {
      String markerKey = markerKeys.get(i);
      String canonicalKey = canonicalByMarker.get(markerKey);
      if (i == 2) {
        StoredReconcileJob readable =
            storedJob(
                canonicalKey.substring(canonicalKey.lastIndexOf('/') + 1),
                "JS_RUNNING",
                System.currentTimeMillis(),
                "",
                "",
                "");
        String readableReference = inlineJobReference(readable);
        pointers.compareAndSet(
            canonicalKey,
            0L,
            PointerReferences.inlineJsonPointer(canonicalKey, readableReference, 1L));
        putQuarantineMarker(markerKey, canonicalKey, 1L, readableReference);
      } else {
        putQuarantineMarker(markerKey, canonicalKey, 1L, "inline:reconcile-job:not-valid");
      }
    }

    var first = gc.runAccountSlice(ACCOUNT_ID, "", "");
    assertFalse(first.nextCanonicalQuarantineToken().isBlank());
    assertTrue(pointers.get(markerKeys.get(2)).isPresent());

    var second =
        gc.runAccountSlice(ACCOUNT_ID, "", first.nextCanonicalQuarantineToken(), "", "", "");

    assertTrue(pointers.get(markerKeys.get(2)).isEmpty());
    assertEquals("", second.nextCanonicalQuarantineToken());
  }

  @Test
  void accountSlicePurgesUnreadableCanonicalPointersAfterQuarantineRetention() {
    System.setProperty("floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms", "0");
    String jobId = "job-corrupt-purge";
    String parentJobId = "parent-corrupt-purge";
    long now = System.currentTimeMillis() - 10_000L;
    String dedupeHash = hashValue(ACCOUNT_ID + "|" + CONNECTOR_ID + "|corrupt-purge|*|*|");
    StoredReconcileJob record =
        storedJob(
            jobId,
            "JS_SUCCEEDED",
            now,
            parentJobId,
            dedupeHash,
            Keys.reconcileReadyPointerByDue(now, ACCOUNT_ID, "lane-native", jobId));
    putNativeJobIndexRows(record);
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer canonical = pointers.get(canonicalKey).orElseThrow();
    assertTrue(
        pointers.compareAndSet(
            canonicalKey,
            canonical.getVersion(),
            PointerReferences.inlineJsonPointer(
                canonicalKey, "inline:reconcile-job:not-valid", canonical.getVersion() + 1L)));

    var first = gc.runAccountSlice(ACCOUNT_ID, "", "");
    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(1, first.canonicalQuarantined());
    assertTrue(result.expired() >= 1);
    assertTrue(result.ptrDeleted() >= 1);
    assertEquals(0, result.canonicalQuarantined());
    assertTrue(jobIndexBackend.loadIndexEntry(canonicalKey).isEmpty());
    assertTrue(jobIndexBackend.loadIndexEntry(Keys.reconcileJobLookupPointerById(jobId)).isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileJobByParentPointer(ACCOUNT_ID, parentJobId, jobId))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(
                Keys.reconcileJobByConnectorPointer(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    String.format("%019d-%s", Long.MAX_VALUE - now, jobId)))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileJobByStatePointer("JS_SUCCEEDED", now, ACCOUNT_ID, jobId))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(
                Keys.reconcileJobByAccountStatePointer(ACCOUNT_ID, "JS_SUCCEEDED", now, jobId))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(
                Keys.reconcileJobByConnectorStatePointer(
                    ACCOUNT_ID, CONNECTOR_ID, "JS_SUCCEEDED", now, jobId))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileDedupePointer(ACCOUNT_ID, dedupeHash))
            .isEmpty());
    assertTrue(pointers.get(record.readyPointerKey).isEmpty());
  }

  @Test
  void accountSliceBatchesQuarantinedCanonicalCleanup() {
    CountingMemoryJobIndexBackend countingBackend = useCountingJobIndexBackend(false);
    System.setProperty("floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms", "0");
    long now = System.currentTimeMillis() - 10_000L;
    for (String jobId : java.util.List.of("job-quarantine-batch-a", "job-quarantine-batch-b")) {
      StoredReconcileJob record = storedJob(jobId, "JS_SUCCEEDED", now, "", "hash-" + jobId, "");
      putNativeJobIndexRows(record);
      String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
      Pointer canonical = pointers.get(canonicalKey).orElseThrow();
      assertTrue(
          pointers.compareAndSet(
              canonicalKey,
              canonical.getVersion(),
              PointerReferences.inlineJsonPointer(
                  canonicalKey, "inline:reconcile-job:not-valid", canonical.getVersion() + 1L)));
    }

    var first = gc.runAccountSlice(ACCOUNT_ID, "", "");
    var second = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(2, first.canonicalQuarantined());
    assertEquals(2, second.expired());
    assertEquals(1, countingBackend.batchedDeleteCalls);
    assertEquals(0, countingBackend.singleDeleteCalls);
  }

  @Test
  void accountSlicePurgesReadableLegacyCanonicalWithoutManifest() {
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "0");
    String jobId = "job-readable-no-manifest";
    String parentJobId = "parent-readable-no-manifest";
    long now = System.currentTimeMillis() - 10_000L;
    String dedupeHash = hashValue(ACCOUNT_ID + "|" + CONNECTOR_ID + "|readable|*|*|");
    StoredReconcileJob record =
        storedJob(
            jobId,
            "JS_SUCCEEDED",
            now,
            parentJobId,
            dedupeHash,
            Keys.reconcileReadyPointerByDue(now, ACCOUNT_ID, "lane-native", jobId));
    putNativeJobIndexRows(record, false);
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    assertTrue(jobIndexBackend.loadCleanupManifest(canonicalKey).isEmpty());

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.expired() >= 1);
    assertTrue(jobIndexBackend.loadIndexEntry(canonicalKey).isEmpty());
    assertTrue(jobIndexBackend.loadIndexEntry(Keys.reconcileJobLookupPointerById(jobId)).isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileJobByParentPointer(ACCOUNT_ID, parentJobId, jobId))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileDedupePointer(ACCOUNT_ID, dedupeHash))
            .isEmpty());
    assertTrue(pointers.get(record.readyPointerKey).isEmpty());
  }

  @Test
  void accountSliceDoesNotPartiallyPurgeUnreadableLegacyCanonicalWithoutManifest() {
    System.setProperty("floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms", "0");
    String jobId = "job-legacy-corrupt-no-manifest";
    String parentJobId = "parent-legacy-corrupt-no-manifest";
    long now = System.currentTimeMillis() - 10_000L;
    String dedupeHash = hashValue(ACCOUNT_ID + "|" + CONNECTOR_ID + "|legacy-corrupt|*|*|");
    StoredReconcileJob record =
        storedJob(
            jobId,
            "JS_SUCCEEDED",
            now,
            parentJobId,
            dedupeHash,
            Keys.reconcileReadyPointerByDue(now, ACCOUNT_ID, "lane-native", jobId));
    putNativeJobIndexRows(record, false);
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer canonical = pointers.get(canonicalKey).orElseThrow();
    assertTrue(
        pointers.compareAndSet(
            canonicalKey,
            canonical.getVersion(),
            PointerReferences.inlineJsonPointer(
                canonicalKey, "inline:reconcile-job:not-valid", canonical.getVersion() + 1L)));

    var first = gc.runAccountSlice(ACCOUNT_ID, "", "");
    var second = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(1, first.canonicalQuarantined());
    assertEquals(1, second.canonicalQuarantined());
    assertEquals(0, second.expired());
    assertTrue(jobIndexBackend.loadIndexEntry(canonicalKey).isPresent());
    assertTrue(
        jobIndexBackend.loadIndexEntry(Keys.reconcileJobLookupPointerById(jobId)).isPresent());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileJobByParentPointer(ACCOUNT_ID, parentJobId, jobId))
            .isPresent());
  }

  @Test
  void accountSliceDoesNotPurgeReadableReplacementThatRacesQuarantinePurge() {
    String jobId = "job-corrupt-replaced-during-purge";
    String parentJobId = "parent-corrupt-replaced";
    long now = System.currentTimeMillis() - 10_000L;
    String dedupeHash = hashValue(ACCOUNT_ID + "|" + CONNECTOR_ID + "|corrupt-replaced|*|*|");
    StoredReconcileJob record =
        storedJob(
            jobId,
            "JS_SUCCEEDED",
            now,
            parentJobId,
            dedupeHash,
            Keys.reconcileReadyPointerByDue(now, ACCOUNT_ID, "lane-native", jobId));
    putNativeJobIndexRows(record);
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer canonical = pointers.get(canonicalKey).orElseThrow();
    assertTrue(
        pointers.compareAndSet(
            canonicalKey,
            canonical.getVersion(),
            PointerReferences.inlineJsonPointer(
                canonicalKey, "inline:reconcile-job:not-valid", canonical.getVersion() + 1L)));

    var first = gc.runAccountSlice(ACCOUNT_ID, "", "");
    assertEquals(1, first.canonicalQuarantined());

    StoredReconcileJob replacement =
        storedJob(jobId, "JS_RUNNING", System.currentTimeMillis(), parentJobId, dedupeHash, "");
    String replacementReference = inlineJobReference(replacement);
    gc.jobIndexBackend =
        new ReplacingBeforeQuarantineDeleteBackend(
            jobIndexBackend, pointers, canonicalKey, replacementReference);
    System.setProperty("floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms", "0");

    var second = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, second.ptrDeleted());
    assertEquals(1, second.canonicalQuarantined());
    assertEquals(replacementReference, pointers.get(canonicalKey).orElseThrow().getBlobUri());
    assertTrue(
        jobIndexBackend.loadIndexEntry(Keys.reconcileJobLookupPointerById(jobId)).isPresent());
  }

  @Test
  void accountSliceDeletesDanglingDedupePointers() {
    String dedupeKey = Keys.reconcileDedupePointer(ACCOUNT_ID, "orphan-hash");
    putPointer(dedupeKey, Keys.reconcileJobPointerById(ACCOUNT_ID, "missing-job"));

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.dedupeDeleted() >= 1);
    assertTrue(pointers.get(dedupeKey).isEmpty());
  }

  @Test
  void accountSliceDoesNotDeleteDedupePointerRepointedToNewerJob() {
    String dedupeHash = hashValue(ACCOUNT_ID + "|" + CONNECTOR_ID + "|incr|*|*|");
    String dedupeKey = Keys.reconcileDedupePointer(ACCOUNT_ID, dedupeHash);
    String oldJobId = "job-terminal-old";
    String newJobId = "job-active-new";

    putInlineReconcileJob(
        oldJobId, "JS_SUCCEEDED", System.currentTimeMillis() - 1_000L, dedupeHash, "");
    String newCanonicalKey =
        putInlineReconcileJob(newJobId, "JS_QUEUED", System.currentTimeMillis(), dedupeHash, "");
    putPointer(dedupeKey, newCanonicalKey);

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, result.dedupeDeleted());
    assertEquals(newCanonicalKey, pointers.get(dedupeKey).orElseThrow().getBlobUri());
  }

  @Test
  void accountSliceDoesNotDeleteDedupePointerRepointedDuringDedupeScan() {
    String dedupeHash = hashValue(ACCOUNT_ID + "|" + CONNECTOR_ID + "|dedupe-race|*|*|");
    String dedupeKey = Keys.reconcileDedupePointer(ACCOUNT_ID, dedupeHash);
    String missingCanonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, "missing-old-job");
    String newCanonicalKey =
        putInlineReconcileJob(
            "job-dedupe-race-new", "JS_QUEUED", System.currentTimeMillis(), dedupeHash, "");
    putPointer(dedupeKey, missingCanonicalKey);
    gc.jobIndexBackend =
        new RepointingDedupeDuringListBackend(
            jobIndexBackend, pointers, dedupeKey, missingCanonicalKey, newCanonicalKey);

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, result.dedupeDeleted());
    assertEquals(newCanonicalKey, pointers.get(dedupeKey).orElseThrow().getBlobUri());
  }

  @Test
  void readySliceDeletesStaleReadyPointers() {
    String jobId = "job-running";
    String canonicalKey =
        putInlineReconcileJob(jobId, "JS_RUNNING", System.currentTimeMillis(), "", "");

    String staleReadyKey =
        Keys.reconcileReadyPointerByDue(
            System.currentTimeMillis() - 120_000L, ACCOUNT_ID, "lane-stale", jobId + "-stale");
    putPointer(staleReadyKey, canonicalKey);

    var result = gc.runReadySlice("");

    assertTrue(result.deleted() >= 1);
    assertTrue(pointers.get(staleReadyKey).isEmpty());
  }

  @Test
  void readySliceDeletesStaleSupersededQueuedReadyPointers() {
    System.setProperty("floecat.gc.reconcile-jobs.ready-stale-grace-ms", "0");
    String jobId = "job-queued-stale-ready";
    long now = System.currentTimeMillis();
    String oldReadyKey = Keys.reconcileReadyPointerByDue(now - 120_000L, ACCOUNT_ID, "lane", jobId);
    String currentReadyKey =
        Keys.reconcileReadyPointerByDue(now + 60_000L, ACCOUNT_ID, "lane", jobId);
    String canonicalKey =
        putInlineReconcileJob(
            jobId, "JS_QUEUED", now, "", currentReadyKey, "lane", "", "", "", "", now + 60_000L);
    putPointer(oldReadyKey, canonicalKey);

    var result = gc.runReadySlice("");

    assertTrue(result.deleted() >= 1);
    assertTrue(pointers.get(oldReadyKey).isEmpty());
  }

  @Test
  void readySliceDeletesStaleStoredReadyPointerWhenNextAttemptMovedForward() {
    System.setProperty("floecat.gc.reconcile-jobs.ready-stale-grace-ms", "0");
    String jobId = "job-queued-old-stored-ready";
    long now = System.currentTimeMillis();
    String oldReadyKey = Keys.reconcileReadyPointerByDue(now - 120_000L, ACCOUNT_ID, "lane", jobId);
    String newReadyKey = Keys.reconcileReadyPointerByDue(now + 60_000L, ACCOUNT_ID, "lane", jobId);
    String canonicalKey =
        putInlineReconcileJob(
            jobId, "JS_QUEUED", now, "", oldReadyKey, "lane", "", "", "", "", now + 60_000L);
    putPointer(oldReadyKey, canonicalKey);
    assertTrue(newReadyKey.endsWith("/" + jobId));

    var result = gc.runReadySlice("");

    assertTrue(result.deleted() >= 1);
    assertTrue(pointers.get(oldReadyKey).isEmpty());
  }

  @Test
  void readySliceContinuationDoesNotSkipEntryAfterBatchLimitedPage() {
    System.setProperty("floecat.gc.reconcile-jobs.page-size", "5");
    System.setProperty("floecat.gc.reconcile-jobs.global-ready-batch-limit", "4");
    System.setProperty("floecat.gc.reconcile-jobs.ready-stale-grace-ms", "0");
    long now = System.currentTimeMillis() - 120_000L;
    java.util.ArrayList<ReconcileReadyQueueStore.ReadyQueueEntry> entries =
        new java.util.ArrayList<>();
    for (int i = 0; i < 5; i++) {
      String jobId = "job-ready-page-" + i;
      entries.add(
          new ReconcileReadyQueueStore.ReadyQueueEntry(
              "ready/key/" + i,
              Keys.reconcileJobPointerById(ACCOUNT_ID, jobId),
              ACCOUNT_ID,
              jobId,
              now + i,
              ReconcileReadyQueueStore.ReadyIndexType.GLOBAL,
              ""));
    }
    RecordingReadyQueueBackend backend = new RecordingReadyQueueBackend(entries);
    gc.readyQueueBackend = backend;

    String token = "";
    for (int i = 0; i < 2; i++) {
      var result = gc.runReadySlice(token);
      token = result.nextToken();
      if (token == null || token.isBlank()) {
        break;
      }
    }

    assertEquals(5, backend.deletedKeys.size());
    assertTrue(backend.deletedKeys.contains("ready/key/4"));
  }

  @Test
  void readySliceKeepsFreshReadyPointerWhenRecordStillShowsNotQueued() {
    String jobId = "job-running-fresh-ready-race";
    String canonicalKey =
        putInlineReconcileJob(jobId, "JS_RUNNING", System.currentTimeMillis(), "", "");
    String freshReadyKey =
        Keys.reconcileReadyPointerByDue(System.currentTimeMillis(), ACCOUNT_ID, "lane", jobId);
    putPointer(freshReadyKey, canonicalKey);

    var result = gc.runReadySlice("");

    assertEquals(0, result.deleted());
    assertTrue(pointers.get(freshReadyKey).isPresent());
  }

  @Test
  void readySliceKeepsReadyPointerWhenCanonicalPayloadIsUnreadable() {
    String jobId = "job-ready-unreadable";
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    String readyKey =
        Keys.reconcileReadyPointerByDue(
            System.currentTimeMillis() - 120_000L, ACCOUNT_ID, "lane", jobId);
    putPointer(canonicalKey, "inline:reconcile-job:not-valid");
    putPointer(readyKey, canonicalKey);

    var result = gc.runReadySlice("");

    assertEquals(0, result.deleted());
    assertEquals(1, result.quarantined());
    assertTrue(pointers.get(readyKey).isPresent());
  }

  @Test
  void accountSliceRemovesNativeIndexRowsForExpiredTerminalJobs() {
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "0");
    long now = System.currentTimeMillis() - 10_000L;
    String jobId = "job-native-expired";
    String parentJobId = "parent-native";
    String dedupeInput = ACCOUNT_ID + "|" + CONNECTOR_ID + "|full|*|*|";
    String dedupeHash = hashValue(dedupeInput);
    String readyPointer =
        Keys.reconcileReadyPointerByDue(now - 1_000L, ACCOUNT_ID, "lane-native", jobId);
    StoredReconcileJob record =
        storedJob(jobId, "JS_SUCCEEDED", now, parentJobId, dedupeHash, readyPointer);
    putNativeJobIndexRows(record);
    String projection = Keys.reconcileJobProjectionPointer(ACCOUNT_ID, jobId);
    putPointer(projection, "inline:reconcile-job-projection:child");
    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.expired() >= 1);
    assertTrue(
        jobIndexBackend.loadIndexEntry(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId)).isEmpty());
    assertTrue(jobIndexBackend.loadIndexEntry(Keys.reconcileJobLookupPointerById(jobId)).isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileJobByParentPointer(ACCOUNT_ID, parentJobId, jobId))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(
                Keys.reconcileJobByConnectorPointer(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    String.format("%019d-%s", Long.MAX_VALUE - now, jobId)))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileDedupePointer(ACCOUNT_ID, dedupeHash))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileJobByStatePointer("JS_SUCCEEDED", now, ACCOUNT_ID, jobId))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(
                Keys.reconcileJobByAccountStatePointer(ACCOUNT_ID, "JS_SUCCEEDED", now, jobId))
            .isEmpty());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(
                Keys.reconcileJobByConnectorStatePointer(
                    ACCOUNT_ID, CONNECTOR_ID, "JS_SUCCEEDED", now, jobId))
            .isEmpty());
    assertTrue(pointers.get(projection).isEmpty());
  }

  @Test
  void accountSliceRemovesRootSummaryPointersThroughPointerStore() {
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "0");
    gc.jobIndexBackend = new RootSummaryBlindBackend(jobIndexBackend);
    long now = System.currentTimeMillis() - 10_000L;
    String jobId = "job-root-summary-expired";
    String dedupeHash = hashValue(ACCOUNT_ID + "|" + CONNECTOR_ID + "|root-summary|*|*|");
    String readyPointer =
        Keys.reconcileReadyPointerByDue(now - 1_000L, ACCOUNT_ID, "lane-native", jobId);
    StoredReconcileJob record = storedJob(jobId, "JS_FAILED", now, "", dedupeHash, readyPointer);
    putNativeJobIndexRows(record);
    String token = String.format("%019d-%s", Long.MAX_VALUE - now, jobId);
    String byAccount = Keys.reconcileRootJobSummaryByAccountPointer(ACCOUNT_ID, token);
    String byConnector =
        Keys.reconcileRootJobSummaryByConnectorPointer(ACCOUNT_ID, CONNECTOR_ID, token);
    String projection = Keys.reconcileJobProjectionPointer(ACCOUNT_ID, jobId);
    putPointer(byAccount, "inline:reconcile-job-list-summary:account");
    putPointer(byConnector, "inline:reconcile-job-list-summary:connector");
    putPointer(projection, "inline:reconcile-job-projection:projection");

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.expired() >= 1);
    assertTrue(pointers.get(byAccount).isEmpty());
    assertTrue(pointers.get(byConnector).isEmpty());
    assertTrue(pointers.get(projection).isEmpty());
  }

  @Test
  void accountSliceRemovesOrphanRootSummaryPointers() {
    long createdAtMs = System.currentTimeMillis() - 10_000L;
    String jobId = "job-orphan-root-summary";
    String token = String.format("%019d-%s", Long.MAX_VALUE - createdAtMs, jobId);
    String byAccount = Keys.reconcileRootJobSummaryByAccountPointer(ACCOUNT_ID, token);
    String byConnector =
        Keys.reconcileRootJobSummaryByConnectorPointer(ACCOUNT_ID, CONNECTOR_ID, token);
    String projection = Keys.reconcileJobProjectionPointer(ACCOUNT_ID, jobId);
    String encoded = encodeRootSummary(jobId, createdAtMs);
    putPointer(byAccount, encoded);
    putPointer(byConnector, encoded);
    putPointer(projection, "inline:reconcile-job-projection:projection");

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.ptrDeleted() >= 3);
    assertTrue(pointers.get(byAccount).isEmpty());
    assertTrue(pointers.get(byConnector).isEmpty());
    assertTrue(pointers.get(projection).isEmpty());
  }

  @Test
  void accountSliceRemovesConnectorOnlyOrphanRootSummaryPointers() {
    long createdAtMs = System.currentTimeMillis() - 10_000L;
    String jobId = "job-connector-only-orphan-root-summary";
    String token = String.format("%019d-%s", Long.MAX_VALUE - createdAtMs, jobId);
    String byConnector =
        Keys.reconcileRootJobSummaryByConnectorPointer(ACCOUNT_ID, CONNECTOR_ID, token);
    String projection = Keys.reconcileJobProjectionPointer(ACCOUNT_ID, jobId);
    String encoded = encodeRootSummary(jobId, createdAtMs);
    putPointer(byConnector, encoded);
    putPointer(projection, "inline:reconcile-job-projection:projection");

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.ptrDeleted() >= 2);
    assertTrue(pointers.get(byConnector).isEmpty());
    assertTrue(pointers.get(projection).isEmpty());
  }

  @Test
  void accountSliceContinuationDoesNotSkipConnectorSummariesAfterPartialPage() {
    System.setProperty("floecat.gc.reconcile-jobs.page-size", "5");
    System.setProperty("floecat.gc.reconcile-jobs.batch-limit", "4");
    long createdAtMs = System.currentTimeMillis() - 10_000L;
    java.util.ArrayList<String> accountSummaryKeys = new java.util.ArrayList<>();
    java.util.ArrayList<String> connectorSummaryKeys = new java.util.ArrayList<>();

    for (int i = 0; i < 3; i++) {
      String jobId = "job-account-orphan-" + i;
      String token = String.format("%019d-%s", Long.MAX_VALUE - createdAtMs - i, jobId);
      String key = Keys.reconcileRootJobSummaryByAccountPointer(ACCOUNT_ID, token);
      accountSummaryKeys.add(key);
      putPointer(key, encodeRootSummary(jobId, createdAtMs + i));
    }
    for (int i = 0; i < 5; i++) {
      String jobId = "job-connector-orphan-" + i;
      String token = String.format("%019d-%s", Long.MAX_VALUE - createdAtMs - 100 - i, jobId);
      String key = Keys.reconcileRootJobSummaryByConnectorPointer(ACCOUNT_ID, CONNECTOR_ID, token);
      connectorSummaryKeys.add(key);
      putPointer(key, encodeRootSummary(jobId, createdAtMs + 100 + i));
    }

    String jobToken = "";
    String dedupeToken = "";
    String rootSummaryToken = "";
    String connectorRootSummaryToken = "";
    for (int i = 0; i < 4; i++) {
      var result =
          gc.runAccountSlice(
              ACCOUNT_ID, jobToken, dedupeToken, rootSummaryToken, connectorRootSummaryToken);
      jobToken = result.nextJobToken();
      dedupeToken = result.nextDedupeToken();
      rootSummaryToken = result.nextRootSummaryToken();
      connectorRootSummaryToken = result.nextConnectorRootSummaryToken();
      if ((jobToken == null || jobToken.isBlank())
          && (dedupeToken == null || dedupeToken.isBlank())
          && (rootSummaryToken == null || rootSummaryToken.isBlank())
          && (connectorRootSummaryToken == null || connectorRootSummaryToken.isBlank())) {
        break;
      }
    }

    for (String key : accountSummaryKeys) {
      assertTrue(pointers.get(key).isEmpty(), key);
    }
    for (String key : connectorSummaryKeys) {
      assertTrue(pointers.get(key).isEmpty(), key);
    }
  }

  @Test
  void accountSliceDoesNotDeleteRootSummaryWhenCanonicalIsRecreatedDuringDelete() {
    long createdAtMs = System.currentTimeMillis() - 10_000L;
    String jobId = "job-root-summary-recreated";
    String token = String.format("%019d-%s", Long.MAX_VALUE - createdAtMs, jobId);
    String byAccount = Keys.reconcileRootJobSummaryByAccountPointer(ACCOUNT_ID, token);
    String projection = Keys.reconcileJobProjectionPointer(ACCOUNT_ID, jobId);
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    putPointer(byAccount, encodeRootSummary(jobId, createdAtMs));
    putPointer(projection, "inline:reconcile-job-projection:projection");
    StoredReconcileJob replacement =
        storedJob(jobId, "JS_RUNNING", System.currentTimeMillis(), "", "", "");
    String replacementReference = inlineJobReference(replacement);
    gc.jobIndexBackend =
        new RecreatingCanonicalBeforeMixedBatchBackend(
            jobIndexBackend,
            () ->
                pointers.compareAndSet(
                    canonicalKey,
                    0L,
                    PointerReferences.inlineJsonPointer(canonicalKey, replacementReference, 1L)));

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, result.ptrDeleted());
    assertTrue(pointers.get(canonicalKey).isPresent());
    assertTrue(pointers.get(byAccount).isPresent());
    assertTrue(pointers.get(projection).isPresent());
  }

  @Test
  void accountSliceKeepsUnreadableRootSummaryPointers() {
    long createdAtMs = System.currentTimeMillis() - 10_000L;
    String jobId = "job-unreadable-root-summary";
    String token = String.format("%019d-%s", Long.MAX_VALUE - createdAtMs, jobId);
    String byAccount = Keys.reconcileRootJobSummaryByAccountPointer(ACCOUNT_ID, token);
    putPointer(byAccount, "inline:reconcile-job-list-summary:not-valid-base64");

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, result.ptrDeleted());
    assertEquals(1, result.rootSummaryQuarantined());
    assertTrue(pointers.get(byAccount).isPresent());
  }

  @Test
  void accountSliceKeepsNativeSecondaryRowsWhenCanonicalPayloadIsUnreadable() {
    String jobId = "job-native-corrupt";
    String parentJobId = "parent-corrupt";
    long now = System.currentTimeMillis() - 10_000L;
    String dedupeHash = hashValue(ACCOUNT_ID + "|" + CONNECTOR_ID + "|full|*|*|");
    String readyPointer =
        Keys.reconcileReadyPointerByDue(now - 1_000L, ACCOUNT_ID, "lane-native", jobId);
    StoredReconcileJob record =
        storedJob(jobId, "JS_SUCCEEDED", now, parentJobId, dedupeHash, readyPointer);
    putNativeJobIndexRows(record);

    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer canonical = pointers.get(canonicalKey).orElseThrow();
    assertTrue(
        pointers.compareAndSet(
            canonicalKey,
            canonical.getVersion(),
            PointerReferences.inlineJsonPointer(
                canonicalKey, "inline:reconcile-job:not-valid", canonical.getVersion() + 1L)));

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, result.ptrDeleted());
    assertEquals(1, result.canonicalQuarantined());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId))
            .isPresent());
    assertTrue(
        jobIndexBackend.loadIndexEntry(Keys.reconcileJobLookupPointerById(jobId)).isPresent());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileJobByParentPointer(ACCOUNT_ID, parentJobId, jobId))
            .isPresent());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(
                Keys.reconcileJobByConnectorPointer(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    String.format("%019d-%s", Long.MAX_VALUE - now, jobId)))
            .isPresent());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileJobByStatePointer("JS_SUCCEEDED", now, ACCOUNT_ID, jobId))
            .isPresent());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(
                Keys.reconcileJobByAccountStatePointer(ACCOUNT_ID, "JS_SUCCEEDED", now, jobId))
            .isPresent());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(
                Keys.reconcileJobByConnectorStatePointer(
                    ACCOUNT_ID, CONNECTOR_ID, "JS_SUCCEEDED", now, jobId))
            .isPresent());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileDedupePointer(ACCOUNT_ID, dedupeHash))
            .isPresent());
  }

  @Test
  void accountSliceKeepsLiveInlineJobs() {
    String jobId = "job-inline-running";
    String canonicalKey =
        putInlineReconcileJob(jobId, "JS_RUNNING", System.currentTimeMillis(), "", "");
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, result.ptrDeleted());
    assertTrue(pointers.get(canonicalKey).isPresent());
    assertTrue(pointers.get(lookupKey).isPresent());
  }

  @Test
  void readySliceKeepsCurrentInlineQueuedReadyPointer() {
    String jobId = "job-inline-queued";
    String readyKey =
        Keys.reconcileReadyPointerByDue(System.currentTimeMillis(), ACCOUNT_ID, "lane", jobId);
    String canonicalKey =
        putInlineReconcileJob(jobId, "JS_QUEUED", System.currentTimeMillis(), "", readyKey);
    putPointer(readyKey, canonicalKey);

    var result = gc.runReadySlice("");

    assertEquals(0, result.deleted());
    assertTrue(pointers.get(readyKey).isPresent());
  }

  @Test
  void readySliceDoesNotDeleteQueuedReadyPointerThatRecordDoesNotYetReflect() {
    String jobId = "job-inline-queued-race";
    long now = System.currentTimeMillis();
    String oldReadyKey = Keys.reconcileReadyPointerByDue(now - 1_000L, ACCOUNT_ID, "lane", jobId);
    String newReadyKey = Keys.reconcileReadyPointerByDue(now, ACCOUNT_ID, "lane", jobId);
    String canonicalKey = putInlineReconcileJob(jobId, "JS_QUEUED", now, "", oldReadyKey);
    putPointer(newReadyKey, canonicalKey);

    var result = gc.runReadySlice("");

    assertEquals(0, result.deleted());
    assertTrue(pointers.get(newReadyKey).isPresent());
  }

  @Test
  void readySliceKeepsCurrentInlineQueuedSecondaryReadyPointers() {
    long dueAtMs = System.currentTimeMillis();
    String jobId = "job-inline-queued-secondary";
    String laneKey = "lane";
    String executionClass = "BATCH";
    String executionLane = "connector:lane";
    String jobKind = "PLAN_TABLE";
    String readyKey = Keys.reconcileReadyPointerByDue(dueAtMs, ACCOUNT_ID, laneKey, jobId);
    String executionClassReadyKey =
        Keys.reconcileReadyByExecutionClassPointerByDue(dueAtMs, executionClass, ACCOUNT_ID, jobId);
    String executionLaneReadyKey =
        Keys.reconcileReadyByExecutionLanePointerByDue(dueAtMs, executionLane, ACCOUNT_ID, jobId);
    String jobKindReadyKey =
        Keys.reconcileReadyByJobKindPointerByDue(dueAtMs, jobKind, ACCOUNT_ID, jobId);
    String canonicalKey =
        putInlineReconcileJob(
            jobId,
            "JS_QUEUED",
            dueAtMs,
            "",
            readyKey,
            laneKey,
            executionClass,
            executionLane,
            "",
            jobKind,
            dueAtMs);
    putPointer(readyKey, canonicalKey);
    putPointer(executionClassReadyKey, canonicalKey);
    putPointer(executionLaneReadyKey, canonicalKey);
    putPointer(jobKindReadyKey, canonicalKey);

    var result = gc.runReadySlice("");

    assertEquals(0, result.deleted());
    assertTrue(pointers.get(readyKey).isPresent());
    assertTrue(pointers.get(executionClassReadyKey).isPresent());
    assertTrue(pointers.get(executionLaneReadyKey).isPresent());
    assertTrue(pointers.get(jobKindReadyKey).isPresent());
  }

  @Test
  void accountSliceDeletesHistoricalReadyPointersForTerminalInlineJobs() {
    long finishedAtMs = System.currentTimeMillis() - 10_000L;
    String jobId = "job-inline-terminal-ready-cleanup";
    String laneKey = "lane";
    String executionClass = "BATCH";
    String executionLane = "connector:lane";
    String jobKind = "PLAN_TABLE";
    String readyKey = Keys.reconcileReadyPointerByDue(finishedAtMs, ACCOUNT_ID, laneKey, jobId);
    String executionClassReadyKey =
        Keys.reconcileReadyByExecutionClassPointerByDue(
            finishedAtMs, executionClass, ACCOUNT_ID, jobId);
    String executionLaneReadyKey =
        Keys.reconcileReadyByExecutionLanePointerByDue(
            finishedAtMs, executionLane, ACCOUNT_ID, jobId);
    String jobKindReadyKey =
        Keys.reconcileReadyByJobKindPointerByDue(finishedAtMs, jobKind, ACCOUNT_ID, jobId);
    String canonicalKey =
        putInlineReconcileJob(
            jobId,
            "JS_SUCCEEDED",
            finishedAtMs,
            "",
            readyKey,
            laneKey,
            executionClass,
            executionLane,
            "",
            jobKind,
            finishedAtMs);
    putPointer(readyKey, canonicalKey);
    putPointer(executionClassReadyKey, canonicalKey);
    putPointer(executionLaneReadyKey, canonicalKey);
    putPointer(jobKindReadyKey, canonicalKey);

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.readyDeleted() >= 4);
    assertTrue(pointers.get(readyKey).isEmpty());
    assertTrue(pointers.get(executionClassReadyKey).isEmpty());
    assertTrue(pointers.get(executionLaneReadyKey).isEmpty());
    assertTrue(pointers.get(jobKindReadyKey).isEmpty());
  }

  @Test
  void indexBackendDefaultRejectsExtraPointerOps() {
    ReconcileJobIndexBackend backend =
        new ReconcileJobIndexBackend() {
          @Override
          public java.util.Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
            return java.util.Optional.empty();
          }

          @Override
          public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
            return true;
          }

          @Override
          public JobIndexQueryPage listCanonicalEntries(
              String accountId, int limit, String pageToken) {
            return new JobIndexQueryPage(java.util.List.of(), "");
          }

          @Override
          public JobIndexQueryPage listDedupeEntries(
              String accountId, int limit, String pageToken) {
            return new JobIndexQueryPage(java.util.List.of(), "");
          }

          @Override
          public JobIndexQueryPage listParentEntries(
              String accountId, String parentJobId, int limit, String pageToken) {
            return new JobIndexQueryPage(java.util.List.of(), "");
          }

          @Override
          public JobIndexQueryPage listConnectorEntries(
              String accountId, String connectorId, int limit, String pageToken) {
            return new JobIndexQueryPage(java.util.List.of(), "");
          }

          @Override
          public JobIndexQueryPage listGlobalStateEntries(
              String state, int limit, String pageToken) {
            return new JobIndexQueryPage(java.util.List.of(), "");
          }

          @Override
          public JobIndexQueryPage listAccountStateEntries(
              String accountId, String state, int limit, String pageToken) {
            return new JobIndexQueryPage(java.util.List.of(), "");
          }

          @Override
          public JobIndexQueryPage listConnectorStateEntries(
              String accountId, String connectorId, String state, int limit, String pageToken) {
            return new JobIndexQueryPage(java.util.List.of(), "");
          }
        };

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            backend.compareAndSetBatch(
                ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
                java.util.List.of(
                    new PointerStore.CasCheckAbsent(
                        Keys.reconcileJobProjectionPointer(ACCOUNT_ID, "missing-job")))));
  }

  private void putPointer(String key, String blobUri) {
    Pointer ptr = PointerReferences.pointerKeyPointer(key, blobUri, 1L);
    pointers.compareAndSet(key, 0L, ptr);
  }

  private void putQuarantineMarker(
      String markerKey, String canonicalKey, long canonicalVersion, String canonicalReference) {
    String payload =
        canonicalVersion
            + "\n"
            + canonicalReference
            + "\n"
            + System.currentTimeMillis()
            + "\n"
            + canonicalKey;
    pointers.compareAndSet(
        markerKey, 0L, PointerReferences.opaqueMarkerPointer(markerKey, payload, 1L));
  }

  private String putInlineReconcileJob(
      String jobId, String state, long updatedAtMs, String dedupeKeyHash, String readyPointerKey) {
    return putInlineReconcileJob(
        jobId, state, updatedAtMs, dedupeKeyHash, readyPointerKey, "", "", "", "", "", 0L);
  }

  private String putInlineReconcileJob(
      String jobId,
      String state,
      long updatedAtMs,
      String dedupeKeyHash,
      String readyPointerKey,
      String laneKey,
      String executionClass,
      String executionLane,
      String pinnedExecutorId,
      String jobKind,
      long nextAttemptAtMs) {
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);

    ObjectNode record = mapper.createObjectNode();
    record.put("jobId", jobId);
    record.put("accountId", ACCOUNT_ID);
    record.put("connectorId", CONNECTOR_ID);
    record.put("state", state);
    record.put("updatedAtMs", updatedAtMs);
    record.put("createdAtMs", updatedAtMs);
    record.put("dedupeKeyHash", dedupeKeyHash);
    record.put("readyPointerKey", readyPointerKey);
    record.put("laneKey", laneKey);
    record.put("executionClass", executionClass);
    record.put("executionLane", executionLane);
    record.put("pinnedExecutorId", pinnedExecutorId);
    record.put("jobKind", jobKind);
    if (nextAttemptAtMs > 0L) {
      record.put("nextAttemptAtMs", nextAttemptAtMs);
    }

    String inlineReference =
        INLINE_JOB_STATE_PREFIX
            + Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(record.toString().getBytes(StandardCharsets.UTF_8));
    pointers.compareAndSet(
        canonicalKey, 0L, PointerReferences.inlineJsonPointer(canonicalKey, inlineReference, 1L));
    putPointer(lookupKey, canonicalKey);
    return canonicalKey;
  }

  private StoredReconcileJob storedJob(
      String jobId,
      String state,
      long createdAtMs,
      String parentJobId,
      String dedupeKeyHash,
      String readyPointerKey) {
    StoredReconcileJob record = new StoredReconcileJob();
    record.jobId = jobId;
    record.accountId = ACCOUNT_ID;
    record.connectorId = CONNECTOR_ID;
    record.parentJobId = parentJobId;
    record.state = state;
    record.createdAtMs = createdAtMs;
    record.updatedAtMs = createdAtMs;
    record.dedupeKeyHash = dedupeKeyHash;
    record.readyPointerKey = readyPointerKey;
    record.laneKey = "lane-native";
    return record;
  }

  private void putNativeJobIndexRows(StoredReconcileJob record) {
    putNativeJobIndexRows(record, true);
  }

  private void putNativeJobIndexRows(StoredReconcileJob record, boolean includeCleanupManifest) {
    String canonicalKey = Keys.reconcileJobPointerById(record.accountId, record.jobId);
    String inlineReference =
        INLINE_JOB_STATE_PREFIX
            + Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(serialize(record).getBytes(StandardCharsets.UTF_8));
    java.util.ArrayList<ReconcileJobIndexStore.JobIndexWriteOp> writes =
        new java.util.ArrayList<>();
    writes.add(
        new ReconcileJobIndexStore.JobIndexUpsert(
            canonicalKey, 0L, inlineReference, PointerReferenceKind.PRK_INLINE_JSON));
    writes.add(
        new ReconcileJobIndexStore.JobIndexUpsert(
            Keys.reconcileJobLookupPointerById(record.jobId),
            0L,
            canonicalKey,
            PointerReferenceKind.PRK_POINTER_KEY));
    if (record.parentJobId != null && !record.parentJobId.isBlank()) {
      writes.add(
          new ReconcileJobIndexStore.JobIndexUpsert(
              Keys.reconcileJobByParentPointer(record.accountId, record.parentJobId, record.jobId),
              0L,
              canonicalKey,
              PointerReferenceKind.PRK_POINTER_KEY));
    }
    writes.add(
        new ReconcileJobIndexStore.JobIndexUpsert(
            Keys.reconcileJobByConnectorPointer(
                record.accountId,
                record.connectorId,
                String.format("%019d-%s", Long.MAX_VALUE - record.createdAtMs, record.jobId)),
            0L,
            canonicalKey,
            PointerReferenceKind.PRK_POINTER_KEY));
    if (record.dedupeKeyHash != null && !record.dedupeKeyHash.isBlank()) {
      writes.add(
          new ReconcileJobIndexStore.JobIndexUpsert(
              Keys.reconcileDedupePointer(record.accountId, record.dedupeKeyHash),
              0L,
              canonicalKey,
              PointerReferenceKind.PRK_POINTER_KEY));
    }
    writes.add(
        new ReconcileJobIndexStore.JobIndexUpsert(
            Keys.reconcileJobByStatePointer(
                record.state, record.createdAtMs, record.accountId, record.jobId),
            0L,
            canonicalKey,
            PointerReferenceKind.PRK_POINTER_KEY));
    writes.add(
        new ReconcileJobIndexStore.JobIndexUpsert(
            Keys.reconcileJobByAccountStatePointer(
                record.accountId, record.state, record.createdAtMs, record.jobId),
            0L,
            canonicalKey,
            PointerReferenceKind.PRK_POINTER_KEY));
    writes.add(
        new ReconcileJobIndexStore.JobIndexUpsert(
            Keys.reconcileJobByConnectorStatePointer(
                record.accountId,
                record.connectorId,
                record.state,
                record.createdAtMs,
                record.jobId),
            0L,
            canonicalKey,
            PointerReferenceKind.PRK_POINTER_KEY));
    java.util.List<ReconcileJobIndexStore.ReadyQueueWrite> readyWrites =
        record.readyPointerKey == null || record.readyPointerKey.isBlank()
            ? java.util.List.of()
            : java.util.List.of(
                new ReconcileJobIndexStore.ReadyQueueWrite(
                    record.readyPointerKey, canonicalKey, PointerReferenceKind.PRK_POINTER_KEY));
    if (includeCleanupManifest) {
      java.util.LinkedHashSet<String> cleanupIndexKeys = new java.util.LinkedHashSet<>();
      cleanupIndexKeys.add(Keys.reconcileJobLookupPointerById(record.jobId));
      if (record.parentJobId != null && !record.parentJobId.isBlank()) {
        cleanupIndexKeys.add(
            Keys.reconcileJobByParentPointer(record.accountId, record.parentJobId, record.jobId));
      }
      cleanupIndexKeys.add(
          Keys.reconcileJobByConnectorPointer(
              record.accountId,
              record.connectorId,
              String.format("%019d-%s", Long.MAX_VALUE - record.createdAtMs, record.jobId)));
      cleanupIndexKeys.add(
          Keys.reconcileJobByStatePointer(
              record.state, record.createdAtMs, record.accountId, record.jobId));
      cleanupIndexKeys.add(
          Keys.reconcileJobByAccountStatePointer(
              record.accountId, record.state, record.createdAtMs, record.jobId));
      cleanupIndexKeys.add(
          Keys.reconcileJobByConnectorStatePointer(
              record.accountId,
              record.connectorId,
              record.state,
              record.createdAtMs,
              record.jobId));
      if (record.dedupeKeyHash != null && !record.dedupeKeyHash.isBlank()) {
        cleanupIndexKeys.add(Keys.reconcileDedupePointer(record.accountId, record.dedupeKeyHash));
      }
      writes.set(
          0,
          new ReconcileJobIndexStore.JobIndexUpsert(
              canonicalKey,
              0L,
              inlineReference,
              PointerReferenceKind.PRK_INLINE_JSON,
              new ReconcileJobIndexCleanupManifest(
                  java.util.List.copyOf(cleanupIndexKeys),
                  readyWrites.stream()
                      .map(ReconcileJobIndexStore.ReadyQueueWrite::readyPointerKey)
                      .toList())));
    }
    assertTrue(
        jobIndexBackend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                writes,
                new ReconcileJobIndexStore.ReadyQueueMutation(readyWrites, java.util.List.of()))));
  }

  private String serialize(StoredReconcileJob record) {
    try {
      return mapper.writeValueAsString(record);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String inlineJobReference(StoredReconcileJob record) {
    return INLINE_JOB_STATE_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(serialize(record).getBytes(StandardCharsets.UTF_8));
  }

  private String encodeRootSummary(String jobId, long createdAtMs) {
    StoredReconcileJobListSummary summary =
        new StoredReconcileJobListSummary(
            ACCOUNT_ID,
            jobId,
            CONNECTOR_ID,
            "JS_FAILED",
            "failed",
            createdAtMs,
            createdAtMs,
            0L,
            0L,
            0L,
            0L,
            1L,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            0L,
            0L,
            0L,
            "",
            ReconcileExecutionClass.BATCH,
            "",
            java.util.Map.of(),
            ReconcileJobKind.PLAN_CONNECTOR,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            createdAtMs);
    try {
      return "inline:reconcile-job-list-summary:"
          + Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(mapper.writeValueAsString(summary).getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  private CountingMemoryJobIndexBackend useCountingJobIndexBackend(boolean rejectFirstBatch) {
    CountingMemoryJobIndexBackend backend = new CountingMemoryJobIndexBackend(rejectFirstBatch);
    backend.bind(pointers);
    jobIndexBackend = backend;
    gc.jobIndexBackend = backend;
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(blobs, pointers, mapper);
    gc.jobIndexStore = new NativeReconcileJobIndexStore();
    gc.jobIndexStore.bind(
        backend,
        payloadStore,
        gc.jobIndexes,
        16,
        (previous, current) -> {},
        (previous, current, operation) -> {});
    return backend;
  }

  private static final class CountingMemoryJobIndexBackend extends MemoryReconcileJobIndexBackend {
    private final boolean rejectFirstBatch;
    private int batchedDeleteCalls;
    private int singleDeleteCalls;

    private CountingMemoryJobIndexBackend(boolean rejectFirstBatch) {
      this.rejectFirstBatch = rejectFirstBatch;
    }

    @Override
    public synchronized boolean compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch batch,
        java.util.List<PointerStore.CasOp> extraPointerOps) {
      long canonicalDeletes =
          batch == null
              ? 0L
              : batch.writes().stream()
                  .filter(
                      write ->
                          write instanceof ReconcileJobIndexStore.JobIndexDelete delete
                              && delete
                                  .pointerKey()
                                  .startsWith(Keys.reconcileJobPointerByIdPrefix(ACCOUNT_ID)))
                  .count();
      if (canonicalDeletes > 1L) {
        batchedDeleteCalls++;
        if (rejectFirstBatch && batchedDeleteCalls == 1) {
          return false;
        }
      } else if (canonicalDeletes == 1L) {
        singleDeleteCalls++;
      }
      return super.compareAndSetBatch(batch, extraPointerOps);
    }
  }

  private static final class ReplacingBeforeQuarantineDeleteBackend
      implements ReconcileJobIndexBackend {
    private final ReconcileJobIndexBackend delegate;
    private final PointerStore pointers;
    private final String canonicalKey;
    private final String replacementReference;
    private boolean replaced;

    private ReplacingBeforeQuarantineDeleteBackend(
        ReconcileJobIndexBackend delegate,
        PointerStore pointers,
        String canonicalKey,
        String replacementReference) {
      this.delegate = delegate;
      this.pointers = pointers;
      this.canonicalKey = canonicalKey;
      this.replacementReference = replacementReference;
    }

    @Override
    public java.util.Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
      return delegate.loadIndexEntry(pointerKey);
    }

    @Override
    public ReconcileJobIndexCleanupManifest loadCleanupManifest(String canonicalPointerKey) {
      return delegate.loadCleanupManifest(canonicalPointerKey);
    }

    @Override
    public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
      replaceBeforeCanonicalDelete(batch);
      return delegate.compareAndSetBatch(batch);
    }

    @Override
    public boolean compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch batch,
        java.util.List<PointerStore.CasOp> extraPointerOps) {
      replaceBeforeCanonicalDelete(batch);
      return delegate.compareAndSetBatch(batch, extraPointerOps);
    }

    private void replaceBeforeCanonicalDelete(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
      if (replaced || batch == null) {
        return;
      }
      boolean deletesCanonical =
          batch.writes().stream()
              .anyMatch(
                  write ->
                      write instanceof ReconcileJobIndexStore.JobIndexDelete delete
                          && canonicalKey.equals(delete.pointerKey()));
      if (!deletesCanonical) {
        return;
      }
      replaced = true;
      Pointer current = pointers.get(canonicalKey).orElseThrow();
      pointers.compareAndSet(
          canonicalKey,
          current.getVersion(),
          PointerReferences.inlineJsonPointer(
              canonicalKey, replacementReference, current.getVersion() + 1L));
    }

    @Override
    public JobIndexQueryPage listCanonicalEntries(String accountId, int limit, String pageToken) {
      return delegate.listCanonicalEntries(accountId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listDedupeEntries(String accountId, int limit, String pageToken) {
      return delegate.listDedupeEntries(accountId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listParentEntries(
        String accountId, String parentJobId, int limit, String pageToken) {
      return delegate.listParentEntries(accountId, parentJobId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listConnectorEntries(
        String accountId, String connectorId, int limit, String pageToken) {
      return delegate.listConnectorEntries(accountId, connectorId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listGlobalStateEntries(String state, int limit, String pageToken) {
      return delegate.listGlobalStateEntries(state, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listAccountStateEntries(
        String accountId, String state, int limit, String pageToken) {
      return delegate.listAccountStateEntries(accountId, state, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listConnectorStateEntries(
        String accountId, String connectorId, String state, int limit, String pageToken) {
      return delegate.listConnectorStateEntries(accountId, connectorId, state, limit, pageToken);
    }
  }

  private static final class RecordingReadyQueueBackend implements ReconcileReadyQueueBackend {
    private final java.util.List<ReconcileReadyQueueStore.ReadyQueueEntry> entries;
    private final java.util.LinkedHashSet<String> deletedKeys = new java.util.LinkedHashSet<>();

    private RecordingReadyQueueBackend(
        java.util.List<ReconcileReadyQueueStore.ReadyQueueEntry> entries) {
      this.entries = java.util.List.copyOf(entries);
    }

    @Override
    public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
        ReadyQueueSlice slice,
        int pageSize,
        String pageToken,
        ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      return new ReconcileReadyQueueStore.ReadyQueueScanPage(java.util.List.of(), "");
    }

    @Override
    public ReadyQueueScanPage scanAllReadyEntries(int pageSize, String pageToken) {
      int offset = 0;
      if (pageToken != null && !pageToken.isBlank()) {
        offset = Integer.parseInt(pageToken);
      }
      if (offset >= entries.size()) {
        return new ReadyQueueScanPage(java.util.List.of(), "");
      }
      int end = Math.min(entries.size(), offset + Math.max(1, pageSize));
      String next = end >= entries.size() ? "" : Integer.toString(end);
      return new ReadyQueueScanPage(entries.subList(offset, end), next);
    }

    @Override
    public boolean deleteReadyEntry(String readyPointerKey) {
      deletedKeys.add(readyPointerKey);
      return true;
    }

    @Override
    public java.util.Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(
        String canonicalPointerKey, ReconcileReadyQueueStore.LeaseScanStats scanStats) {
      return java.util.Optional.empty();
    }
  }

  private static final class RepointingDedupeDuringListBackend implements ReconcileJobIndexBackend {
    private final ReconcileJobIndexBackend delegate;
    private final PointerStore pointers;
    private final String dedupeKey;
    private final String expectedOldCanonicalKey;
    private final String newCanonicalKey;
    private boolean repointed;

    private RepointingDedupeDuringListBackend(
        ReconcileJobIndexBackend delegate,
        PointerStore pointers,
        String dedupeKey,
        String expectedOldCanonicalKey,
        String newCanonicalKey) {
      this.delegate = delegate;
      this.pointers = pointers;
      this.dedupeKey = dedupeKey;
      this.expectedOldCanonicalKey = expectedOldCanonicalKey;
      this.newCanonicalKey = newCanonicalKey;
    }

    @Override
    public java.util.Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
      return delegate.loadIndexEntry(pointerKey);
    }

    @Override
    public ReconcileJobIndexCleanupManifest loadCleanupManifest(String canonicalPointerKey) {
      return delegate.loadCleanupManifest(canonicalPointerKey);
    }

    @Override
    public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
      return delegate.compareAndSetBatch(batch);
    }

    @Override
    public boolean compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch batch,
        java.util.List<PointerStore.CasOp> extraPointerOps) {
      return delegate.compareAndSetBatch(batch, extraPointerOps);
    }

    @Override
    public JobIndexQueryPage listCanonicalEntries(String accountId, int limit, String pageToken) {
      return delegate.listCanonicalEntries(accountId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listDedupeEntries(String accountId, int limit, String pageToken) {
      JobIndexQueryPage page = delegate.listDedupeEntries(accountId, limit, pageToken);
      if (!repointed
          && page.entries().stream().anyMatch(entry -> dedupeKey.equals(entry.pointerKey()))) {
        repointed = true;
        Pointer current = pointers.get(dedupeKey).orElseThrow();
        assertEquals(expectedOldCanonicalKey, current.getBlobUri());
        assertTrue(
            pointers.compareAndSet(
                dedupeKey,
                current.getVersion(),
                PointerReferences.pointerKeyPointer(
                    dedupeKey, newCanonicalKey, current.getVersion() + 1L)));
      }
      return page;
    }

    @Override
    public JobIndexQueryPage listParentEntries(
        String accountId, String parentJobId, int limit, String pageToken) {
      return delegate.listParentEntries(accountId, parentJobId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listConnectorEntries(
        String accountId, String connectorId, int limit, String pageToken) {
      return delegate.listConnectorEntries(accountId, connectorId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listGlobalStateEntries(String state, int limit, String pageToken) {
      return delegate.listGlobalStateEntries(state, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listAccountStateEntries(
        String accountId, String state, int limit, String pageToken) {
      return delegate.listAccountStateEntries(accountId, state, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listConnectorStateEntries(
        String accountId, String connectorId, String state, int limit, String pageToken) {
      return delegate.listConnectorStateEntries(accountId, connectorId, state, limit, pageToken);
    }
  }

  private static final class RecreatingCanonicalBeforeMixedBatchBackend
      implements ReconcileJobIndexBackend {
    private final ReconcileJobIndexBackend delegate;
    private final Runnable beforeBatch;
    private boolean recreated;

    private RecreatingCanonicalBeforeMixedBatchBackend(
        ReconcileJobIndexBackend delegate, Runnable beforeBatch) {
      this.delegate = delegate;
      this.beforeBatch = beforeBatch;
    }

    @Override
    public java.util.Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
      return delegate.loadIndexEntry(pointerKey);
    }

    @Override
    public ReconcileJobIndexCleanupManifest loadCleanupManifest(String canonicalPointerKey) {
      return delegate.loadCleanupManifest(canonicalPointerKey);
    }

    @Override
    public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
      return delegate.compareAndSetBatch(batch);
    }

    @Override
    public boolean compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch batch,
        java.util.List<PointerStore.CasOp> extraPointerOps) {
      if (!recreated) {
        recreated = true;
        beforeBatch.run();
      }
      return delegate.compareAndSetBatch(batch, extraPointerOps);
    }

    @Override
    public JobIndexQueryPage listCanonicalEntries(String accountId, int limit, String pageToken) {
      return delegate.listCanonicalEntries(accountId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listDedupeEntries(String accountId, int limit, String pageToken) {
      return delegate.listDedupeEntries(accountId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listParentEntries(
        String accountId, String parentJobId, int limit, String pageToken) {
      return delegate.listParentEntries(accountId, parentJobId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listConnectorEntries(
        String accountId, String connectorId, int limit, String pageToken) {
      return delegate.listConnectorEntries(accountId, connectorId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listGlobalStateEntries(String state, int limit, String pageToken) {
      return delegate.listGlobalStateEntries(state, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listAccountStateEntries(
        String accountId, String state, int limit, String pageToken) {
      return delegate.listAccountStateEntries(accountId, state, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listConnectorStateEntries(
        String accountId, String connectorId, String state, int limit, String pageToken) {
      return delegate.listConnectorStateEntries(accountId, connectorId, state, limit, pageToken);
    }
  }

  private static final class RootSummaryBlindBackend implements ReconcileJobIndexBackend {
    private final ReconcileJobIndexBackend delegate;

    private RootSummaryBlindBackend(ReconcileJobIndexBackend delegate) {
      this.delegate = delegate;
    }

    @Override
    public java.util.Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
      if (pointerKey != null && pointerKey.contains("/reconcile/jobs/root-summaries/")) {
        return java.util.Optional.empty();
      }
      return delegate.loadIndexEntry(pointerKey);
    }

    @Override
    public ReconcileJobIndexCleanupManifest loadCleanupManifest(String canonicalPointerKey) {
      return delegate.loadCleanupManifest(canonicalPointerKey);
    }

    @Override
    public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
      return delegate.compareAndSetBatch(batch);
    }

    @Override
    public boolean compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch batch,
        java.util.List<PointerStore.CasOp> extraPointerOps) {
      return delegate.compareAndSetBatch(batch, extraPointerOps);
    }

    @Override
    public JobIndexQueryPage listCanonicalEntries(String accountId, int limit, String pageToken) {
      return delegate.listCanonicalEntries(accountId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listDedupeEntries(String accountId, int limit, String pageToken) {
      return delegate.listDedupeEntries(accountId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listParentEntries(
        String accountId, String parentJobId, int limit, String pageToken) {
      return delegate.listParentEntries(accountId, parentJobId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listConnectorEntries(
        String accountId, String connectorId, int limit, String pageToken) {
      return delegate.listConnectorEntries(accountId, connectorId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listGlobalStateEntries(String state, int limit, String pageToken) {
      return delegate.listGlobalStateEntries(state, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listAccountStateEntries(
        String accountId, String state, int limit, String pageToken) {
      return delegate.listAccountStateEntries(accountId, state, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listConnectorStateEntries(
        String accountId, String connectorId, String state, int limit, String pageToken) {
      return delegate.listConnectorStateEntries(accountId, connectorId, state, limit, pageToken);
    }
  }
}
