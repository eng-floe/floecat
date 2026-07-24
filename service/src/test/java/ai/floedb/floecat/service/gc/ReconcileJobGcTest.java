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
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexEntrySnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.NativeReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexCleanupManifest;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
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
import java.util.List;
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
    System.clearProperty("floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms");
    System.clearProperty("floecat.gc.reconcile-jobs.retention-backfill-page-size");
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
  void idleAccountSliceDoesNotLoadNonDueTerminalJobs() {
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "86400000");
    StoredReconcileJob record =
        storedJob("job-not-due", "JS_SUCCEEDED", System.currentTimeMillis(), "", "", "");
    putNativeJobIndexRows(record);

    long started = System.nanoTime();
    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");
    long elapsedMs = java.time.Duration.ofNanos(System.nanoTime() - started).toMillis();

    assertEquals(0, result.scanned());
    assertEquals(0, result.expired());
    assertTrue(elapsedMs < 500L, "idle slice took " + elapsedMs + "ms");
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(Keys.reconcileJobPointerById(ACCOUNT_ID, record.jobId))
            .isPresent());
  }

  @Test
  void terminalRetentionBackfillIsBoundedAndCompletesOnce() {
    System.setProperty("floecat.gc.reconcile-jobs.retention-backfill-page-size", "1");
    StoredReconcileJob first = storedJob("legacy-terminal-a", "JS_SUCCEEDED", 100L, "", "", "");
    StoredReconcileJob second = storedJob("legacy-terminal-b", "JS_FAILED", 200L, "", "", "");
    putNativeJobIndexRows(first, false);
    putNativeJobIndexRows(second, false);
    assertTrue(
        jobIndexBackend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexDelete(
                        gc.jobIndexes.terminalRetentionPointerKey(first),
                        1L,
                        Keys.reconcileJobPointerById(ACCOUNT_ID, first.jobId),
                        "")),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));
    assertTrue(
        jobIndexBackend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexDelete(
                        gc.jobIndexes.terminalRetentionPointerKey(second),
                        1L,
                        Keys.reconcileJobPointerById(ACCOUNT_ID, second.jobId),
                        "")),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));

    var firstSlice = gc.runTerminalRetentionBackfillSlice(ACCOUNT_ID, "");
    assertEquals(1, firstSlice.scanned());
    assertFalse(firstSlice.complete());
    assertFalse(firstSlice.nextToken().isBlank());

    var secondSlice = gc.runTerminalRetentionBackfillSlice(ACCOUNT_ID, firstSlice.nextToken());
    assertEquals(1, secondSlice.scanned());
    assertTrue(secondSlice.complete());
    assertTrue(gc.terminalRetentionBackfillComplete(ACCOUNT_ID));
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(gc.jobIndexes.terminalRetentionPointerKey(first))
            .isPresent());
    assertTrue(
        jobIndexBackend
            .loadIndexEntry(gc.jobIndexes.terminalRetentionPointerKey(second))
            .isPresent());

    assertEquals(0, gc.runTerminalRetentionBackfillSlice(ACCOUNT_ID, "").scanned());
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
  void accountSliceStopsOversizedCleanupAtDeadlineAndRetriesPinnedPage() {
    java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(1_000L);
    DeadlineAdvancingMemoryJobIndexBackend deadlineBackend =
        new DeadlineAdvancingMemoryJobIndexBackend(now);
    deadlineBackend.bind(pointers);
    jobIndexBackend = deadlineBackend;
    gc.jobIndexBackend = deadlineBackend;
    gc.clock = now::get;
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(blobs, pointers, mapper);
    gc.jobIndexStore = new NativeReconcileJobIndexStore();
    gc.jobIndexStore.bind(
        deadlineBackend,
        payloadStore,
        gc.jobIndexes,
        16,
        (previous, current) -> {},
        (previous, current, operation) -> {});
    System.setProperty("floecat.gc.reconcile-jobs.page-size", "1");
    System.setProperty("floecat.gc.reconcile-jobs.slice-millis", "50");
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "1");

    putNativeJobIndexRows(storedJob("job-a-seed", "JS_RUNNING", 500L, "", "", ""));
    String jobId = "job-z-oversized";
    putNativeJobIndexRows(storedJob(jobId, "JS_SUCCEEDED", 500L, "", "hash-oversized", ""));
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    JobIndexEntrySnapshot canonical = deadlineBackend.loadIndexEntry(canonicalKey).orElseThrow();
    ReconcileJobIndexCleanupManifest originalManifest =
        deadlineBackend.loadCleanupManifest(canonicalKey);
    java.util.ArrayList<String> cleanupKeys =
        new java.util.ArrayList<>(originalManifest.indexPointerKeys());
    for (int i = 0; i < 150; i++) {
      String key = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "legacy-parent-" + i, jobId);
      putPointer(key, canonicalKey);
      cleanupKeys.add(key);
    }
    assertTrue(
        deadlineBackend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                java.util.List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        canonicalKey,
                        canonical.version(),
                        canonical.blobUri(),
                        PointerReferenceKind.PRK_INLINE_JSON,
                        new ReconcileJobIndexCleanupManifest(
                            cleanupKeys, originalManifest.readyPointerKeys()))),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));
    String pageStart = "";
    deadlineBackend.advanceCleanupPhases = true;

    var first = gc.runAccountSlice(ACCOUNT_ID, pageStart, "");

    assertEquals(0, first.expired());
    assertEquals("", first.nextJobToken());
    assertEquals(1, deadlineBackend.cleanupPhaseCalls);
    assertTrue(deadlineBackend.loadIndexEntry(canonicalKey).isPresent());

    var second = gc.runAccountSlice(ACCOUNT_ID, first.nextJobToken(), "");

    assertEquals(1, second.expired());
    assertTrue(deadlineBackend.loadIndexEntry(canonicalKey).isEmpty());
  }

  @Test
  void accountSliceStopsPreparingAtDeadlineAndStillCommitsOnePreparedCleanup() {
    java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(1_000L);
    SlowPreparationMemoryJobIndexBackend slowBackend =
        new SlowPreparationMemoryJobIndexBackend(now);
    slowBackend.bind(pointers);
    jobIndexBackend = slowBackend;
    gc.jobIndexBackend = slowBackend;
    gc.clock = now::get;
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(blobs, pointers, mapper);
    gc.jobIndexStore = new NativeReconcileJobIndexStore();
    gc.jobIndexStore.bind(
        slowBackend,
        payloadStore,
        gc.jobIndexes,
        16,
        (previous, current) -> {},
        (previous, current, operation) -> {});
    System.setProperty("floecat.gc.reconcile-jobs.slice-millis", "50");
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "1");
    String firstJobId = "job-a-slow-preparation";
    String secondJobId = "job-z-after-slow-preparation";
    putNativeJobIndexRows(storedJob(firstJobId, "JS_SUCCEEDED", 500L, "", "hash-slow-first", ""));
    putNativeJobIndexRows(storedJob(secondJobId, "JS_SUCCEEDED", 500L, "", "hash-slow-second", ""));

    var first = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(1, first.scanned());
    assertEquals(1, first.expired());
    assertEquals(1, slowBackend.preparations);
    assertTrue(slowBackend.lastCanonicalLimit > 1);
    assertTrue(
        slowBackend.loadIndexEntry(Keys.reconcileJobPointerById(ACCOUNT_ID, firstJobId)).isEmpty());
    assertTrue(
        slowBackend
            .loadIndexEntry(Keys.reconcileJobPointerById(ACCOUNT_ID, secondJobId))
            .isPresent());

    var second = gc.runAccountSlice(ACCOUNT_ID, first.nextJobToken(), "");

    assertEquals(1, second.scanned());
    assertEquals(1, second.expired());
    assertEquals(2, slowBackend.preparations);
    assertTrue(
        slowBackend
            .loadIndexEntry(Keys.reconcileJobPointerById(ACCOUNT_ID, secondJobId))
            .isEmpty());
  }

  @Test
  void accountSliceResumesAfterLastPreparedCanonicalWhenDeadlineCutsPage() {
    java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(1_000L);
    DeadlineAfterPageFetchMemoryJobIndexBackend deadlineBackend =
        new DeadlineAfterPageFetchMemoryJobIndexBackend(now);
    deadlineBackend.bind(pointers);
    jobIndexBackend = deadlineBackend;
    gc.jobIndexBackend = deadlineBackend;
    gc.clock = now::get;
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(blobs, pointers, mapper);
    gc.jobIndexStore = new NativeReconcileJobIndexStore();
    gc.jobIndexStore.bind(
        deadlineBackend,
        payloadStore,
        gc.jobIndexes,
        16,
        (previous, current) -> {},
        (previous, current, operation) -> {});
    System.setProperty("floecat.gc.reconcile-jobs.slice-millis", "50");
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "1");
    String firstJobId = "job-a-running-before-deadline";
    String secondJobId = "job-z-expired-after-deadline";
    String firstCanonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJobId);
    String secondCanonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, secondJobId);
    StoredReconcileJob firstRecord =
        storedJob(firstJobId, "JS_SUCCEEDED", 500L, "", "hash-before-deadline", "");
    putNativeJobIndexRows(firstRecord);
    putNativeJobIndexRows(
        storedJob(secondJobId, "JS_SUCCEEDED", 500L, "", "hash-after-deadline", ""));

    var first = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(1, first.scanned());
    assertEquals(1, first.expired());
    assertEquals(gc.jobIndexes.terminalRetentionPointerKey(firstRecord), first.nextJobToken());
    assertTrue(deadlineBackend.loadIndexEntry(firstCanonicalKey).isEmpty());
    assertTrue(deadlineBackend.loadIndexEntry(secondCanonicalKey).isPresent());

    var second = gc.runAccountSlice(ACCOUNT_ID, first.nextJobToken(), "");

    assertEquals(1, second.scanned());
    assertEquals(1, second.expired());
    assertTrue(deadlineBackend.loadIndexEntry(firstCanonicalKey).isEmpty());
    assertTrue(deadlineBackend.loadIndexEntry(secondCanonicalKey).isEmpty());
  }

  @Test
  void accountSliceResumesClaimedCleanupBeforeRecheckingStateAndDeletesQuarantineMarker() {
    String jobId = "job-claimed-cleanup";
    putNativeJobIndexRows(
        storedJob(jobId, "JS_RUNNING", System.currentTimeMillis(), "", "hash-claimed-cleanup", ""));
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    JobIndexEntrySnapshot canonical = jobIndexBackend.loadIndexEntry(canonicalKey).orElseThrow();
    assertFalse(
        gc.jobIndexStore
            .buildJobDeleteBatch(
                new CanonicalPointerSnapshot(
                    canonical.pointerKey(), canonical.blobUri(), canonical.version()))
            .writes()
            .isEmpty());
    JobIndexEntrySnapshot claimed = jobIndexBackend.loadIndexEntry(canonicalKey).orElseThrow();
    assertTrue(claimed.cleanupLocked());
    String markerKey =
        Keys.reconcileCanonicalQuarantinePointer(ACCOUNT_ID, hashValue(canonicalKey));
    putQuarantineMarker(markerKey, canonicalKey, claimed.version(), claimed.blobUri());

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(1, result.expired());
    assertTrue(jobIndexBackend.loadIndexEntry(canonicalKey).isEmpty());
    assertTrue(pointers.get(markerKey).isEmpty());
  }

  @Test
  void claimedCleanupDeletesManifestPointersWhenCanonicalBlobIsUnreadable() {
    String jobId = "job-claimed-unreadable";
    long createdAtMs = 123L;
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    String sortableToken = String.format("%019d-%s", Long.MAX_VALUE - createdAtMs, jobId);
    String projectionKey = Keys.reconcileJobProjectionPointer(ACCOUNT_ID, jobId);
    String accountSummaryKey =
        Keys.reconcileRootJobSummaryByAccountPointer(ACCOUNT_ID, sortableToken);
    String connectorSummaryKey =
        Keys.reconcileRootJobSummaryByConnectorPointer(ACCOUNT_ID, CONNECTOR_ID, sortableToken);
    putPointer(projectionKey, "projection");
    putPointer(accountSummaryKey, "unreadable-summary");
    putPointer(connectorSummaryKey, "unreadable-summary");
    assertTrue(
        jobIndexBackend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                java.util.List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        canonicalKey,
                        0L,
                        "unreadable-job-state",
                        PointerReferenceKind.PRK_INLINE_JSON,
                        new ReconcileJobIndexCleanupManifest(
                            java.util.List.of(),
                            java.util.List.of(),
                            java.util.List.of(
                                projectionKey, accountSummaryKey, connectorSummaryKey)))),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));
    JobIndexEntrySnapshot canonical = jobIndexBackend.loadIndexEntry(canonicalKey).orElseThrow();
    assertFalse(
        gc.jobIndexStore
            .buildJobDeleteBatch(
                new CanonicalPointerSnapshot(
                    canonical.pointerKey(), canonical.blobUri(), canonical.version()))
            .writes()
            .isEmpty());
    assertTrue(jobIndexBackend.loadIndexEntry(canonicalKey).orElseThrow().cleanupLocked());
    JobIndexEntrySnapshot claimed = jobIndexBackend.loadIndexEntry(canonicalKey).orElseThrow();
    putQuarantineMarker(
        Keys.reconcileCanonicalQuarantinePointer(ACCOUNT_ID, hashValue(canonicalKey)),
        canonicalKey,
        claimed.version(),
        claimed.blobUri());

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(1, result.expired());
    assertTrue(jobIndexBackend.loadIndexEntry(canonicalKey).isEmpty());
    assertTrue(pointers.get(projectionKey).isEmpty());
    assertTrue(pointers.get(accountSummaryKey).isEmpty());
    assertTrue(pointers.get(connectorSummaryKey).isEmpty());
  }

  @Test
  void accountSliceDoesNotPinPageWhenClaimedCleanupPreparationMustRetry() {
    PreparationFailingLockedMemoryBackend failingBackend =
        new PreparationFailingLockedMemoryBackend();
    failingBackend.bind(pointers);
    jobIndexBackend = failingBackend;
    gc.jobIndexBackend = failingBackend;
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(blobs, pointers, mapper);
    gc.jobIndexStore = new NativeReconcileJobIndexStore();
    gc.jobIndexStore.bind(
        failingBackend,
        payloadStore,
        gc.jobIndexes,
        16,
        (previous, current) -> {},
        (previous, current, operation) -> {});
    System.setProperty("floecat.gc.reconcile-jobs.page-size", "1");
    System.setProperty("floecat.gc.reconcile-jobs.batch-limit", "1");
    String lockedJobId = "job-a-locked-preparation-failure";
    String laterJobId = "job-z-after-locked-preparation-failure";
    putNativeJobIndexRows(
        storedJob(lockedJobId, "JS_SUCCEEDED", 1L, "", "hash-locked-failure", ""));
    putNativeJobIndexRows(storedJob(laterJobId, "JS_RUNNING", 1L, "", "hash-later-running", ""));
    failingBackend.lockedCanonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, lockedJobId);

    var first = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, first.expired());
    assertTrue(first.nextJobToken().isBlank());
    assertEquals(1, failingBackend.failedPreparations);

    var second = gc.runAccountSlice(ACCOUNT_ID, first.nextJobToken(), "");

    assertEquals(1, second.scanned());
    assertEquals(0, second.expired());
    assertEquals(2, failingBackend.failedPreparations);
    assertTrue(
        failingBackend
            .loadIndexEntry(Keys.reconcileJobPointerById(ACCOUNT_ID, laterJobId))
            .isPresent());
  }

  @Test
  void accountSliceKeepsUnreadableCanonicalPointers() {
    String jobId = "job-missing-inline";
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    putPointer(canonicalKey, "inline:reconcile-job:not-valid");
    putPointer(Keys.reconcileJobLookupPointerById(jobId), canonicalKey);
    putPointer(Keys.reconcileTerminalRetentionPointer(ACCOUNT_ID, 0L, jobId), canonicalKey);

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
    putPointer(Keys.reconcileTerminalRetentionPointer(ACCOUNT_ID, 0L, jobId), canonicalKey);

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

    assertEquals(1, second.ptrDeleted());
    assertEquals(0, second.canonicalQuarantined());
    assertTrue(pointers.get(canonicalKey).isPresent());
    assertTrue(pointers.get(markerKey).isEmpty());
  }

  @Test
  void accountSliceDeletesQuarantineMarkerWithRecoveredReadableTerminalJob() {
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "1");
    String jobId = "job-readable-terminal-with-quarantine-marker";
    putNativeJobIndexRows(
        storedJob(
            jobId,
            "JS_SUCCEEDED",
            System.currentTimeMillis() - 10_000L,
            "",
            "hash-readable-terminal-marker",
            ""));
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    JobIndexEntrySnapshot canonical = jobIndexBackend.loadIndexEntry(canonicalKey).orElseThrow();
    String markerKey =
        Keys.reconcileCanonicalQuarantinePointer(ACCOUNT_ID, hashValue(canonicalKey));
    putQuarantineMarker(markerKey, canonicalKey, canonical.version(), canonical.blobUri());

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(1, result.expired());
    assertTrue(jobIndexBackend.loadIndexEntry(canonicalKey).isEmpty());
    assertTrue(pointers.get(markerKey).isEmpty());
  }

  @Test
  void accountSlicePreservesQuarantineAgeAcrossCanonicalVersionOnlyChange() {
    System.setProperty("floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms", "60000");
    String jobId = "job-corrupt-version-only-change";
    StoredReconcileJob record =
        storedJob(
            jobId,
            "JS_SUCCEEDED",
            System.currentTimeMillis() - 10_000L,
            "",
            "hash-corrupt-version-only-change",
            "");
    putNativeJobIndexRows(record);
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer readable = pointers.get(canonicalKey).orElseThrow();
    String corruptReference = "inline:reconcile-job:not-valid";
    assertTrue(
        pointers.compareAndSet(
            canonicalKey,
            readable.getVersion(),
            PointerReferences.inlineJsonPointer(
                canonicalKey, corruptReference, readable.getVersion() + 1L)));
    Pointer corrupt = pointers.get(canonicalKey).orElseThrow();
    String markerKey =
        Keys.reconcileCanonicalQuarantinePointer(ACCOUNT_ID, hashValue(canonicalKey));
    String markerPayload =
        corrupt.getVersion()
            + "\n"
            + corruptReference
            + "\n"
            + (System.currentTimeMillis() - 120_000L)
            + "\n"
            + canonicalKey;
    assertTrue(
        pointers.compareAndSet(
            markerKey, 0L, PointerReferences.opaqueMarkerPointer(markerKey, markerPayload, 1L)));
    assertTrue(
        pointers.compareAndSet(
            canonicalKey,
            corrupt.getVersion(),
            PointerReferences.inlineJsonPointer(
                canonicalKey, corruptReference, corrupt.getVersion() + 1L)));

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(1, result.expired());
    assertTrue(pointers.get(canonicalKey).isEmpty());
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
      if (i == 3) {
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
    assertTrue(pointers.get(markerKeys.get(3)).isPresent());

    var second = gc.runAccountSlice(ACCOUNT_ID, "", first.nextCanonicalQuarantineToken());

    assertTrue(pointers.get(markerKeys.get(3)).isEmpty());
    assertEquals("", second.nextCanonicalQuarantineToken());
  }

  @Test
  void accountSlicePurgesUnreadableCanonicalPointersAfterQuarantineRetention() {
    System.setProperty("floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms", "0");
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "1");
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
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "1");
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
  void accountSliceDefersUnreadableCanonicalWithoutManifestOrFallbackScan() {
    System.setProperty("floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms", "0");
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "1");
    AwaitingMigrationMemoryJobIndexBackend awaitingBackend =
        new AwaitingMigrationMemoryJobIndexBackend();
    awaitingBackend.bind(pointers);
    jobIndexBackend = awaitingBackend;
    gc.jobIndexBackend = awaitingBackend;
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(blobs, pointers, mapper);
    gc.jobIndexStore = new NativeReconcileJobIndexStore();
    gc.jobIndexStore.bind(
        awaitingBackend,
        payloadStore,
        gc.jobIndexes,
        16,
        (previous, current) -> {},
        (previous, current, operation) -> {});

    String jobId = "job-awaiting-cleanup-migration";
    long now = System.currentTimeMillis() - 10_000L;
    StoredReconcileJob record =
        storedJob(jobId, "JS_SUCCEEDED", now, "parent-awaiting", "dedupe-awaiting", "");
    putNativeJobIndexRows(record, false);
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer canonical = pointers.get(canonicalKey).orElseThrow();
    assertTrue(
        pointers.compareAndSet(
            canonicalKey,
            canonical.getVersion(),
            PointerReferences.inlineJsonPointer(
                canonicalKey, "inline:reconcile-job:not-valid", canonical.getVersion() + 1L)));

    gc.runAccountSlice(ACCOUNT_ID, "", "");
    var retained = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(1, retained.canonicalQuarantined());
    assertTrue(awaitingBackend.loadIndexEntry(canonicalKey).isPresent());
    assertEquals(0, awaitingBackend.discoveryCalls);
  }

  @Test
  void accountSliceRetainsReadableCanonicalUntilManifestMigrationCompletes() {
    AwaitingMigrationMemoryJobIndexBackend awaitingBackend =
        new AwaitingMigrationMemoryJobIndexBackend();
    awaitingBackend.bind(pointers);
    jobIndexBackend = awaitingBackend;
    gc.jobIndexBackend = awaitingBackend;
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(blobs, pointers, mapper);
    gc.jobIndexStore = new NativeReconcileJobIndexStore();
    gc.jobIndexStore.bind(
        awaitingBackend,
        payloadStore,
        gc.jobIndexes,
        16,
        (previous, current) -> {},
        (previous, current, operation) -> {});

    String jobId = "job-readable-awaiting-cleanup-migration";
    StoredReconcileJob record =
        storedJob(
            jobId,
            "JS_SUCCEEDED",
            System.currentTimeMillis() - 10_000L,
            "parent-readable-awaiting",
            "dedupe-readable-awaiting",
            "");
    putNativeJobIndexRows(record);
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);

    var retained = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertEquals(0, retained.expired());
    assertTrue(awaitingBackend.loadIndexEntry(canonicalKey).isPresent());
  }

  @Test
  void accountSliceDoesNotPurgeReadableReplacementThatRacesQuarantinePurge() {
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "1");
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
  void accountSliceKeepsNativeSecondaryRowsWhenCanonicalPayloadIsUnreadable() {
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "1");
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
  void accountSliceDeletesHistoricalReadyPointersForTerminalInlineJobs() {
    System.setProperty("floecat.gc.reconcile-jobs.retention-ms", "1");
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
    putPointer(
        Keys.reconcileJobByAccountStatePointer(ACCOUNT_ID, "JS_SUCCEEDED", finishedAtMs, jobId),
        canonicalKey);
    gc.runTerminalRetentionBackfillSlice(ACCOUNT_ID, "");

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
    String terminalRetentionKey = gc.jobIndexes.terminalRetentionPointerKey(record);
    if (!terminalRetentionKey.isBlank()) {
      writes.add(
          new ReconcileJobIndexStore.JobIndexUpsert(
              terminalRetentionKey, 0L, canonicalKey, PointerReferenceKind.PRK_POINTER_KEY));
    }
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
      if (!terminalRetentionKey.isBlank()) {
        cleanupIndexKeys.add(terminalRetentionKey);
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

  private static final class DeadlineAdvancingMemoryJobIndexBackend
      extends MemoryReconcileJobIndexBackend {
    private final java.util.concurrent.atomic.AtomicLong clock;
    private boolean advanceCleanupPhases;
    private int cleanupPhaseCalls;

    private DeadlineAdvancingMemoryJobIndexBackend(java.util.concurrent.atomic.AtomicLong clock) {
      this.clock = clock;
    }

    @Override
    public synchronized boolean compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch batch,
        java.util.List<PointerStore.CasOp> extraPointerOps) {
      boolean cleanupPhase =
          batch != null
              && batch.writes().stream()
                  .anyMatch(
                      write ->
                          write instanceof ReconcileJobIndexStore.JobIndexCheck check
                              && check.requireCleanupLock());
      boolean committed = super.compareAndSetBatch(batch, extraPointerOps);
      if (committed && advanceCleanupPhases && cleanupPhase) {
        cleanupPhaseCalls++;
        clock.addAndGet(100L);
      }
      return committed;
    }
  }

  private static final class SlowPreparationMemoryJobIndexBackend
      extends MemoryReconcileJobIndexBackend {
    private final java.util.concurrent.atomic.AtomicLong clock;
    private int preparations;
    private int lastCanonicalLimit;

    private SlowPreparationMemoryJobIndexBackend(java.util.concurrent.atomic.AtomicLong clock) {
      this.clock = clock;
    }

    @Override
    public ReconcileJobIndexBackend.JobIndexQueryPage listTerminalRetentionEntries(
        String accountId, int limit, String pageToken) {
      lastCanonicalLimit = limit;
      return super.listTerminalRetentionEntries(accountId, limit, pageToken);
    }

    @Override
    public synchronized java.util.Optional<ReconcileJobIndexBackend.JobCleanupSession>
        beginJobCleanup(
            CanonicalPointerSnapshot expected, ReconcileJobIndexCleanupManifest fallbackManifest) {
      java.util.Optional<ReconcileJobIndexBackend.JobCleanupSession> session =
          super.beginJobCleanup(expected, fallbackManifest);
      preparations++;
      clock.addAndGet(100L);
      return session;
    }
  }

  private static final class DeadlineAfterPageFetchMemoryJobIndexBackend
      extends MemoryReconcileJobIndexBackend {
    private final java.util.concurrent.atomic.AtomicLong clock;

    private DeadlineAfterPageFetchMemoryJobIndexBackend(
        java.util.concurrent.atomic.AtomicLong clock) {
      this.clock = clock;
    }

    @Override
    public ReconcileJobIndexBackend.JobIndexQueryPage listTerminalRetentionEntries(
        String accountId, int limit, String pageToken) {
      ReconcileJobIndexBackend.JobIndexQueryPage page =
          super.listTerminalRetentionEntries(accountId, limit, pageToken);
      clock.addAndGet(100L);
      return page;
    }
  }

  private static final class PreparationFailingLockedMemoryBackend
      extends MemoryReconcileJobIndexBackend {
    private String lockedCanonicalKey;
    private int failedPreparations;

    @Override
    public synchronized java.util.Optional<JobIndexEntrySnapshot> loadIndexEntry(
        String pointerKey) {
      return super.loadIndexEntry(pointerKey)
          .map(
              entry ->
                  entry.pointerKey().equals(lockedCanonicalKey)
                      ? new JobIndexEntrySnapshot(
                          entry.pointerKey(), entry.blobUri(), entry.version(), true)
                      : entry);
    }

    @Override
    public synchronized java.util.Optional<ReconcileJobIndexBackend.JobCleanupSession>
        beginJobCleanup(
            CanonicalPointerSnapshot expected, ReconcileJobIndexCleanupManifest fallbackManifest) {
      if (expected != null && expected.canonicalPointerKey().equals(lockedCanonicalKey)) {
        failedPreparations++;
        return java.util.Optional.empty();
      }
      return super.beginJobCleanup(expected, fallbackManifest);
    }
  }

  private static final class AwaitingMigrationMemoryJobIndexBackend
      extends MemoryReconcileJobIndexBackend {
    private int discoveryCalls;

    @Override
    public boolean legacyCleanupMigrationComplete() {
      return false;
    }

    @Override
    public ReconcileJobIndexCleanupManifest discoverLegacyCleanupManifest(
        String canonicalPointerKey) {
      discoveryCalls++;
      return super.discoverLegacyCleanupManifest(canonicalPointerKey);
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
    public JobIndexQueryPage listTerminalRetentionEntries(
        String accountId, int limit, String pageToken) {
      return delegate.listTerminalRetentionEntries(accountId, limit, pageToken);
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
    public JobIndexQueryPage listTerminalRetentionEntries(
        String accountId, int limit, String pageToken) {
      return delegate.listTerminalRetentionEntries(accountId, limit, pageToken);
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
