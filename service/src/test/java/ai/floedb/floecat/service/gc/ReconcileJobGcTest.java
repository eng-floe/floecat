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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobListSummary;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexEntrySnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
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
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("floecat.gc.reconcile-jobs.page-size");
    System.clearProperty("floecat.gc.reconcile-jobs.batch-limit");
    System.clearProperty("floecat.gc.reconcile-jobs.slice-millis");
    System.clearProperty("floecat.gc.reconcile-jobs.retention-ms");
    System.clearProperty("floecat.gc.reconcile-jobs.global-ready-batch-limit");
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
    String canonicalKey =
        putInlineReconcileJob(
            jobId, "JS_SUCCEEDED", System.currentTimeMillis() - 10_000L, dedupeHash, readyPointer);
    String historyBlob = Keys.reconcileJobBlobUri(ACCOUNT_ID, jobId, "history");
    blobs.put(
        historyBlob,
        "{\"old\":true}".getBytes(StandardCharsets.UTF_8),
        "application/json; charset=UTF-8");
    putPointer(dedupePointer, canonicalKey);
    putPointer(readyPointer, canonicalKey);
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

  private void putPointer(String key, String blobUri) {
    Pointer ptr = PointerReferences.pointerKeyPointer(key, blobUri, 1L);
    pointers.compareAndSet(key, 0L, ptr);
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

    @Override
    public boolean purgeEntriesByCanonicalReference(String canonicalPointerKey) {
      return delegate.purgeEntriesByCanonicalReference(canonicalPointerKey);
    }
  }
}
