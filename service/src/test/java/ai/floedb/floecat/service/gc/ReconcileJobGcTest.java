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
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.repo.model.Keys;
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
  void accountSliceDeletesDanglingCanonicalPointers() {
    String jobId = "job-missing-inline";
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    putPointer(canonicalKey, "inline:reconcile-job:not-valid");
    putPointer(Keys.reconcileJobLookupPointerById(jobId), canonicalKey);

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.ptrDeleted() >= 1);
    assertTrue(pointers.get(canonicalKey).isEmpty());
    assertTrue(pointers.get(Keys.reconcileJobLookupPointerById(jobId)).isEmpty());
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
  void readySliceDeletesStaleReadyPointers() {
    String jobId = "job-running";
    String canonicalKey =
        putInlineReconcileJob(jobId, "JS_RUNNING", System.currentTimeMillis(), "", "");

    String staleReadyKey =
        Keys.reconcileReadyPointerByDue(
            System.currentTimeMillis(), ACCOUNT_ID, "lane-stale", jobId + "-stale");
    putPointer(staleReadyKey, canonicalKey);

    var result = gc.runReadySlice("");

    assertTrue(result.deleted() >= 1);
    assertTrue(pointers.get(staleReadyKey).isEmpty());
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
  }

  @Test
  void accountSlicePurgesNativeSecondaryRowsWhenCanonicalPayloadIsUnreadable() {
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
            Pointer.newBuilder()
                .setKey(canonicalKey)
                .setBlobUri("inline:reconcile-job:not-valid")
                .setVersion(canonical.getVersion() + 1L)
                .build()));

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.ptrDeleted() >= 1);
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
    Pointer ptr = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
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
    putPointer(canonicalKey, inlineReference);
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
    assertTrue(
        jobIndexBackend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                java.util.List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(canonicalKey, 0L, inlineReference),
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        Keys.reconcileJobLookupPointerById(record.jobId), 0L, canonicalKey),
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        Keys.reconcileJobByParentPointer(
                            record.accountId, record.parentJobId, record.jobId),
                        0L,
                        canonicalKey),
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        Keys.reconcileJobByConnectorPointer(
                            record.accountId,
                            record.connectorId,
                            String.format(
                                "%019d-%s", Long.MAX_VALUE - record.createdAtMs, record.jobId)),
                        0L,
                        canonicalKey),
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        Keys.reconcileDedupePointer(record.accountId, record.dedupeKeyHash),
                        0L,
                        canonicalKey),
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        Keys.reconcileJobByStatePointer(
                            record.state, record.createdAtMs, record.accountId, record.jobId),
                        0L,
                        canonicalKey),
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        Keys.reconcileJobByAccountStatePointer(
                            record.accountId, record.state, record.createdAtMs, record.jobId),
                        0L,
                        canonicalKey),
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        Keys.reconcileJobByConnectorStatePointer(
                            record.accountId,
                            record.connectorId,
                            record.state,
                            record.createdAtMs,
                            record.jobId),
                        0L,
                        canonicalKey)),
                new ReconcileJobIndexStore.ReadyQueueMutation(
                    java.util.List.of(
                        new ReconcileJobIndexStore.ReadyQueueWrite(
                            record.readyPointerKey, canonicalKey)),
                    java.util.List.of()))));
  }

  private String serialize(StoredReconcileJob record) {
    try {
      return mapper.writeValueAsString(record);
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
}
