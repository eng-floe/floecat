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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
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

  private PointerStore pointers;
  private BlobStore blobs;
  private ObjectMapper mapper;
  private ReconcileJobGc gc;

  @BeforeEach
  void setUp() {
    pointers = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    mapper = new ObjectMapper();

    gc = new ReconcileJobGc();
    gc.pointerStore = pointers;
    gc.blobStore = blobs;
    gc.mapper = mapper;
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
    String dedupeInput = ACCOUNT_ID + "|" + CONNECTOR_ID + "|incr|*|*|";
    String dedupePointer = Keys.reconcileDedupePointer(ACCOUNT_ID, hashValue(dedupeInput));
    String readyPointer =
        Keys.reconcileReadyPointerByDue(
            System.currentTimeMillis() - 1_000L, ACCOUNT_ID, "lane", jobId);
    String canonicalBlob =
        putReconcileJob(
            jobId, "JS_SUCCEEDED", System.currentTimeMillis() - 10_000L, dedupeInput, readyPointer);
    String historyBlob = Keys.reconcileJobBlobUri(ACCOUNT_ID, jobId, "history");
    blobs.put(
        historyBlob,
        "{\"old\":true}".getBytes(StandardCharsets.UTF_8),
        "application/json; charset=UTF-8");
    putPointer(dedupePointer, canonicalBlob);
    putPointer(readyPointer, canonicalBlob);

    var result = gc.runAccountSlice(ACCOUNT_ID, "", "");

    assertTrue(result.expired() >= 1);
    assertTrue(pointers.get(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId)).isEmpty());
    assertTrue(pointers.get(Keys.reconcileJobLookupPointerById(jobId)).isEmpty());
    assertTrue(pointers.get(dedupePointer).isEmpty());
    assertTrue(pointers.get(readyPointer).isEmpty());
    assertFalse(blobs.head(canonicalBlob).isPresent());
    assertFalse(blobs.head(historyBlob).isPresent());
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
    putReconcileJob(jobId, "JS_RUNNING", System.currentTimeMillis(), "", "");

    String staleReadyKey =
        Keys.reconcileReadyPointerByDue(
            System.currentTimeMillis(), ACCOUNT_ID, "lane-stale", jobId + "-stale");
    String canonicalBlob =
        pointers.get(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId)).orElseThrow().getBlobUri();
    putPointer(staleReadyKey, canonicalBlob);

    var result = gc.runReadySlice("");

    assertTrue(result.deleted() >= 1);
    assertTrue(pointers.get(staleReadyKey).isEmpty());
  }

  private void putPointer(String key, String blobUri) {
    Pointer ptr = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
    pointers.compareAndSet(key, 0L, ptr);
  }

  private String putReconcileJob(
      String jobId, String state, long updatedAtMs, String dedupeKey, String readyPointerKey) {
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    String blobUri = Keys.reconcileJobBlobUri(ACCOUNT_ID, jobId, "seed");
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);

    ObjectNode record = mapper.createObjectNode();
    record.put("jobId", jobId);
    record.put("accountId", ACCOUNT_ID);
    record.put("connectorId", CONNECTOR_ID);
    record.put("state", state);
    record.put("updatedAtMs", updatedAtMs);
    record.put("createdAtMs", updatedAtMs);
    record.put("dedupeKey", dedupeKey);
    record.put("readyPointerKey", readyPointerKey);

    blobs.put(
        blobUri,
        record.toString().getBytes(StandardCharsets.UTF_8),
        "application/json; charset=UTF-8");
    putPointer(canonicalKey, blobUri);
    putPointer(lookupKey, blobUri);
    return blobUri;
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
