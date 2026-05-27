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

package ai.floedb.floecat.service.reconciler.jobs.durable.projection;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class ReconcileJobProjectionStoreTest {
  @Test
  void sameGenerationUpsertReplacesStaleProjectionPayload() {
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(new InMemoryBlobStore(), new InMemoryPointerStore(), new ObjectMapper());

    ReconcileJobProjectionStore store = new ReconcileJobProjectionStore();
    store.bind(new InMemoryPointerStore(), payloadStore);

    StoredReconcileJobProjection initial = projection(7L, "JS_WAITING", "Waiting", 1L, 0L, 0L);
    StoredReconcileJobProjection replacement =
        projection(7L, "JS_SUCCEEDED", "Succeeded", 3L, 1L, 2L);

    store.upsert(initial);
    StoredReconcileJobProjection accepted = store.upsert(replacement);

    assertEquals(replacement, accepted);
    assertEquals(replacement, store.load("acct-1", "job-1").orElseThrow());
  }

  @Test
  void newerGenerationProjectionIsNotOverwrittenByOlderGeneration() {
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(new InMemoryBlobStore(), new InMemoryPointerStore(), new ObjectMapper());

    ReconcileJobProjectionStore store = new ReconcileJobProjectionStore();
    store.bind(new InMemoryPointerStore(), payloadStore);

    StoredReconcileJobProjection newer = projection(9L, "JS_SUCCEEDED", "Succeeded", 5L, 2L, 1L);
    StoredReconcileJobProjection older = projection(8L, "JS_RUNNING", "Running", 4L, 1L, 0L);

    store.upsert(newer);
    StoredReconcileJobProjection accepted = store.upsert(older);

    assertEquals(newer, accepted);
    assertEquals(newer, store.load("acct-1", "job-1").orElseThrow());
  }

  private static StoredReconcileJobProjection projection(
      long generation,
      String state,
      String message,
      long tablesScanned,
      long tablesChanged,
      long snapshotsProcessed) {
    return new StoredReconcileJobProjection(
        "acct-1",
        "job-1",
        generation,
        state,
        message,
        100L,
        "JS_SUCCEEDED".equals(state) ? 200L : 0L,
        tablesScanned,
        tablesChanged,
        0L,
        0L,
        0L,
        snapshotsProcessed,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        "",
        true);
  }
}
