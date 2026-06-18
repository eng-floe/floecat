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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore.ReadyIndexType;
import ai.floedb.floecat.service.repo.model.Keys;
import org.junit.jupiter.api.Test;

class ReadyQueueBackendSupportTest {
  @Test
  void toReadyQueueRowRecognizesExecutionClassReadyPointers() {
    String readyPointerKey =
        Keys.reconcileReadyByExecutionClassPointerByDue(1234L, "DEFAULT", "acct-1", "job-1");

    ReadyQueueBackendSupport.ReadyQueueRow row =
        ReadyQueueBackendSupport.toReadyQueueRow(
            readyPointerKey, "/accounts/acct-1/reconcile/jobs/by-id/job-1");

    assertNotNull(row);
    assertEquals("reconcile-ready#execution-class#DEFAULT", row.partitionKey());
    assertEquals(ReadyIndexType.EXECUTION_CLASS, row.entry().indexType());
    assertEquals("DEFAULT", row.entry().filterValue());
    assertEquals("acct-1", row.entry().accountId());
    assertEquals("job-1", row.entry().jobId());
  }

  @Test
  void toReadyQueueRowResolvesDeleteKeyWithoutCanonicalPointer() {
    String readyPointerKey = Keys.reconcileReadyPointerByDue(1234L, "acct-1", "lane-1", "job-1");

    // The write path knows the canonical pointer; deletes pruning a leaked pointer do not. The
    // key-only resolver must still land on the exact primary key the entry was written under,
    // otherwise the delete silently targets nothing (the original starvation bug).
    ReadyQueueBackendSupport.ReadyQueueRow withCanonical =
        ReadyQueueBackendSupport.toReadyQueueRow(
            readyPointerKey, "/accounts/acct-1/reconcile/jobs/by-id/job-1");
    ReadyQueueBackendSupport.ReadyQueueRow keyOnly =
        ReadyQueueBackendSupport.toReadyQueueRow(readyPointerKey);

    assertNotNull(keyOnly);
    assertEquals(withCanonical.partitionKey(), keyOnly.partitionKey());
    assertEquals(withCanonical.sortKey(), keyOnly.sortKey());
    assertEquals("reconcile-ready#global", keyOnly.partitionKey());
    assertEquals(ReadyIndexType.GLOBAL, keyOnly.entry().indexType());
    assertEquals("acct-1", keyOnly.entry().accountId());
    assertEquals("job-1", keyOnly.entry().jobId());
  }
}
