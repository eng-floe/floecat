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

package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RootRepairRequestsTest {

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("tbl")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final String MARKER = Keys.rootResyncPendingPointer("acct", "tbl");

  @Test
  void firstRequestEnqueuesTheResyncMarker() {
    var pointers = new InMemoryPointerStore();
    var repairs = new RootRepairRequests(new RootResyncQueue(pointers), () -> 0L);

    repairs.request(TABLE);

    assertTrue(pointers.get(MARKER).isPresent());
  }

  @Test
  void repeatedRequestsWithinTheWindowAreSuppressed() {
    var pointers = new InMemoryPointerStore();
    var clock = new AtomicLong();
    var repairs = new RootRepairRequests(new RootResyncQueue(pointers), clock::get);

    // A broken table fails every query against it; only the first observer in the window may
    // touch the store. The marker's version records each enqueue (touch-on-exists), so a
    // suppressed report is visible as an unchanged version.
    repairs.request(TABLE);
    clock.addAndGet(TimeUnit.SECONDS.toNanos(30));
    repairs.request(TABLE);
    repairs.request(TABLE);

    assertEquals(1L, pointers.get(MARKER).orElseThrow().getVersion());
  }

  @Test
  void aRequestAfterTheWindowEnqueuesAgain() {
    var pointers = new InMemoryPointerStore();
    var clock = new AtomicLong();
    var repairs = new RootRepairRequests(new RootResyncQueue(pointers), clock::get);

    repairs.request(TABLE);
    clock.addAndGet(TimeUnit.MINUTES.toNanos(6));
    repairs.request(TABLE);

    // The second window's report touched the still-pending marker: version bumped.
    assertEquals(2L, pointers.get(MARKER).orElseThrow().getVersion());
  }

  @Test
  void anEnqueueFailureIsSwallowedAndReleasesTheWindow() {
    var failures = new AtomicInteger(RootResyncQueue.ENQUEUE_ATTEMPTS);
    var pointers =
        new InMemoryPointerStore() {
          @Override
          public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
            if (failures.getAndDecrement() > 0) {
              throw new IllegalStateException("pointer store down");
            }
            return super.compareAndSet(key, expectedVersion, next);
          }
        };
    var repairs = new RootRepairRequests(new RootResyncQueue(pointers, backoffMs -> {}), () -> 0L);

    // The store is down for the whole first enqueue: the query path must not see the failure.
    assertDoesNotThrow(() -> repairs.request(TABLE));
    // The failed report released its window claim, so the next query's report retries and lands.
    repairs.request(TABLE);
    assertTrue(pointers.get(MARKER).isPresent());
  }

  @Test
  void aDisabledInstanceNoOps() {
    assertDoesNotThrow(() -> RootRepairRequests.disabled().request(TABLE));
  }
}
