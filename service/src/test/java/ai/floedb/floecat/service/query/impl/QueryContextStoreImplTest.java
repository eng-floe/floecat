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

package ai.floedb.floecat.service.query.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.ScanHandle;
import ai.floedb.floecat.query.rpc.TableInfo;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.error.impl.FloecatStatus;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

class QueryContextStoreImplTest {

  @Test
  void updatingAContextKeepsItsOpenScanHandles() {
    QueryContextStoreImpl store = QueryContextStores.forTesting();
    store.put(query("q", 60_000L));

    ScanHandle first = store.createScanSession("c", session("q"));
    ScanHandle second = store.createScanSession("c", session("q"));
    store.extendLease("q", System.currentTimeMillis() + 120_000L);

    assertTrue(store.getScanSession(first).isPresent());
    assertTrue(store.getScanSession(second).isPresent());
  }

  @Test
  void closingOneScanKeepsItsSiblings() {
    QueryContextStoreImpl store = QueryContextStores.forTesting();
    store.put(query("q", 60_000L));
    ScanHandle first = store.createScanSession("c", session("q"));
    ScanHandle second = store.createScanSession("c", session("q"));

    store.removeScanSession(first);

    assertFalse(store.getScanSession(first).isPresent());
    assertTrue(store.getScanSession(second).isPresent());
  }

  @Test
  void anEndedQueryServesNoScans() {
    QueryContextStoreImpl store = QueryContextStores.forTesting();
    store.put(query("q", 60_000L));
    ScanHandle handle = store.createScanSession("c", session("q"));

    store.end("q", false);

    assertEquals(0, store.openScanSessions());
    assertFalse(store.getScanSession(handle).isPresent());
  }

  @Test
  void anEndedQueryTakesNoNewScan() {
    QueryContextStoreImpl store = QueryContextStores.forTesting();
    store.put(query("q", 60_000L));
    store.end("q", true);

    assertThrows(RuntimeException.class, () -> store.createScanSession("c", session("q")));
    assertEquals(0, store.openScanSessions());
  }

  @Test
  void anExpiredRenewReleasesTheScans() {
    QueryContextStoreImpl store = QueryContextStores.forTesting();
    store.put(query("q", 60_000L));
    store.createScanSession("c", session("q"));
    store.clock = java.time.Clock.offset(store.clock, java.time.Duration.ofMinutes(2));

    assertTrue(store.extendLease("q", System.currentTimeMillis() + 600_000L).isEmpty());
    assertEquals(0, store.openScanSessions());
  }

  @Test
  void aScanAgainstAnExpiredQueryReportsNotActive() {
    QueryContextStoreImpl store = QueryContextStores.forTesting();
    store.put(query("q", 60_000L));
    store.clock = java.time.Clock.offset(store.clock, java.time.Duration.ofMinutes(2));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class, () -> store.createScanSession("c", session("q")));
    FloecatStatus status = FloecatStatus.fromThrowable(error);
    assertEquals(ErrorCode.MC_PRECONDITION_FAILED, status.errorCode());
    assertEquals("query.not.active", status.messageKey());
  }

  @Test
  void aQueryHoldsABoundedNumberOfScans() {
    QueryContextStoreImpl store = QueryContextStores.forTesting();
    store.put(query("q", 60_000L));
    for (int i = 0; i < QueryContextStoreImpl.MAX_OPEN_SCANS_PER_QUERY; i++) {
      store.createScanSession("c", session("q"));
    }

    assertThrows(RuntimeException.class, () -> store.createScanSession("c", session("q")));
    assertEquals(QueryContextStoreImpl.MAX_OPEN_SCANS_PER_QUERY, store.openScanSessions());
  }

  @Test
  void aMissingQueryTakesNoScan() {
    QueryContextStoreImpl store = QueryContextStores.forTesting();

    assertThrows(RuntimeException.class, () -> store.createScanSession("c", session("gone")));
    assertEquals(0, store.openScanSessions());
  }

  @Test
  void aQueryPastItsLeaseServesNoScans() {
    QueryContextStoreImpl store = QueryContextStores.forTesting();
    store.put(query("q", 60_000L));
    ScanHandle handle = store.createScanSession("c", session("q"));
    store.createScanSession("c", session("q"));

    store.clock = java.time.Clock.offset(store.clock, java.time.Duration.ofMinutes(2));

    assertFalse(store.getScanSession(handle).isPresent());
    assertEquals(0, store.openScanSessions());
  }

  @Test
  void activeContextsStayResidentOverTheSizeCap() {
    QueryContextStoreImpl store = QueryContextStores.forTesting(1L);
    for (int i = 0; i < 50; i++) {
      store.put(query("q-" + i, 60_000L));
    }

    for (int i = 0; i < 50; i++) {
      assertTrue(store.get("q-" + i).isPresent(), "active context q-" + i + " was evicted");
    }
  }

  @Test
  void updateExpiresAContextBeforeApplyingTheMutation() {
    // The update seam must apply the same lease rule as get(). Otherwise InitScan or
    // DescribeInputs could mutate a context after its lease elapsed.
    QueryContextStoreImpl store = QueryContextStores.forTesting();
    byte[] selections = {1, 2, 3};
    java.time.Clock start = store.clock;
    store.put(query("q-expired-update", 60_000L));
    store.clock = java.time.Clock.offset(start, java.time.Duration.ofMinutes(2));

    QueryContext refused =
        store
            .update("q-expired-update", ctx -> ctx.toBuilder().relationPins(selections).build())
            .orElseThrow();
    assertFalse(refused.isActive());
    assertNull(refused.getRelationPins());
    assertSame(refused, store.get("q-expired-update").orElseThrow());
  }

  private static QueryContext query(String queryId, long ttlMs) {
    return QueryContext.newActive(
        queryId,
        PrincipalContext.newBuilder().setAccountId("acct").setQueryId(queryId).build(),
        null,
        null,
        null,
        null,
        ttlMs,
        1,
        ResourceId.newBuilder().setId("cat").build());
  }

  private static ScanSession session(String queryId) {
    return ScanSession.builder()
        .queryId(queryId)
        .tableId(ResourceId.newBuilder().setId("t").build())
        .snapshotId(1L)
        .selection(TablePin.newBuilder().setSnapshotId(1L).build())
        .tableInfo(TableInfo.getDefaultInstance())
        .targetBatchItems(10)
        .targetBatchBytes(1024)
        .build();
  }
}
