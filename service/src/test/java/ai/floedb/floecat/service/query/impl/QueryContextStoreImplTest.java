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

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.ScanHandle;
import ai.floedb.floecat.query.rpc.TableInfo;
import org.junit.jupiter.api.Test;

class QueryContextStoreImplTest {

  @Test
  void updatingAContextKeepsItsOpenScanHandles() throws InterruptedException {
    QueryContextStoreImpl store = QueryContextStores.forTesting();
    store.put(
        QueryContext.newActive(
            "q",
            PrincipalContext.newBuilder().setAccountId("acct").setQueryId("q").build(),
            null,
            null,
            null,
            null,
            60_000L,
            1,
            ResourceId.newBuilder().setId("cat").build()));

    ScanHandle first = store.createScanSession("c", session());
    ScanHandle second = store.createScanSession("c", session());
    store.extendLease("q", System.currentTimeMillis() + 120_000L);
    // Removal notifications run asynchronously; give a wrongly fired one time to land.
    Thread.sleep(200);

    assertTrue(store.getScanSession(first).isPresent());
    assertTrue(store.getScanSession(second).isPresent());
  }

  private static ScanSession session() {
    return ScanSession.builder()
        .queryId("q")
        .tableId(ResourceId.newBuilder().setId("t").build())
        .snapshotId(1L)
        .tableInfo(TableInfo.getDefaultInstance())
        .targetBatchItems(10)
        .targetBatchBytes(1024)
        .build();
  }
}
