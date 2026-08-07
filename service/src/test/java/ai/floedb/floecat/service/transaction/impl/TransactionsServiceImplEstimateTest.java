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
package ai.floedb.floecat.service.transaction.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.service.repo.model.Keys;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TransactionsServiceImplEstimateTest {

  @Test
  void connectorAccountFenceIsChargedOnceAndDeletesDoNotCarryIt() {
    var service = new TransactionsServiceImpl();
    var fencedAccounts = new HashSet<String>();
    String pointer = Keys.connectorPointerById("acct", "connector-1");

    assertEquals(
        1, service.connectorFenceOps("acct", "tx", pointer, "blob://connector-1", fencedAccounts));
    assertEquals(
        0, service.connectorFenceOps("acct", "tx", pointer, "blob://connector-2", fencedAccounts));

    var deleteOnlyAccounts = new HashSet<String>();
    assertEquals(
        0,
        service.connectorFenceOps(
            "acct",
            "tx",
            pointer,
            Keys.transactionDeleteSentinelUri("acct", "tx", pointer),
            deleteOnlyAccounts));
  }

  @Test
  void snapshotTableFenceIsChargedOnceUnlessTheTableIsMutatedInTheBatch() {
    var service = new TransactionsServiceImpl();
    String snapshot = Keys.snapshotPointerById("acct", "table-1", 7L);
    var fencedTables = new HashSet<String>();

    assertEquals(
        1,
        service.snapshotFenceOps(
            "acct", "tx", snapshot, "blob://snapshot-1", Set.of(), fencedTables));
    assertEquals(
        0,
        service.snapshotFenceOps(
            "acct", "tx", snapshot, "blob://snapshot-2", Set.of(), fencedTables));
    assertEquals(
        0,
        service.snapshotFenceOps(
            "acct",
            "tx",
            snapshot,
            "blob://snapshot-3",
            Set.of(Keys.tablePointerById("acct", "table-1")),
            new HashSet<>()));
    assertEquals(
        0,
        service.snapshotFenceOps(
            "acct",
            "tx",
            snapshot,
            Keys.transactionDeleteSentinelUri("acct", "tx", snapshot),
            Set.of(),
            new HashSet<>()));
  }
}
