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

package ai.floedb.floecat.service.repo.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class KeysTest {

  @Test
  void tableCommitJournalPointerUsesPathSafeEncoding() {
    assertEquals(
        "/accounts/acct%20id/tables/table%20id/tx-journal/tx%20id",
        Keys.tableCommitJournalPointer("acct id", "table id", "tx id"));
  }

  @Test
  void snapshotPointerByIdUsesPathSafeEncoding() {
    assertEquals(
        "/accounts/acct%20id/tables/table%20id/snapshots/by-id/0000000000000000000",
        Keys.snapshotPointerById("acct id", "table id", 0L));
  }

  @Test
  void tableCommitOutboxPendingPointerUsesGlobalAccountPartition() {
    assertEquals(
        "/accounts/acct%20id/tx-outbox/pending/0000000000000000123/table%20id/tx%20id",
        Keys.tableCommitOutboxPendingPointer(123L, "acct id", "table id", "tx id"));
  }

  @Test
  void snapshotConstraintsKeysUsePathSafeEncoding() {
    assertEquals(
        "/accounts/acct%20id/tables/table%20id/constraints/by-snapshot/0000000000000000007",
        Keys.snapshotConstraintsPointer("acct id", "table id", 7L));
    assertEquals(
        "/accounts/acct%20id/tables/table%20id/snapshots/0000000000000000007/stats/constraints",
        Keys.snapshotConstraintsStatsPointer("acct id", "table id", 7L));
  }
}
