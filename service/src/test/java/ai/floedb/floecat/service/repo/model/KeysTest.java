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
  void snapshotPointerByIdUsesPathSafeEncoding() {
    assertEquals(
        "/accounts/acct%20id/tables/table%20id/snapshots/by-id/0000000000000000000",
        Keys.snapshotPointerById("acct id", "table id", 0L));
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

  @Test
  void encodeSegmentUsesRfc3986PathSegmentRules() {
    assertEquals("a%20b", Keys.encodeSegment("a b"));
    assertEquals("a%2Bb", Keys.encodeSegment("a+b"));
    assertEquals("a%2Fb", Keys.encodeSegment("a/b"));
    assertEquals("a%25b", Keys.encodeSegment("a%b"));
  }

  @Test
  void transactionDeleteSentinelEncodesOpaquePointerKey() {
    assertEquals(
        "/accounts/acct/transactions/tx/delete/%2Faccounts%2Fa%2Fb",
        Keys.transactionDeleteSentinelUri("acct", "tx", "/accounts/a/b"));
  }
}
