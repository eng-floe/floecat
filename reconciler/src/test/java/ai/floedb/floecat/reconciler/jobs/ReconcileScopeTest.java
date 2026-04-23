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

package ai.floedb.floecat.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ReconcileScopeTest {
  private static final String NAMESPACE_ID = "ns-1";
  private static final String TABLE_ID = "tbl-1";

  @Test
  void namespaceMatchingUsesNamespaceIds() {
    ReconcileScope scope = ReconcileScope.of(List.of(NAMESPACE_ID), null);

    assertTrue(scope.matchesNamespaceId(NAMESPACE_ID));
    assertFalse(scope.matchesNamespaceId("ns-2"));
  }

  @Test
  void tableAcceptanceRequiresMatchingTableWhenTableFilterPresent() {
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID);

    assertTrue(scope.acceptsTable("ignored", TABLE_ID));
    assertFalse(scope.acceptsTable("ignored", "other"));
  }

  @Test
  void storesExplicitStatsRequests() {
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            TABLE_ID,
            List.of(
                scopedStatsRequest(TABLE_ID, 10L, "table", List.of()),
                scopedStatsRequest(TABLE_ID, 11L, "column:7", List.of())));

    assertTrue(scope.hasStatsRequestFilter());
    assertEquals(
        List.of(
            scopedStatsRequest(TABLE_ID, 10L, "table", List.of()),
            scopedStatsRequest(TABLE_ID, 11L, "column:7", List.of())),
        scope.destinationStatsRequests());
  }

  private static ReconcileScope.ScopedStatsRequest scopedStatsRequest(
      String tableId, long snapshotId, String targetSpec, List<String> columnSelectors) {
    return new ReconcileScope.ScopedStatsRequest(tableId, snapshotId, targetSpec, columnSelectors);
  }
}
