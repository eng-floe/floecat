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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ReconcileScopeTest {

  @Test
  void namespaceMatchingAcceptsSegmentedAndFqSingleSegmentPaths() {
    ReconcileScope segmented = ReconcileScope.of(List.of(List.of("dest", "ns")), null, List.of());
    ReconcileScope fqSingle = ReconcileScope.of(List.of(List.of("dest.ns")), null, List.of());

    assertTrue(segmented.matchesNamespace("dest.ns"));
    assertTrue(fqSingle.matchesNamespace("dest.ns"));
  }

  @Test
  void tableAcceptanceRequiresMatchingTableWhenTableFilterPresent() {
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("dest", "ns")), "tbl", List.of());

    assertTrue(scope.acceptsTable("dest.ns", "tbl"));
    assertFalse(scope.acceptsTable("dest.ns", "other"));
  }
}
