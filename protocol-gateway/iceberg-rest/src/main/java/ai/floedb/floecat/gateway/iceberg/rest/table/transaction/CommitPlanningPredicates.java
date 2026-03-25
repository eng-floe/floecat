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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import java.util.List;
import java.util.Map;

final class CommitPlanningPredicates {
  private CommitPlanningPredicates() {}

  static boolean requiresAssertCreate(List<Map<String, Object>> requirements) {
    if (requirements == null || requirements.isEmpty()) {
      return false;
    }
    for (Map<String, Object> requirement : requirements) {
      if (CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE.equals(
          requirement == null ? null : requirement.get("type"))) {
        return true;
      }
    }
    return false;
  }

  static boolean hasCommittedSnapshot(ai.floedb.floecat.catalog.rpc.Table table) {
    if (table == null || table.getPropertiesMap().isEmpty()) {
      return false;
    }
    String currentSnapshotId = table.getPropertiesMap().get("current-snapshot-id");
    return currentSnapshotId != null && !currentSnapshotId.isBlank();
  }
}
