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

package ai.floedb.floecat.systemcatalog.statssystable;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class StatsScannerTestSupport {
  private StatsScannerTestSupport() {}

  static SystemObjectScanContext context(
      TestTableScanContextBuilder builder, StatsProvider statsProvider) {
    return new SystemObjectScanContext(
        builder.overlay(),
        NameRef.getDefaultInstance(),
        ResourceId.newBuilder()
            .setAccountId("account")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("catalog")
            .build(),
        EngineContext.empty(),
        statsProvider);
  }

  static final class FakeStatsProvider implements StatsProvider {
    private final Map<Key, List<TargetStatsRecord>> records = new HashMap<>();

    void put(ResourceId tableId, long snapshotId, List<TargetStatsRecord> items) {
      records.put(new Key(tableId, snapshotId), List.copyOf(items));
    }

    @Override
    public TargetStatsPage listPersistedStats(
        ResourceId tableId,
        long snapshotId,
        Optional<String> targetType,
        int limit,
        String pageToken) {
      List<TargetStatsRecord> rows = records.getOrDefault(new Key(tableId, snapshotId), List.of());
      if (targetType.isEmpty()) {
        return new TargetStatsPage(rows, "");
      }
      String requested = targetType.get();
      List<TargetStatsRecord> filtered =
          rows.stream().filter(row -> matchesTargetType(row, requested)).toList();
      return new TargetStatsPage(filtered, "");
    }

    private static boolean matchesTargetType(TargetStatsRecord row, String targetType) {
      if ("TABLE".equalsIgnoreCase(targetType)) {
        return row.hasTarget() && row.getTarget().hasTable();
      }
      if ("COLUMN".equalsIgnoreCase(targetType)) {
        return row.hasTarget() && row.getTarget().hasColumn();
      }
      if ("EXPRESSION".equalsIgnoreCase(targetType)) {
        return row.hasTarget() && row.getTarget().hasExpression();
      }
      return false;
    }
  }

  private record Key(ResourceId tableId, long snapshotId) {}
}
