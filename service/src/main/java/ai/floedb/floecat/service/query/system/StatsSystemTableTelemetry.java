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

package ai.floedb.floecat.service.query.system;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.ScanTelemetryHook;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.time.Duration;

/** Telemetry helpers for {@code sys.stats_*} system-table scans. */
public final class StatsSystemTableTelemetry {

  private static final String SYS_SCHEMA = "sys";
  private static final String STATS_PREFIX = "stats_";

  private StatsSystemTableTelemetry() {}

  /** Returns whether the table id resolves to one of the {@code sys.stats_*} system tables. */
  public static boolean isStatsSystemTable(CatalogOverlay graph, ResourceId tableId) {
    return graph
        .resolveSystemTableName(tableId)
        .filter(
            name ->
                name.getPathCount() == 1
                    && SYS_SCHEMA.equalsIgnoreCase(name.getPath(0))
                    && name.getName().toLowerCase().startsWith(STATS_PREFIX))
        .isPresent();
  }

  /** Returns the canonical table name, or falls back to the resource id if unavailable. */
  public static String resourceName(CatalogOverlay graph, ResourceId tableId) {
    return graph
        .resolveSystemTableName(tableId)
        .map(NameRefUtil::canonical)
        .orElse(tableId.getId());
  }

  /** Builds a telemetry hook bound to one operation/resource pair. */
  public static ScanTelemetryHook hook(
      Observability observability, String operation, String resourceName) {
    Tag[] tags =
        new Tag[] {
          Tag.of(TagKey.COMPONENT, "service"),
          Tag.of(TagKey.OPERATION, operation),
          Tag.of(TagKey.RESOURCE, resourceName)
        };
    return new ScanTelemetryHook() {
      @Override
      public void onRowsScanned(long delta) {
        observability.counter(ServiceMetrics.Stats.SYSTABLE_ROWS_SCANNED, delta, tags);
      }

      @Override
      public void onRowsEmitted(long delta) {
        observability.counter(ServiceMetrics.Stats.SYSTABLE_ROWS_EMITTED, delta, tags);
      }

      @Override
      public void onBatchEmitted(long rows) {
        observability.counter(ServiceMetrics.Stats.SYSTABLE_BATCHES_EMITTED, 1, tags);
      }

      @Override
      public void onComplete(long elapsedMs) {
        observability.timer(
            ServiceMetrics.Stats.SYSTABLE_BUILD_LATENCY,
            Duration.ofMillis(Math.max(0L, elapsedMs)),
            tags);
      }
    };
  }
}
