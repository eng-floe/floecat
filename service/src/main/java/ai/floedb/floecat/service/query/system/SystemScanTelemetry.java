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

import ai.floedb.floecat.scanner.spi.ScanTelemetryHook;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;

/** Telemetry helpers for generic system-table scan prefilter signals. */
public final class SystemScanTelemetry {

  private SystemScanTelemetry() {}

  /** Builds a telemetry hook bound to one operation/resource pair. */
  public static ScanTelemetryHook hook(
      Observability observability, String operation, String resourceName) {
    Tag[] baseTags =
        new Tag[] {
          Tag.of(TagKey.COMPONENT, "service"),
          Tag.of(TagKey.OPERATION, operation),
          Tag.of(TagKey.RESOURCE, resourceName)
        };

    return new ScanTelemetryHook() {
      @Override
      public void onPrefilterConstraint(String scanner, String column, long valueCount) {
        observability.counter(
            ServiceMetrics.SystemScan.PREFILTER_CONSTRAINTS_TOTAL,
            Math.max(0L, valueCount),
            tags(baseTags, Tag.of(TagKey.SCOPE, scanner), Tag.of(TagKey.REASON, column)));
      }

      @Override
      public void onPrefilterImpossible(String scanner) {
        observability.counter(
            ServiceMetrics.SystemScan.PREFILTER_IMPOSSIBLE_TOTAL,
            1,
            tags(baseTags, Tag.of(TagKey.SCOPE, scanner), Tag.of(TagKey.REASON, "contradiction")));
      }
    };
  }

  private static Tag[] tags(Tag[] base, Tag... extra) {
    Tag[] out = new Tag[base.length + extra.length];
    System.arraycopy(base, 0, out, 0, base.length);
    System.arraycopy(extra, 0, out, base.length, extra.length);
    return out;
  }

  /** Combines two telemetry hooks into a single delegating hook. */
  public static ScanTelemetryHook combine(ScanTelemetryHook first, ScanTelemetryHook second) {
    if (first == null || first == ScanTelemetryHook.NOOP) {
      return second == null ? ScanTelemetryHook.NOOP : second;
    }
    if (second == null || second == ScanTelemetryHook.NOOP) {
      return first;
    }
    return new ScanTelemetryHook() {
      @Override
      public void onRowsScanned(long delta) {
        first.onRowsScanned(delta);
        second.onRowsScanned(delta);
      }

      @Override
      public void onRowsEmitted(long delta) {
        first.onRowsEmitted(delta);
        second.onRowsEmitted(delta);
      }

      @Override
      public void onBatchEmitted(long rows) {
        first.onBatchEmitted(rows);
        second.onBatchEmitted(rows);
      }

      @Override
      public void onPrefilterConstraint(String scanner, String column, long valueCount) {
        first.onPrefilterConstraint(scanner, column, valueCount);
        second.onPrefilterConstraint(scanner, column, valueCount);
      }

      @Override
      public void onPrefilterImpossible(String scanner) {
        first.onPrefilterImpossible(scanner);
        second.onPrefilterImpossible(scanner);
      }

      @Override
      public void onComplete(long elapsedMs) {
        first.onComplete(elapsedMs);
        second.onComplete(elapsedMs);
      }
    };
  }
}
