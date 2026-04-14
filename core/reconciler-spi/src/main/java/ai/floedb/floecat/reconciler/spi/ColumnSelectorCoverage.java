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
package ai.floedb.floecat.reconciler.spi;

import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import java.util.HashSet;
import java.util.Set;

/** Shared selector parsing and coverage checks for reconciler backends. */
public final class ColumnSelectorCoverage {
  private ColumnSelectorCoverage() {}

  public static SelectorCoverage parse(Set<String> selectors) {
    if (selectors == null || selectors.isEmpty()) {
      return new SelectorCoverage(Set.of(), Set.of(), false);
    }
    Set<Long> ids = new HashSet<>();
    Set<String> names = new HashSet<>();
    boolean hasMalformedIdSelector = false;
    for (String selector : selectors) {
      if (selector == null) {
        continue;
      }
      String normalized = selector.trim();
      if (normalized.isEmpty()) {
        continue;
      }
      if (normalized.startsWith("#")) {
        String digits = normalized.substring(1).trim();
        if (!digits.isEmpty() && digits.chars().allMatch(Character::isDigit)) {
          try {
            ids.add(Long.parseLong(digits));
          } catch (NumberFormatException ignored) {
            hasMalformedIdSelector = true;
          }
        } else {
          hasMalformedIdSelector = true;
        }
        continue;
      }
      names.add(normalized);
    }
    return new SelectorCoverage(Set.copyOf(ids), Set.copyOf(names), hasMalformedIdSelector);
  }

  public static void recordColumnCoverage(
      TargetStatsRecord record, Set<Long> presentIds, Set<String> presentNames) {
    if (record == null) {
      return;
    }
    StatsTarget target = record.getTarget();
    if (target == null || !target.hasColumn()) {
      return;
    }
    long columnId = target.getColumn().getColumnId();
    if (columnId > 0) {
      presentIds.add(columnId);
    }
    if (!record.hasScalar()) {
      return;
    }
    ScalarStats scalar = record.getScalar();
    String displayName = scalar.getDisplayName();
    if (displayName != null) {
      String normalized = displayName.trim();
      if (!normalized.isEmpty()) {
        presentNames.add(normalized);
      }
    }
  }

  public record SelectorCoverage(
      Set<Long> columnIds, Set<String> columnNames, boolean hasMalformedIdSelector) {
    public boolean isEmpty() {
      return !hasMalformedIdSelector && columnIds.isEmpty() && columnNames.isEmpty();
    }

    public boolean isUnsatisfiable() {
      return hasMalformedIdSelector;
    }

    public boolean isSatisfiedBy(Set<Long> presentIds, Set<String> presentNames) {
      if (hasMalformedIdSelector) {
        return false;
      }
      return presentIds.containsAll(columnIds) && presentNames.containsAll(columnNames);
    }
  }
}
