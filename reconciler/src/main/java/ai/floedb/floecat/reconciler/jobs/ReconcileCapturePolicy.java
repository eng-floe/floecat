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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Output and column policy for reconcile-time capture execution. */
public final class ReconcileCapturePolicy {
  public enum Output {
    TABLE_STATS,
    FILE_STATS,
    COLUMN_STATS,
    PARQUET_PAGE_INDEX
  }

  public record Column(String selector, boolean captureStats, boolean captureIndex) {
    public Column {
      selector = selector == null ? "" : selector.trim();
    }

    public boolean enabled() {
      return !selector.isBlank() && (captureStats || captureIndex);
    }
  }

  private static final ReconcileCapturePolicy EMPTY =
      new ReconcileCapturePolicy(List.of(), Set.of());

  private final List<Column> columns;
  private final Set<Output> outputs;

  private ReconcileCapturePolicy(List<Column> columns, Set<Output> outputs) {
    this.columns =
        columns == null
            ? List.of()
            : columns.stream()
                .filter(Objects::nonNull)
                .map(
                    column ->
                        new Column(column.selector(), column.captureStats(), column.captureIndex()))
                .filter(Column::enabled)
                .distinct()
                .toList();
    this.outputs = outputs == null ? Set.of() : Set.copyOf(new LinkedHashSet<>(outputs));
  }

  public static ReconcileCapturePolicy empty() {
    return EMPTY;
  }

  public static ReconcileCapturePolicy of(List<Column> columns, Set<Output> outputs) {
    if ((columns == null || columns.isEmpty()) && (outputs == null || outputs.isEmpty())) {
      return EMPTY;
    }
    return new ReconcileCapturePolicy(columns, outputs);
  }

  public List<Column> columns() {
    return columns;
  }

  public Set<Output> outputs() {
    return outputs;
  }

  public boolean isEmpty() {
    return columns.isEmpty() && outputs.isEmpty();
  }

  public boolean requestsStats() {
    return outputs.contains(Output.TABLE_STATS)
        || outputs.contains(Output.FILE_STATS)
        || outputs.contains(Output.COLUMN_STATS);
  }

  public boolean requestsIndexes() {
    return outputs.contains(Output.PARQUET_PAGE_INDEX);
  }

  public Set<String> selectorsForStats() {
    return selectors(true, false);
  }

  public Set<String> selectorsForIndex() {
    return selectors(false, true);
  }

  public Set<String> selectorsForAnyCapture() {
    return selectors(true, true);
  }

  private Set<String> selectors(boolean stats, boolean index) {
    if (columns.isEmpty()) {
      return Set.of();
    }
    LinkedHashSet<String> selectors = new LinkedHashSet<>();
    for (Column column : columns) {
      if (column == null || column.selector().isBlank()) {
        continue;
      }
      if ((stats && column.captureStats()) || (index && column.captureIndex())) {
        selectors.add(column.selector());
      }
    }
    return Set.copyOf(selectors);
  }
}
