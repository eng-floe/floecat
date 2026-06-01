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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Output and column policy for reconcile-time capture execution. */
public final class ReconcileCapturePolicy {
  public enum DefaultColumnScope {
    FIRST_N,
    ALL,
    EXPLICIT_ONLY
  }

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

  public static final int DEFAULT_MAX_COLUMNS = 32;

  private static final ReconcileCapturePolicy EMPTY =
      new ReconcileCapturePolicy(
          List.of(), Set.of(), DefaultColumnScope.FIRST_N, DEFAULT_MAX_COLUMNS, null);

  private final List<Column> columns;
  private final Set<Output> outputs;
  private final DefaultColumnScope defaultColumnScope;
  private final int maxDefaultColumns;

  /**
   * Cost budget for this capture job. Engines whose {@code estimatedCost()} exceeds this hint are
   * skipped. Defaults to {@link JobCostHint#EXPENSIVE} (no restriction).
   */
  private final JobCostHint maxCost;

  @JsonCreator
  private ReconcileCapturePolicy(
      @JsonProperty("columns") List<Column> columns,
      @JsonProperty("outputs") Set<Output> outputs,
      @JsonProperty("defaultColumnScope") DefaultColumnScope defaultColumnScope,
      @JsonProperty("maxDefaultColumns") Integer maxDefaultColumns,
      @JsonProperty("maxCost") JobCostHint maxCost) {
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
    this.defaultColumnScope =
        defaultColumnScope == null ? DefaultColumnScope.FIRST_N : defaultColumnScope;
    this.maxDefaultColumns =
        maxDefaultColumns == null || maxDefaultColumns <= 0
            ? DEFAULT_MAX_COLUMNS
            : maxDefaultColumns;
    this.maxCost = maxCost == null ? JobCostHint.EXPENSIVE : maxCost;
  }

  public static ReconcileCapturePolicy empty() {
    return EMPTY;
  }

  public static ReconcileCapturePolicy of(List<Column> columns, Set<Output> outputs) {
    return of(columns, outputs, DefaultColumnScope.FIRST_N, DEFAULT_MAX_COLUMNS);
  }

  public static ReconcileCapturePolicy of(
      List<Column> columns,
      Set<Output> outputs,
      DefaultColumnScope defaultColumnScope,
      int maxDefaultColumns) {
    if ((columns == null || columns.isEmpty()) && (outputs == null || outputs.isEmpty())) {
      return EMPTY;
    }
    return new ReconcileCapturePolicy(
        columns, outputs, defaultColumnScope, maxDefaultColumns, null);
  }

  /** Returns a copy of this policy with the specified {@code maxCost} budget. */
  public ReconcileCapturePolicy withMaxCost(JobCostHint maxCost) {
    return new ReconcileCapturePolicy(
        columns, outputs, defaultColumnScope, maxDefaultColumns, maxCost);
  }

  @JsonProperty("columns")
  public List<Column> columns() {
    return columns;
  }

  @JsonProperty("outputs")
  public Set<Output> outputs() {
    return outputs;
  }

  @JsonProperty("defaultColumnScope")
  public DefaultColumnScope defaultColumnScope() {
    return defaultColumnScope;
  }

  @JsonProperty("maxDefaultColumns")
  public int maxDefaultColumns() {
    return maxDefaultColumns;
  }

  @JsonProperty("maxCost")
  public JobCostHint maxCost() {
    return maxCost;
  }

  @JsonIgnore
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

  public boolean hasExplicitColumns() {
    return !columns.isEmpty();
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
