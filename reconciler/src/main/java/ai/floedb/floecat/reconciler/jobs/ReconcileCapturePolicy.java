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

import ai.floedb.floecat.stats.spi.JobCostHint;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Output, column, and cost policy for reconcile-time capture execution.
 *
 * <p>The {@link #maxCost()} field limits which stat kinds an engine may attempt within a given
 * context. The sync path sets {@link JobCostHint#CHEAP} or {@link JobCostHint#MEDIUM} to enforce
 * latency budgets; the async path leaves it at the default ({@link JobCostHint#EXPENSIVE}, meaning
 * no restriction). Floescan {@code EXEC_FILE_GROUP} jobs always use {@link JobCostHint#EXPENSIVE}.
 *
 * <p>Engines check {@code estimatedCost().fitsIn(policy.maxCost())} before attempting expensive
 * operations and return partial results rather than blocking when the cost exceeds the budget.
 */
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
      new ReconcileCapturePolicy(List.of(), Set.of(), JobCostHint.EXPENSIVE);

  private final List<Column> columns;
  private final Set<Output> outputs;

  /** Upper bound on job cost; engines skip stat kinds that exceed this hint. */
  private final JobCostHint maxCost;

  private ReconcileCapturePolicy(List<Column> columns, Set<Output> outputs, JobCostHint maxCost) {
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
    this.maxCost = maxCost == null ? JobCostHint.EXPENSIVE : maxCost;
  }

  public static ReconcileCapturePolicy empty() {
    return EMPTY;
  }

  /**
   * Creates a policy with no cost restriction ({@link JobCostHint#EXPENSIVE}).
   *
   * <p>Use this form for async and background jobs where no latency budget applies.
   */
  public static ReconcileCapturePolicy of(List<Column> columns, Set<Output> outputs) {
    return of(columns, outputs, JobCostHint.EXPENSIVE);
  }

  /**
   * Creates a policy with an explicit cost ceiling.
   *
   * <p>Use this form for sync capture, where {@link JobCostHint#CHEAP} or {@link
   * JobCostHint#MEDIUM} restricts which stat kinds the engine may attempt within the latency
   * budget.
   */
  public static ReconcileCapturePolicy of(
      List<Column> columns, Set<Output> outputs, JobCostHint maxCost) {
    if ((columns == null || columns.isEmpty())
        && (outputs == null || outputs.isEmpty())
        && (maxCost == null || maxCost == JobCostHint.EXPENSIVE)) {
      return EMPTY;
    }
    return new ReconcileCapturePolicy(columns, outputs, maxCost);
  }

  public List<Column> columns() {
    return columns;
  }

  public Set<Output> outputs() {
    return outputs;
  }

  /**
   * Upper bound on the cost this policy permits an engine to incur.
   *
   * <p>Engines should call {@code estimatedCost().fitsIn(policy.maxCost())} before attempting
   * expensive operations. If the engine's cost exceeds this hint, it must skip that stat kind and
   * return partial results rather than blocking or failing.
   *
   * <p>Default: {@link JobCostHint#EXPENSIVE} (no restriction). Set to {@link JobCostHint#CHEAP} or
   * {@link JobCostHint#MEDIUM} on sync-path policies to enforce latency budgets.
   */
  public JobCostHint maxCost() {
    return maxCost;
  }

  /**
   * Returns {@code true} when there is no capture work to do (no columns and no outputs).
   *
   * <p>Note: this method is blind to {@link #maxCost()}. A policy with an empty column/output set
   * but a non-default {@code maxCost} still returns {@code true} here because no capture operations
   * are defined regardless of cost. The {@code maxCost} field only constrains operations that
   * exist.
   */
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
