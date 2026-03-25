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

package ai.floedb.floecat.stats.spi;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.TableStats;
import java.util.Objects;
import java.util.Optional;

/** One typed payload for a single stats target value. */
public record StatsCaptureValue(
    Optional<TableStats> table,
    Optional<FileColumnStats> file,
    Optional<StatsColumnValue> column,
    Optional<StatsExpressionValue> expression) {

  public StatsCaptureValue {
    table = Objects.requireNonNullElse(table, Optional.empty());
    file = Objects.requireNonNullElse(file, Optional.empty());
    column = Objects.requireNonNullElse(column, Optional.empty());
    expression = Objects.requireNonNullElse(expression, Optional.empty());
    int present =
        (table.isPresent() ? 1 : 0)
            + (file.isPresent() ? 1 : 0)
            + (column.isPresent() ? 1 : 0)
            + (expression.isPresent() ? 1 : 0);
    if (present != 1) {
      throw new IllegalArgumentException("Exactly one payload must be set");
    }
  }

  public static StatsCaptureValue forTable(TableStats tableStats) {
    return new StatsCaptureValue(
        Optional.of(Objects.requireNonNull(tableStats, "tableStats")),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  public static StatsCaptureValue forFile(FileColumnStats fileStats) {
    return new StatsCaptureValue(
        Optional.empty(),
        Optional.of(Objects.requireNonNull(fileStats, "fileStats")),
        Optional.empty(),
        Optional.empty());
  }

  public static StatsCaptureValue forColumn(StatsColumnValue columnValue) {
    return new StatsCaptureValue(
        Optional.empty(),
        Optional.empty(),
        Optional.of(Objects.requireNonNull(columnValue, "columnValue")),
        Optional.empty());
  }

  public static StatsCaptureValue forExpression(StatsExpressionValue expressionValue) {
    return new StatsCaptureValue(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(Objects.requireNonNull(expressionValue, "expressionValue")));
  }
}
