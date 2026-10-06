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

package ai.floedb.floecat.arrow;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.SequencedSet;

/**
 * The column-name rules of a system-scan projection ({@code required_columns}): names match
 * case-insensitively, surrounding whitespace and blank names are ignored, duplicates collapse to
 * their first occurrence, and output follows the requested order. Names the table does not have are
 * dropped by the caller, so a projection naming only unknown columns has zero columns.
 */
public final class RequiredColumns {

  private RequiredColumns() {}

  /**
   * The requested names, trimmed, lower-cased and de-duplicated in requested order. Empty means
   * every column.
   */
  public static List<String> normalize(List<String> requiredColumns) {
    if (requiredColumns == null || requiredColumns.isEmpty()) {
      return List.of();
    }
    SequencedSet<String> normalized = new LinkedHashSet<>();
    for (String column : requiredColumns) {
      if (column == null) {
        continue;
      }
      String name = key(column);
      if (!name.isEmpty()) {
        normalized.add(name);
      }
    }
    return List.copyOf(normalized);
  }

  /** Lookup key for a column name, comparable with the names {@link #normalize} returns. */
  public static String key(String columnName) {
    return columnName.trim().toLowerCase(Locale.ROOT);
  }

  /**
   * Whether a scan narrowed to {@code normalized} (as {@link #normalize} returns it) needs {@code
   * columnName}. Empty needs every column.
   */
  public static boolean includes(List<String> normalized, String columnName) {
    return normalized.isEmpty() || normalized.contains(key(columnName));
  }
}
