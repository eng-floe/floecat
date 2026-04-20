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

package ai.floedb.floecat.scanner.expr;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Safe prefilter constraints extracted from a predicate expression.
 *
 * <p>This is an optimization-only structure used to narrow source reads. Full predicate semantics
 * are still enforced by the normal row/arrow filtering path.
 */
public record ScanPrefilter(
    boolean impossible, Map<String, Set<String>> equalsValues, Map<String, Long> equalsLongs) {

  private static final ScanPrefilter NONE = new ScanPrefilter(false, Map.of(), Map.of());
  private static final ScanPrefilter IMPOSSIBLE = new ScanPrefilter(true, Map.of(), Map.of());

  public ScanPrefilter {
    equalsValues =
        equalsValues == null
            ? Map.of()
            : equalsValues.entrySet().stream()
                .collect(
                    java.util.stream.Collectors.toUnmodifiableMap(
                        e -> e.getKey().toLowerCase(java.util.Locale.ROOT),
                        e -> Set.copyOf(e.getValue())));
    equalsLongs =
        equalsLongs == null
            ? Map.of()
            : equalsLongs.entrySet().stream()
                .collect(
                    java.util.stream.Collectors.toUnmodifiableMap(
                        e -> e.getKey().toLowerCase(java.util.Locale.ROOT), Map.Entry::getValue));
  }

  /** Returns an empty prefilter with no narrowing constraints. */
  public static ScanPrefilter none() {
    return NONE;
  }

  /** Returns an impossible prefilter indicating the predicate can never match. */
  public static ScanPrefilter impossibleFilter() {
    return IMPOSSIBLE;
  }

  /**
   * Returns string equality candidates for the given column, or an empty set when unconstrained.
   */
  public Set<String> valuesFor(String column) {
    if (column == null || column.isBlank()) {
      return Set.of();
    }
    return equalsValues.getOrDefault(column.toLowerCase(java.util.Locale.ROOT), Set.of());
  }

  /** Returns exact long equality for the given column, when present. */
  public OptionalLong longFor(String column) {
    if (column == null || column.isBlank()) {
      return OptionalLong.empty();
    }
    Long value = equalsLongs.get(column.toLowerCase(java.util.Locale.ROOT));
    return value == null ? OptionalLong.empty() : OptionalLong.of(value);
  }

  /**
   * Returns true when either unconstrained for {@code column} or constrained to contain {@code
   * value}.
   */
  public boolean matchesValue(String column, String value) {
    Set<String> allowed = valuesFor(column);
    return allowed.isEmpty() || allowed.contains(value);
  }

  /**
   * Returns true when either unconstrained for {@code column} or exactly equal to {@code value}.
   */
  public boolean matchesLong(String column, long value) {
    OptionalLong constrained = longFor(column);
    return constrained.isEmpty() || constrained.getAsLong() == value;
  }

  /** Returns true when this prefilter carries no constraints. */
  public boolean isNone() {
    return !impossible && equalsValues.isEmpty() && equalsLongs.isEmpty();
  }

  /** Returns true when this prefilter includes any of the provided constrained columns. */
  public boolean constrainsAny(String... columns) {
    if (columns == null) {
      return false;
    }
    for (String column : columns) {
      if (column == null || column.isBlank()) {
        continue;
      }
      String key = column.toLowerCase(java.util.Locale.ROOT);
      if (equalsValues.containsKey(key) || equalsLongs.containsKey(key)) {
        return true;
      }
    }
    return false;
  }

  /** Returns true when this prefilter includes any of the provided constrained columns. */
  public boolean constrainsAny(List<String> columns) {
    if (columns == null || columns.isEmpty()) {
      return false;
    }
    for (String column : columns) {
      if (column == null || column.isBlank()) {
        continue;
      }
      String key = column.toLowerCase(java.util.Locale.ROOT);
      if (equalsValues.containsKey(key) || equalsLongs.containsKey(key)) {
        return true;
      }
    }
    return false;
  }
}
