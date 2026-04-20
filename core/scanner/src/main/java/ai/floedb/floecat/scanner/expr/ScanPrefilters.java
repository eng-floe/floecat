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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/** Extracts safe equality prefilters from scanner predicate expressions. */
public final class ScanPrefilters {

  private ScanPrefilters() {}

  /**
   * Extracts scanner prefilter constraints from a predicate.
   *
   * <p>Supported syntax is intentionally small and safe:
   *
   * <ul>
   *   <li>{@code AND} conjunctions
   *   <li>{@code Eq(ColumnRef, Literal)} and flipped order
   * </ul>
   *
   * <p>Unsupported nodes are ignored. Contradictory numeric equalities produce {@link
   * ScanPrefilter#impossibleFilter()}.
   */
  public static ScanPrefilter fromPredicate(Expr predicate, Set<String> supportedColumns) {
    if (predicate == null) {
      return ScanPrefilter.none();
    }

    Set<String> normalizedColumns = normalizeColumns(supportedColumns);
    Map<String, Set<String>> equalsValues = new HashMap<>();
    Map<String, Long> equalsLongs = new HashMap<>();

    if (!collect(predicate, normalizedColumns, equalsValues, equalsLongs)) {
      return ScanPrefilter.impossibleFilter();
    }

    if (equalsValues.isEmpty() && equalsLongs.isEmpty()) {
      return ScanPrefilter.none();
    }
    return new ScanPrefilter(false, equalsValues, equalsLongs);
  }

  private static boolean collect(
      Expr expr,
      Set<String> supportedColumns,
      Map<String, Set<String>> equalsValues,
      Map<String, Long> equalsLongs) {
    return switch (expr) {
      case Expr.And and ->
          collect(and.left(), supportedColumns, equalsValues, equalsLongs)
              && collect(and.right(), supportedColumns, equalsValues, equalsLongs);
      case Expr.Eq eq -> collectEq(eq, supportedColumns, equalsValues, equalsLongs);
      default -> true;
    };
  }

  private static boolean collectEq(
      Expr.Eq eq,
      Set<String> supportedColumns,
      Map<String, Set<String>> equalsValues,
      Map<String, Long> equalsLongs) {
    ColumnLiteralPair pair = columnLiteralPair(eq.left(), eq.right());
    if (pair == null) {
      return true;
    }

    String column = pair.column().toLowerCase(Locale.ROOT);
    if (!supportedColumns.isEmpty() && !supportedColumns.contains(column)) {
      return true;
    }

    equalsValues.computeIfAbsent(column, ignored -> new HashSet<>()).add(pair.literal());

    OptionalLong parsed = parseLong(pair.literal());
    if (parsed.isPresent()) {
      Long current = equalsLongs.get(column);
      if (current != null && current.longValue() != parsed.getAsLong()) {
        return false;
      }
      equalsLongs.put(column, parsed.getAsLong());
    }
    return true;
  }

  private static ColumnLiteralPair columnLiteralPair(Expr left, Expr right) {
    if (left instanceof Expr.ColumnRef column && right instanceof Expr.Literal literal) {
      return new ColumnLiteralPair(column.name(), literal.value());
    }
    if (right instanceof Expr.ColumnRef column && left instanceof Expr.Literal literal) {
      return new ColumnLiteralPair(column.name(), literal.value());
    }
    return null;
  }

  private static Set<String> normalizeColumns(Set<String> supportedColumns) {
    if (supportedColumns == null || supportedColumns.isEmpty()) {
      return Set.of();
    }
    return supportedColumns.stream()
        .map(c -> c.toLowerCase(Locale.ROOT))
        .collect(java.util.stream.Collectors.toSet());
  }

  private static OptionalLong parseLong(String literal) {
    try {
      return OptionalLong.of(Long.parseLong(literal));
    } catch (NumberFormatException ignored) {
      return OptionalLong.empty();
    }
  }

  private record ColumnLiteralPair(String column, String literal) {}
}
