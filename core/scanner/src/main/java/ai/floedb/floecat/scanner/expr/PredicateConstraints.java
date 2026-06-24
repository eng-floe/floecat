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
import java.util.Optional;
import java.util.Set;

/**
 * Conservative finite-domain constraints extracted from a predicate expression.
 *
 * <p>These constraints are an optimization input only. The original predicate must still be
 * evaluated downstream for correctness.
 */
public final class PredicateConstraints {

  private static final PredicateConstraints EMPTY = new PredicateConstraints(false, Map.of());
  private static final PredicateConstraints ALWAYS_FALSE = new PredicateConstraints(true, Map.of());

  private final boolean alwaysFalse;
  private final Map<String, Set<String>> valuesByColumn;

  private PredicateConstraints(boolean alwaysFalse, Map<String, Set<String>> valuesByColumn) {
    this.alwaysFalse = alwaysFalse;
    Map<String, Set<String>> copy = new HashMap<>();
    valuesByColumn.forEach((k, v) -> copy.put(canonical(k), Set.copyOf(v)));
    this.valuesByColumn = Map.copyOf(copy);
  }

  public static PredicateConstraints empty() {
    return EMPTY;
  }

  public static PredicateConstraints alwaysFalse() {
    return ALWAYS_FALSE;
  }

  public static PredicateConstraints from(Expr expr) {
    if (expr == null) {
      return empty();
    }
    return extract(expr);
  }

  public boolean isAlwaysFalse() {
    return alwaysFalse;
  }

  public Optional<Set<String>> values(String columnName) {
    Set<String> values = valuesByColumn.get(canonical(columnName));
    return values == null ? Optional.empty() : Optional.of(values);
  }

  public boolean hasConstraint(String columnName) {
    return values(columnName).isPresent();
  }

  private static PredicateConstraints extract(Expr expr) {
    return switch (expr) {
      case Expr.And and -> intersect(extract(and.left()), extract(and.right()));
      case Expr.Or or -> union(extract(or.left()), extract(or.right()));
      case Expr.Eq eq -> equality(eq);
      case Expr.BooleanLiteral bool -> bool.value() ? empty() : alwaysFalse();
      default -> empty();
    };
  }

  private static PredicateConstraints equality(Expr.Eq eq) {
    ColumnLiteral pair = columnLiteral(eq.left(), eq.right());
    if (pair == null || pair.literal() == null) {
      return empty();
    }
    return new PredicateConstraints(false, Map.of(pair.column(), Set.of(pair.literal())));
  }

  private static ColumnLiteral columnLiteral(Expr left, Expr right) {
    if (left instanceof Expr.ColumnRef column && right instanceof Expr.Literal literal) {
      return new ColumnLiteral(column.name(), literal.value());
    }
    if (right instanceof Expr.ColumnRef column && left instanceof Expr.Literal literal) {
      return new ColumnLiteral(column.name(), literal.value());
    }
    return null;
  }

  private static PredicateConstraints intersect(
      PredicateConstraints left, PredicateConstraints right) {
    if (left.alwaysFalse || right.alwaysFalse) {
      return alwaysFalse();
    }
    Map<String, Set<String>> out = new HashMap<>();
    left.valuesByColumn.forEach((k, v) -> out.put(k, new HashSet<>(v)));
    for (Map.Entry<String, Set<String>> entry : right.valuesByColumn.entrySet()) {
      out.merge(
          entry.getKey(),
          new HashSet<>(entry.getValue()),
          (a, b) -> {
            a.retainAll(b);
            return a;
          });
      if (out.get(entry.getKey()).isEmpty()) {
        return alwaysFalse();
      }
    }
    return out.isEmpty() ? empty() : new PredicateConstraints(false, out);
  }

  private static PredicateConstraints union(PredicateConstraints left, PredicateConstraints right) {
    if (left.alwaysFalse) {
      return right;
    }
    if (right.alwaysFalse) {
      return left;
    }
    Map<String, Set<String>> out = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : left.valuesByColumn.entrySet()) {
      Set<String> rightValues = right.valuesByColumn.get(entry.getKey());
      if (rightValues == null) {
        continue;
      }
      Set<String> values = new HashSet<>(entry.getValue());
      values.addAll(rightValues);
      out.put(entry.getKey(), values);
    }
    return out.isEmpty() ? empty() : new PredicateConstraints(false, out);
  }

  private static String canonical(String columnName) {
    return columnName == null ? "" : columnName.toLowerCase(Locale.ROOT);
  }

  private record ColumnLiteral(String column, String literal) {}
}
