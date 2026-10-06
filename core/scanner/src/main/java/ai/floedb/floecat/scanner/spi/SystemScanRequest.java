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

package ai.floedb.floecat.scanner.spi;

import ai.floedb.floecat.arrow.RequiredColumns;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.expr.PredicateConstraints;
import java.util.List;

/** Shared row/Arrow scan request metadata for system-object scanners. */
public record SystemScanRequest(
    Expr predicate, PredicateConstraints constraints, List<String> requiredColumns) {

  public SystemScanRequest {
    constraints = constraints == null ? PredicateConstraints.empty() : constraints;
    requiredColumns = requiredColumns == null ? List.of() : List.copyOf(requiredColumns);
  }

  public static SystemScanRequest of(Expr predicate, List<String> requiredColumns) {
    return new SystemScanRequest(predicate, PredicateConstraints.from(predicate), requiredColumns);
  }

  public static SystemScanRequest empty() {
    return of(null, List.of());
  }

  /**
   * Whether the scan must produce {@code column}: the request names it, names no column (every
   * column), or the predicate reads it. The predicate is evaluated before projection, so a column
   * it reads is needed even when the caller does not keep it.
   */
  public boolean needs(String column) {
    return RequiredColumns.includes(RequiredColumns.normalize(requiredColumns), column)
        || readsColumn(predicate, RequiredColumns.key(column));
  }

  private static boolean readsColumn(Expr expr, String key) {
    return switch (expr) {
      case null -> false;
      case Expr.ColumnRef ref -> RequiredColumns.key(ref.name()).equals(key);
      case Expr.Literal literal -> false;
      case Expr.BooleanLiteral literal -> false;
      case Expr.Eq eq -> readsColumn(eq.left(), key) || readsColumn(eq.right(), key);
      case Expr.And and -> readsColumn(and.left(), key) || readsColumn(and.right(), key);
      case Expr.Or or -> readsColumn(or.left(), key) || readsColumn(or.right(), key);
      case Expr.Gt gt -> readsColumn(gt.left(), key) || readsColumn(gt.right(), key);
      case Expr.IsNull isNull -> readsColumn(isNull.expression(), key);
      case Expr.Not not -> readsColumn(not.expression(), key);
    };
  }
}
