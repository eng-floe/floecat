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

package ai.floedb.floecat.service.query.system;

import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.expr.PredicateExpressionProvider;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import java.util.List;
import java.util.stream.Stream;

public final class SystemRowFilter {

  public static final PredicateExpressionProvider EXPRESSION_PROVIDER = SystemRowFilter::toExpr;

  public static List<SystemObjectRow> applyPredicates(
      List<SystemObjectRow> rows, List<SchemaColumn> schema, List<Predicate> predicates) {

    if (predicates.isEmpty()) return rows;

    return filter(rows.stream(), schema, predicates).toList();
  }

  public static Stream<SystemObjectRow> filter(
      Stream<SystemObjectRow> rows, List<SchemaColumn> schema, List<Predicate> predicates) {
    if (predicates.isEmpty()) return rows;
    return rows.filter(row -> matchesAll(row, schema, predicates));
  }

  private static boolean matchesAll(
      SystemObjectRow row, List<SchemaColumn> schema, List<Predicate> predicates) {

    for (Predicate p : predicates) {
      if (!matches(row, schema, p)) {
        return false;
      }
    }
    return true;
  }

  private static boolean matches(SystemObjectRow row, List<SchemaColumn> schema, Predicate p) {

    int idx = columnIndex(schema, p.getColumn());
    if (idx < 0) return false;

    Object value = row.values()[idx];

    return switch (p.getOp()) {
      case OP_EQ -> value != null && value.toString().equals(p.getValues(0));
      case OP_IS_NULL -> value == null;
      case OP_IS_NOT_NULL -> value != null;
      default ->
          throw new UnsupportedOperationException("Predicate not yet supported: " + p.getOp());
    };
  }

  private static int columnIndex(List<SchemaColumn> schema, String name) {

    for (int i = 0; i < schema.size(); i++) {
      if (schema.get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }
    return -1;
  }

  private SystemRowFilter() {}

  /** Converts the provided predicates into a logical expression tree. */
  public static Expr toExpr(List<Predicate> predicates) {
    if (predicates.isEmpty()) {
      return null;
    }
    Expr expr = null;
    for (Predicate predicate : predicates) {
      Expr predicateExpr = predicateExpr(predicate);
      expr = expr == null ? predicateExpr : new Expr.And(expr, predicateExpr);
    }
    return expr;
  }

  private static Expr predicateExpr(Predicate predicate) {
    Expr column = new Expr.ColumnRef(predicate.getColumn());

    return switch (predicate.getOp()) {
      case OP_EQ -> new Expr.Eq(column, new Expr.Literal(firstValue(predicate)));
      case OP_IS_NULL -> new Expr.IsNull(column);
      case OP_IS_NOT_NULL -> new Expr.Not(new Expr.IsNull(column));
      default ->
          throw new UnsupportedOperationException(
              "Predicate not yet supported: " + predicate.getOp());
    };
  }

  private static String firstValue(Predicate predicate) {
    return predicate.getValuesCount() > 0 ? predicate.getValues(0) : null;
  }
}
