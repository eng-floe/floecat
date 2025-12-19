package ai.floedb.floecat.service.query.system;

import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.expr.Expr;
import ai.floedb.floecat.systemcatalog.expr.PredicateExpressionProvider;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import java.util.List;

public final class SystemRowFilter {

  public static final PredicateExpressionProvider EXPRESSION_PROVIDER = SystemRowFilter::toExpr;

  public static List<SystemObjectRow> applyPredicates(
      List<SystemObjectRow> rows, List<SchemaColumn> schema, List<Predicate> predicates) {

    if (predicates.isEmpty()) return rows;

    return rows.stream().filter(row -> matchesAll(row, schema, predicates)).toList();
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
