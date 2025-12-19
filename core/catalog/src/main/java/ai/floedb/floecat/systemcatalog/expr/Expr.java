package ai.floedb.floecat.systemcatalog.expr;

/** Logical expression tree for system scan predicates. */
public sealed interface Expr
    permits Expr.ColumnRef,
        Expr.Literal,
        Expr.Eq,
        Expr.And,
        Expr.Or,
        Expr.Gt,
        Expr.IsNull,
        Expr.Not {

  /** Column reference by logical name. */
  record ColumnRef(String name) implements Expr {}

  /** Constant literal (string or null). */
  record Literal(String value) implements Expr {}

  /** Equality comparison. */
  record Eq(Expr left, Expr right) implements Expr {}

  /** Boolean AND. */
  record And(Expr left, Expr right) implements Expr {}

  /** Boolean OR. */
  record Or(Expr left, Expr right) implements Expr {}

  /** Greater-than comparison. */
  record Gt(Expr left, Expr right) implements Expr {}

  /** Null-check helper. */
  record IsNull(Expr expression) implements Expr {}

  /** Negation helper. */
  record Not(Expr expression) implements Expr {}
}
