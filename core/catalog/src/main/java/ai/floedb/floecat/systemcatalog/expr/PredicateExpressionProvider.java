package ai.floedb.floecat.systemcatalog.expr;

import ai.floedb.floecat.common.rpc.Predicate;
import java.util.List;

/** Functional contract for exporting predicates as expression trees. */
@FunctionalInterface
public interface PredicateExpressionProvider {

  /** Returns an expression tree that semantically matches the supplied predicates. */
  Expr toExpr(List<Predicate> predicates);
}
