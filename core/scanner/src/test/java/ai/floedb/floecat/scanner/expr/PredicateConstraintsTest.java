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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

class PredicateConstraintsTest {

  @Test
  void extractsEqualityDomain() {
    PredicateConstraints constraints = PredicateConstraints.from(eq("table_name", "orders"));

    assertThat(constraints.values("TABLE_NAME")).contains(Set.of("orders"));
  }

  @Test
  void intersectsEqualitiesUnderAnd() {
    PredicateConstraints constraints =
        PredicateConstraints.from(
            new Expr.And(
                new Expr.Or(eq("table_name", "orders"), eq("table_name", "customers")),
                eq("table_name", "orders")));

    assertThat(constraints.values("table_name")).contains(Set.of("orders"));
  }

  @Test
  void detectsContradictoryAnd() {
    PredicateConstraints constraints =
        PredicateConstraints.from(new Expr.And(eq("table_name", "orders"), eq("table_name", "x")));

    assertThat(constraints.isAlwaysFalse()).isTrue();
    assertThat(constraints.values("table_name")).isEmpty();
    assertThat(constraints.hasConstraint("table_name")).isFalse();
  }

  @Test
  void unionsCommonConstraintsUnderOr() {
    PredicateConstraints constraints =
        PredicateConstraints.from(new Expr.Or(eq("table_name", "orders"), eq("table_name", "x")));

    assertThat(constraints.values("table_name")).contains(Set.of("orders", "x"));
  }

  @Test
  void doesNotConstrainColumnMissingFromOneOrBranch() {
    PredicateConstraints constraints =
        PredicateConstraints.from(new Expr.Or(eq("table_name", "orders"), eq("table_schema", "s")));

    assertThat(constraints.values("table_name")).isEmpty();
    assertThat(constraints.values("table_schema")).isEmpty();
  }

  @Test
  void unsupportedExpressionProducesNoConstraints() {
    PredicateConstraints constraints =
        PredicateConstraints.from(
            new Expr.Gt(new Expr.ColumnRef("ordinal_position"), new Expr.Literal("1")));

    assertThat(constraints.values("ordinal_position")).isEmpty();
    assertThat(constraints.isAlwaysFalse()).isFalse();
  }

  @Test
  void falseLiteralProducesAlwaysFalseConstraint() {
    PredicateConstraints constraints = PredicateConstraints.from(new Expr.BooleanLiteral(false));

    assertThat(constraints.isAlwaysFalse()).isTrue();
    assertThat(constraints.values("table_name")).isEmpty();
  }

  private static Expr eq(String column, String value) {
    return new Expr.Eq(new Expr.ColumnRef(column), new Expr.Literal(value));
  }
}
