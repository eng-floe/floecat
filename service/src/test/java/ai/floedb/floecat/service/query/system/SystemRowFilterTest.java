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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.expr.Expr;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import java.util.List;
import org.junit.jupiter.api.Test;

class SystemRowFilterTest {

  private static final List<SchemaColumn> SCHEMA =
      List.of(
          SchemaColumn.newBuilder().setName("col_a").build(),
          SchemaColumn.newBuilder().setName("col_b").build());

  @Test
  void emptyPredicatesReturnsAllRows() {
    var rows =
        List.of(
            new SystemObjectRow(new Object[] {"a", "b"}),
            new SystemObjectRow(new Object[] {"c", "d"}));

    var result = SystemRowFilter.applyPredicates(rows, SCHEMA, List.of());

    assertThat(result).hasSize(2);
  }

  @Test
  void eqPredicateMatchesCorrectRow() {
    var rows =
        List.of(
            new SystemObjectRow(new Object[] {"a", "b"}),
            new SystemObjectRow(new Object[] {"c", "d"}));

    Predicate predicate =
        Predicate.newBuilder().setColumn("col_a").setOp(Operator.OP_EQ).addValues("a").build();

    var result = SystemRowFilter.applyPredicates(rows, SCHEMA, List.of(predicate));

    assertThat(result).hasSize(1);
    assertThat(result.get(0).values()[0]).isEqualTo("a");
  }

  @Test
  void eqPredicateWithNonMatchingValueReturnsEmpty() {
    var rows =
        List.of(
            new SystemObjectRow(new Object[] {"a", "b"}),
            new SystemObjectRow(new Object[] {"c", "d"}));

    Predicate predicate =
        Predicate.newBuilder().setColumn("col_a").setOp(Operator.OP_EQ).addValues("x").build();

    var result = SystemRowFilter.applyPredicates(rows, SCHEMA, List.of(predicate));

    assertThat(result).isEmpty();
  }

  @Test
  void isNullPredicateMatchesNullValue() {
    var rows =
        List.of(
            new SystemObjectRow(new Object[] {null, "b"}),
            new SystemObjectRow(new Object[] {"c", "d"}));

    Predicate predicate =
        Predicate.newBuilder().setColumn("col_a").setOp(Operator.OP_IS_NULL).build();

    var result = SystemRowFilter.applyPredicates(rows, SCHEMA, List.of(predicate));

    assertThat(result).hasSize(1);
    assertThat(result.get(0).values()[0]).isNull();
  }

  @Test
  void isNotNullPredicateMatchesNonNullValue() {
    var rows =
        List.of(
            new SystemObjectRow(new Object[] {null, "b"}),
            new SystemObjectRow(new Object[] {"c", "d"}));

    Predicate predicate =
        Predicate.newBuilder().setColumn("col_a").setOp(Operator.OP_IS_NOT_NULL).build();

    var result = SystemRowFilter.applyPredicates(rows, SCHEMA, List.of(predicate));

    assertThat(result).hasSize(1);
    assertThat(result.get(0).values()[0]).isEqualTo("c");
  }

  @Test
  void unknownColumnRejectsAllRows() {
    var rows =
        List.of(
            new SystemObjectRow(new Object[] {"a", "b"}),
            new SystemObjectRow(new Object[] {"c", "d"}));

    Predicate predicate =
        Predicate.newBuilder()
            .setColumn("does_not_exist")
            .setOp(Operator.OP_EQ)
            .addValues("a")
            .build();

    var result = SystemRowFilter.applyPredicates(rows, SCHEMA, List.of(predicate));

    assertThat(result).isEmpty();
  }

  @Test
  void columnNameMatchingIsCaseInsensitive() {
    var rows =
        List.of(
            new SystemObjectRow(new Object[] {"a", "b"}),
            new SystemObjectRow(new Object[] {"c", "d"}));

    Predicate predicate =
        Predicate.newBuilder().setColumn("COL_A").setOp(Operator.OP_EQ).addValues("a").build();

    var result = SystemRowFilter.applyPredicates(rows, SCHEMA, List.of(predicate));

    assertThat(result).hasSize(1);
    assertThat(result.get(0).values()[0]).isEqualTo("a");
  }

  @Test
  void eqPredicateDoesNotMatchNullValues() {
    var rows =
        List.of(
            new SystemObjectRow(new Object[] {null, "b"}),
            new SystemObjectRow(new Object[] {"a", "d"}));

    Predicate predicate =
        Predicate.newBuilder().setColumn("col_a").setOp(Operator.OP_EQ).addValues("a").build();

    var result = SystemRowFilter.applyPredicates(rows, SCHEMA, List.of(predicate));

    assertThat(result).hasSize(1);
    assertThat(result.get(0).values()[0]).isEqualTo("a");
  }

  @Test
  void multiplePredicatesAreCombinedWithAndSemantics() {
    var rows =
        List.of(
            new SystemObjectRow(new Object[] {"a", "b"}),
            new SystemObjectRow(new Object[] {"a", "d"}),
            new SystemObjectRow(new Object[] {"c", "b"}),
            new SystemObjectRow(new Object[] {"c", "d"}));

    Predicate predicate1 =
        Predicate.newBuilder().setColumn("col_a").setOp(Operator.OP_EQ).addValues("a").build();

    Predicate predicate2 =
        Predicate.newBuilder().setColumn("col_b").setOp(Operator.OP_EQ).addValues("b").build();

    var result = SystemRowFilter.applyPredicates(rows, SCHEMA, List.of(predicate1, predicate2));

    assertThat(result).hasSize(1);
    assertThat(result.get(0).values()).containsExactly("a", "b");
  }

  @Test
  void expressionCapturesEqAndNullPredicates() {
    Predicate eq =
        Predicate.newBuilder().setColumn("col_a").setOp(Operator.OP_EQ).addValues("a").build();
    Predicate isNull = Predicate.newBuilder().setColumn("col_b").setOp(Operator.OP_IS_NULL).build();

    Expr expr = SystemRowFilter.toExpr(List.of(eq, isNull));

    assertThat(expr).isInstanceOf(Expr.And.class);
    Expr.And and = (Expr.And) expr;

    assertThat(and.left()).isInstanceOf(Expr.Eq.class);
    Expr.Eq eqExpr = (Expr.Eq) and.left();
    assertThat(eqExpr.left()).isInstanceOf(Expr.ColumnRef.class);
    assertThat(eqExpr.right()).isInstanceOf(Expr.Literal.class);

    assertThat(and.right()).isInstanceOf(Expr.IsNull.class);
  }

  @Test
  void expressionRepresentsIsNotNullAsNegation() {
    Predicate predicate =
        Predicate.newBuilder().setColumn("col_a").setOp(Operator.OP_IS_NOT_NULL).build();

    Expr expr = SystemRowFilter.toExpr(List.of(predicate));

    assertThat(expr).isInstanceOf(Expr.Not.class);
    Expr.Not not = (Expr.Not) expr;
    assertThat(not.expression()).isInstanceOf(Expr.IsNull.class);
  }

  private static Predicate predicate(String column, Operator op, String... values) {
    Predicate.Builder builder = Predicate.newBuilder().setColumn(column).setOp(op);
    for (String value : values) {
      builder.addValues(value);
    }
    return builder.build();
  }
}
