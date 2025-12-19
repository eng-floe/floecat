package ai.floedb.floecat.service.query.system;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.query.rpc.SchemaColumn;
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
}
