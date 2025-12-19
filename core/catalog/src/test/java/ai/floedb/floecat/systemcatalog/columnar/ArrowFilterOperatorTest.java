package ai.floedb.floecat.systemcatalog.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.expr.Expr;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

class ArrowFilterOperatorTest {

  @Test
  void filtersRowsAccordingToExpression() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("id").setLogicalType("INT").setFieldId(1).build(),
            SchemaColumn.newBuilder()
                .setName("label")
                .setLogicalType("VARCHAR")
                .setFieldId(2)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1, "keep"}),
            new SystemObjectRow(new Object[] {2, "skip"}),
            new SystemObjectRow(new Object[] {3, "keep"}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("id"), new Expr.Literal("1"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      RowStreamToArrowBatchAdapter adapter = new RowStreamToArrowBatchAdapter(allocator, schema, 3);
      List<ColumnarBatch> batches = adapter.adapt(rows.stream()).toList();

      assertThat(batches).hasSize(1);
      ColumnarBatch batch = batches.get(0);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        VectorSchemaRoot root = filtered.root();
        assertThat(root.getRowCount()).isEqualTo(1);

        IntVector idVector = (IntVector) root.getVector(0);
        assertThat(idVector.get(0)).isEqualTo(1);

        VarCharVector labelVector = (VarCharVector) root.getVector(1);
        assertThat(new String(labelVector.get(0), StandardCharsets.UTF_8)).isEqualTo("keep");
      }
    }
  }

  @Test
  void filtersRowsWithGreaterThanExpression() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("id").setLogicalType("INT").setFieldId(1).build(),
            SchemaColumn.newBuilder()
                .setName("label")
                .setLogicalType("VARCHAR")
                .setFieldId(2)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1, "keep"}),
            new SystemObjectRow(new Object[] {2, "skip"}),
            new SystemObjectRow(new Object[] {3, "keep"}));

    Expr expr = new Expr.Gt(new Expr.ColumnRef("id"), new Expr.Literal("1"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      RowStreamToArrowBatchAdapter adapter = new RowStreamToArrowBatchAdapter(allocator, schema, 3);
      List<ColumnarBatch> batches = adapter.adapt(rows.stream()).toList();

      assertThat(batches).hasSize(1);
      ColumnarBatch batch = batches.get(0);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        VectorSchemaRoot root = filtered.root();
        assertThat(root.getRowCount()).isEqualTo(2);

        IntVector idVector = (IntVector) root.getVector(0);
        assertThat(idVector.get(0)).isEqualTo(2);
        assertThat(idVector.get(1)).isEqualTo(3);

        VarCharVector labelVector = (VarCharVector) root.getVector(1);
        assertThat(new String(labelVector.get(0), StandardCharsets.UTF_8)).isEqualTo("skip");
        assertThat(new String(labelVector.get(1), StandardCharsets.UTF_8)).isEqualTo("keep");
      }
    }
  }
}
