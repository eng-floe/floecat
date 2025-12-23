package ai.floedb.floecat.systemcatalog.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

class ArrowProjectOperatorTest {

  @Test
  void retainsRequestedColumnOrderAndValues() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("id").setLogicalType("INT").setFieldId(1).build(),
            SchemaColumn.newBuilder()
                .setName("label")
                .setLogicalType("VARCHAR")
                .setFieldId(2)
                .build(),
            SchemaColumn.newBuilder()
                .setName("extra")
                .setLogicalType("VARCHAR")
                .setFieldId(3)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1, "keep", "a"}),
            new SystemObjectRow(new Object[] {2, "skip", "b"}));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      RowStreamToArrowBatchAdapter adapter = new RowStreamToArrowBatchAdapter(allocator, schema, 2);
      ColumnarBatch batch = adapter.adapt(rows.stream()).toList().get(0);

      List<String> required = List.of("label", "id");
      ColumnarBatch projected = ArrowProjectOperator.project(batch, required, allocator);
      try (projected) {
        VectorSchemaRoot root = projected.root();
        assertThat(root.getFieldVectors()).hasSize(2);

        VarCharVector labelVector = (VarCharVector) root.getVector(0);
        assertThat(new String(labelVector.get(0), StandardCharsets.UTF_8)).isEqualTo("keep");
        assertThat(new String(labelVector.get(1), StandardCharsets.UTF_8)).isEqualTo("skip");

        IntVector idVector = (IntVector) root.getVector(1);
        assertThat(idVector.get(0)).isEqualTo(1);
        assertThat(idVector.get(1)).isEqualTo(2);
      }
    }
  }

  @Test
  void returnsOriginalBatchWhenNoColumnsRequested() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("id").setLogicalType("INT").setFieldId(1).build());
    List<SystemObjectRow> rows =
        List.of(new SystemObjectRow(new Object[] {1}), new SystemObjectRow(new Object[] {2}));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      RowStreamToArrowBatchAdapter adapter = new RowStreamToArrowBatchAdapter(allocator, schema, 2);
      ColumnarBatch batch = adapter.adapt(rows.stream()).toList().get(0);

      ColumnarBatch projected = ArrowProjectOperator.project(batch, List.of(), allocator);
      try (projected) {
        assertThat(projected.root().getFieldVectors()).hasSize(1);
      }
    }
  }
}
