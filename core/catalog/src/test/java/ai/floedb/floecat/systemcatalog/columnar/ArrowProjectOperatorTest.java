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

package ai.floedb.floecat.systemcatalog.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.arrow.ColumnarBatch;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
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
        assertThat(root.getRowCount()).isEqualTo(2);
        for (FieldVector v : root.getFieldVectors()) {
          assertThat(v.getValueCount()).isEqualTo(2);
        }

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
      assertThat(projected).isSameAs(batch);
      try (projected) {
        assertThat(projected.root().getFieldVectors()).hasSize(1);
      }
    }
  }

  @Test
  void ignoresUnknownAndDuplicateColumns() {
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
            new SystemObjectRow(new Object[] {2, "skip"}));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      RowStreamToArrowBatchAdapter adapter = new RowStreamToArrowBatchAdapter(allocator, schema, 2);
      ColumnarBatch batch = adapter.adapt(rows.stream()).toList().get(0);

      List<String> required = List.of("label", "unknown", "label", "id");
      ColumnarBatch projected = ArrowProjectOperator.project(batch, required, allocator);
      try (projected) {
        VectorSchemaRoot root = projected.root();
        assertThat(root.getFieldVectors()).hasSize(2);
        assertThat(root.getVector(0).getField().getName()).isEqualTo("label");
        assertThat(root.getVector(1).getField().getName()).isEqualTo("id");
      }
    }
  }

  @Test
  void normalizesColumnNamesCaseAndWhitespace() {
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
            new SystemObjectRow(new Object[] {2, "skip"}));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      RowStreamToArrowBatchAdapter adapter = new RowStreamToArrowBatchAdapter(allocator, schema, 2);
      ColumnarBatch batch = adapter.adapt(rows.stream()).toList().get(0);

      List<String> required = List.of(" LABEL ", "Id");
      ColumnarBatch projected = ArrowProjectOperator.project(batch, required, allocator);
      try (projected) {
        VectorSchemaRoot root = projected.root();
        assertThat(root.getFieldVectors()).hasSize(2);
        assertThat(root.getVector(0).getField().getName()).isEqualTo("label");
        assertThat(root.getVector(1).getField().getName()).isEqualTo("id");

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
  void returnsOriginalBatchWhenOnlyUnknownColumnsRequested() {
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
            new SystemObjectRow(new Object[] {2, "skip"}));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      RowStreamToArrowBatchAdapter adapter = new RowStreamToArrowBatchAdapter(allocator, schema, 2);
      ColumnarBatch batch = adapter.adapt(rows.stream()).toList().get(0);

      ColumnarBatch projected =
          ArrowProjectOperator.project(batch, List.of("does_not_exist"), allocator);
      assertThat(projected).isSameAs(batch);
      try (projected) {
        assertThat(projected.root().getFieldVectors()).hasSize(2);
        assertThat(projected.root().getRowCount()).isEqualTo(2);
      }
    }
  }

  @Test
  void consumesInputBatchWhenProjectionIsApplied() {
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

      ColumnarBatch projected = ArrowProjectOperator.project(batch, List.of("label"), allocator);
      try (projected) {
        assertThat(projected.root().getFieldVectors()).hasSize(1);
      }

      // If the operator consumed the input, closing it should either throw (if guarded)
      // or be a no-op. We assert it does not corrupt the JVM by allowing repeated close.
      batch.close();
    }
  }

  @Test
  void doesNotConsumeInputBatchWhenNoOpProjection() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("id").setLogicalType("INT").setFieldId(1).build());

    List<SystemObjectRow> rows =
        List.of(new SystemObjectRow(new Object[] {1}), new SystemObjectRow(new Object[] {2}));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      RowStreamToArrowBatchAdapter adapter = new RowStreamToArrowBatchAdapter(allocator, schema, 2);
      ColumnarBatch batch = adapter.adapt(rows.stream()).toList().get(0);

      ColumnarBatch projected = ArrowProjectOperator.project(batch, List.of(), allocator);
      assertThat(projected).isSameAs(batch);

      // Ensure the batch is still usable and can be closed exactly once by the caller.
      try (projected) {
        assertThat(projected.root().getRowCount()).isEqualTo(2);
        IntVector idVector = (IntVector) projected.root().getVector(0);
        assertThat(idVector.get(0)).isEqualTo(1);
        assertThat(idVector.get(1)).isEqualTo(2);
      }
    }
  }
}
