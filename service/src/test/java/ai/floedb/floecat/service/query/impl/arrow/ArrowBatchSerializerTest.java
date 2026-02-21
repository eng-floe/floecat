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

package ai.floedb.floecat.service.query.impl.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.arrow.ArrowBatchSink;
import ai.floedb.floecat.arrow.SimpleColumnarBatch;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class ArrowBatchSerializerTest {

  // -------------------------------------------------------------------------
  //  schemaForColumns tests
  // -------------------------------------------------------------------------

  @Test
  void schemaForColumns_emptyRequired_returnsFullSchema() {
    List<SchemaColumn> scannerSchema = List.of(col("id", "INT"), col("name", "VARCHAR"));

    Schema result = ArrowBatchSerializer.schemaForColumns(scannerSchema, List.of());
    assertThat(result.getFields()).hasSize(2);
    assertThat(result.getFields().get(0).getName()).isEqualTo("id");
    assertThat(result.getFields().get(1).getName()).isEqualTo("name");
  }

  @Test
  void schemaForColumns_nullRequired_returnsFullSchema() {
    List<SchemaColumn> scannerSchema = List.of(col("id", "INT"));
    Schema result = ArrowBatchSerializer.schemaForColumns(scannerSchema, null);
    assertThat(result.getFields()).hasSize(1);
  }

  @Test
  void schemaForColumns_projection_reordersAndFilters() {
    List<SchemaColumn> scannerSchema =
        List.of(col("id", "INT"), col("name", "VARCHAR"), col("status", "VARCHAR"));

    Schema result = ArrowBatchSerializer.schemaForColumns(scannerSchema, List.of("status", "id"));
    assertThat(result.getFields()).hasSize(2);
    assertThat(result.getFields().get(0).getName()).isEqualTo("status");
    assertThat(result.getFields().get(1).getName()).isEqualTo("id");
  }

  @Test
  void schemaForColumns_unknownColumnsSilentlyDropped() {
    List<SchemaColumn> scannerSchema = List.of(col("id", "INT"), col("name", "VARCHAR"));
    Schema result =
        ArrowBatchSerializer.schemaForColumns(scannerSchema, List.of("id", "nonexistent"));
    assertThat(result.getFields()).hasSize(1);
    assertThat(result.getFields().get(0).getName()).isEqualTo("id");
  }

  @Test
  void schemaForColumns_duplicatesDeduplicatedPreservingFirstOccurrence() {
    List<SchemaColumn> scannerSchema = List.of(col("id", "INT"), col("name", "VARCHAR"));
    Schema result =
        ArrowBatchSerializer.schemaForColumns(scannerSchema, List.of("id", "name", "id"));
    assertThat(result.getFields()).hasSize(2);
    assertThat(result.getFields().get(0).getName()).isEqualTo("id");
    assertThat(result.getFields().get(1).getName()).isEqualTo("name");
  }

  @Test
  void schemaForColumns_caseInsensitiveMatching() {
    List<SchemaColumn> scannerSchema = List.of(col("MyColumn", "VARCHAR"));
    Schema result = ArrowBatchSerializer.schemaForColumns(scannerSchema, List.of("MYCOLUMN"));
    assertThat(result.getFields()).hasSize(1);
    assertThat(result.getFields().get(0).getName()).isEqualTo("MyColumn");
  }

  // -------------------------------------------------------------------------
  //  serialize tests
  // -------------------------------------------------------------------------

  @Test
  void serialize_callsSinkInOrder() {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = singleIntSchema();
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
      root.allocateNew();
      root.setRowCount(1);

      SimpleColumnarBatch batch = new SimpleColumnarBatch(root);
      ArrowScanPlan plan = new ArrowScanPlan(schema, Stream.of(batch));

      List<String> calls = new ArrayList<>();
      ArrowBatchSink sink =
          new ArrowBatchSink() {
            @Override
            public void onSchema(Schema s) {
              calls.add("schema");
            }

            @Override
            public void onBatch(VectorSchemaRoot r) {
              calls.add("batch");
            }

            @Override
            public void onComplete() {
              calls.add("complete");
            }
          };

      AtomicBoolean cleanupCalled = new AtomicBoolean(false);
      ArrowBatchSerializer.serialize(plan, sink, () -> false, () -> cleanupCalled.set(true));

      assertThat(calls).containsExactly("schema", "batch", "complete");
      assertThat(cleanupCalled).isTrue();
    }
  }

  @Test
  void serialize_cleanupCalledOnException() {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = singleIntSchema();
      ArrowScanPlan plan = new ArrowScanPlan(schema, Stream.of());

      ArrowBatchSink failingSink =
          new ArrowBatchSink() {
            @Override
            public void onSchema(Schema s) {
              throw new RuntimeException("schema error");
            }

            @Override
            public void onBatch(VectorSchemaRoot r) {}

            @Override
            public void onComplete() {}
          };

      AtomicBoolean cleanupCalled = new AtomicBoolean(false);

      assertThatThrownBy(
              () ->
                  ArrowBatchSerializer.serialize(
                      plan, failingSink, () -> false, () -> cleanupCalled.set(true)))
          .isInstanceOf(RuntimeException.class)
          .hasMessage("schema error");

      assertThat(cleanupCalled).isTrue();
    }
  }

  @Test
  void serialize_cancellationStopsStreamBeforeComplete() {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = singleIntSchema();

      // Produce two batches; cancel after the first.
      // Both roots are closed explicitly to avoid Arrow memory-leak errors when the
      // second batch is never consumed by the serializer (it stops on cancellation).
      AtomicInteger batchCount = new AtomicInteger(0);
      VectorSchemaRoot root1 = VectorSchemaRoot.create(schema, allocator);
      root1.allocateNew();
      root1.setRowCount(1);
      VectorSchemaRoot root2 = VectorSchemaRoot.create(schema, allocator);
      root2.allocateNew();
      root2.setRowCount(1);

      // wrap in try-with-resources so both are closed even if root2 is never consumed
      try (root1;
          root2) {
        SimpleColumnarBatch batch1 = new SimpleColumnarBatch(root1);
        SimpleColumnarBatch batch2 = new SimpleColumnarBatch(root2);
        ArrowScanPlan plan = new ArrowScanPlan(schema, Stream.of(batch1, batch2));

        AtomicBoolean cancelled = new AtomicBoolean(false);
        List<String> calls = new ArrayList<>();

        ArrowBatchSerializer.serialize(
            plan,
            new ArrowBatchSink() {
              @Override
              public void onSchema(Schema s) {
                calls.add("schema");
              }

              @Override
              public void onBatch(VectorSchemaRoot r) {
                calls.add("batch");
                batchCount.incrementAndGet();
                cancelled.set(true); // Cancel after first batch
              }

              @Override
              public void onComplete() {
                calls.add("complete");
              }
            },
            cancelled::get,
            () -> {});

        // onComplete must NOT be called when cancelled
        assertThat(calls).containsExactly("schema", "batch");
        assertThat(batchCount.get()).isEqualTo(1);
      }
    }
  }

  @Test
  void serialize_emptyStream_callsSchemaAndComplete() {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Schema schema = singleIntSchema();
      ArrowScanPlan plan = new ArrowScanPlan(schema, Stream.of());

      List<String> calls = new ArrayList<>();
      ArrowBatchSerializer.serialize(
          plan,
          new ArrowBatchSink() {
            @Override
            public void onSchema(Schema s) {
              calls.add("schema");
            }

            @Override
            public void onBatch(VectorSchemaRoot r) {
              calls.add("batch");
            }

            @Override
            public void onComplete() {
              calls.add("complete");
            }
          },
          () -> false,
          () -> {});

      assertThat(calls).containsExactly("schema", "complete");
    }
  }

  // -------------------------------------------------------------------------
  //  Helpers
  // -------------------------------------------------------------------------

  private static SchemaColumn col(String name, String type) {
    return SchemaColumn.newBuilder().setName(name).setLogicalType(type).setNullable(true).build();
  }

  private static Schema singleIntSchema() {
    return new Schema(List.of(Field.nullable("val", new ArrowType.Int(32, true))));
  }
}
