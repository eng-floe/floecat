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

class RowStreamToArrowBatchAdapterTest {

  @Test
  void adaptsRowsIntoArrowBatches() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("id")
                .setLogicalType("INT")
                .setNullable(false)
                .build(),
            SchemaColumn.newBuilder()
                .setName("label")
                .setLogicalType("VARCHAR")
                .setNullable(true)
                .build(),
            SchemaColumn.newBuilder()
                .setName("tags")
                .setLogicalType("VARCHAR[]")
                .setNullable(true)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1, "hello", new String[] {"a", "b"}}),
            new SystemObjectRow(new Object[] {2, null, null}));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      RowStreamToArrowBatchAdapter adapter = new RowStreamToArrowBatchAdapter(allocator, schema, 2);
      List<ColumnarBatch> batches = adapter.adapt(rows.stream()).toList();

      assert batches.size() == 1;
      ColumnarBatch batch = batches.get(0);
      try (batch) {
        VectorSchemaRoot root = batch.root();
        assert root.getRowCount() == 2;

        IntVector idVector = (IntVector) root.getVector(0);
        assert idVector.get(0) == 1;
        assert idVector.get(1) == 2;

        VarCharVector labelVector = (VarCharVector) root.getVector(1);
        assert !labelVector.isNull(0);
        assert new String(labelVector.get(0), StandardCharsets.UTF_8).equals("hello");
        assert labelVector.isNull(1);

        VarCharVector tagsVector = (VarCharVector) root.getVector(2);
        assert new String(tagsVector.get(0), StandardCharsets.UTF_8).equals("[a, b]");
        assert tagsVector.isNull(1);
      }
    }
  }
}
