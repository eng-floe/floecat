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

package ai.floedb.floecat.scanner.columnar;

import ai.floedb.floecat.arrow.ColumnarBatch;
import ai.floedb.floecat.arrow.RequiredColumns;
import ai.floedb.floecat.arrow.SimpleColumnarBatch;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Columnar projection operator that reuses existing vectors via {@link TransferPair}s.
 *
 * <p>Column names follow {@link RequiredColumns}. Names the batch does not have are dropped, so a
 * request naming only unknown columns yields a zero-column batch that keeps the row count.
 *
 * <p>When projection is executed (at least one non-blank name requested), the input batch is closed
 * and the projected batch owns a new {@link VectorSchemaRoot}. If no columns are requested the
 * original batch is returned untouched.
 */
public final class ArrowProjectOperator {

  private ArrowProjectOperator() {}

  public static ColumnarBatch project(
      ColumnarBatch batch, List<String> requiredColumns, BufferAllocator allocator) {
    List<String> requested = RequiredColumns.normalize(requiredColumns);
    if (requested.isEmpty()) {
      return batch;
    }

    VectorSchemaRoot root = batch.root();
    Map<String, FieldVector> vectorsByName = new HashMap<>();
    for (FieldVector vector : root.getFieldVectors()) {
      vectorsByName.putIfAbsent(RequiredColumns.key(vector.getField().getName()), vector);
    }

    int rowCount = root.getRowCount();
    List<FieldVector> selected = new ArrayList<>();
    for (String column : requested) {
      FieldVector vector = vectorsByName.get(column);
      if (vector == null) {
        continue;
      }
      TransferPair transfer = vector.getTransferPair(allocator);
      transfer.transfer();
      FieldVector target = (FieldVector) transfer.getTo();
      target.setValueCount(rowCount);
      selected.add(target);
    }

    batch.close();

    VectorSchemaRoot projectedRoot = new VectorSchemaRoot(selected);
    projectedRoot.setRowCount(rowCount);
    return new SimpleColumnarBatch(projectedRoot);
  }
}
