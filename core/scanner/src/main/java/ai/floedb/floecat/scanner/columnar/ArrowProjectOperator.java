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
import ai.floedb.floecat.arrow.SimpleColumnarBatch;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Columnar projection operator that reuses existing vectors via {@link TransferPair}s.
 *
 * <p>Unknown column names are silently ignored and duplicate requests are collapsed so each column
 * appears at most once in its first requested position.
 *
 * <p>When projection is executed (non-empty required list), the input batch is closed and the
 * projected batch owns a new {@link VectorSchemaRoot}. If no columns are requested the original
 * batch is returned untouched.
 */
public final class ArrowProjectOperator {

  private ArrowProjectOperator() {}

  public static ColumnarBatch project(
      ColumnarBatch batch, List<String> requiredColumns, BufferAllocator allocator) {
    if (requiredColumns.isEmpty()) {
      return batch;
    }

    VectorSchemaRoot root = batch.root();
    Map<String, FieldVector> vectorsByName = new HashMap<>();
    for (FieldVector vector : root.getFieldVectors()) {
      vectorsByName.put(vector.getField().getName().toLowerCase(Locale.ROOT), vector);
    }

    List<String> normalizedOrder = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    for (String column : requiredColumns) {
      if (column == null) {
        continue;
      }
      String normalized = column.trim().toLowerCase(Locale.ROOT);
      if (normalized.isEmpty()) {
        continue;
      }
      if (seen.add(normalized)) {
        normalizedOrder.add(normalized);
      }
    }

    List<FieldVector> selected = new ArrayList<>();
    for (String column : normalizedOrder) {
      FieldVector vector = vectorsByName.get(column);
      if (vector == null) {
        continue;
      }
      TransferPair transfer = vector.getTransferPair(allocator);
      transfer.transfer();
      FieldVector target = (FieldVector) transfer.getTo();
      target.setValueCount(root.getRowCount());
      selected.add(target);
    }

    if (selected.isEmpty()) {
      return batch;
    }

    int rowCount = root.getRowCount();
    batch.close();

    VectorSchemaRoot projectedRoot = new VectorSchemaRoot(selected);
    projectedRoot.setRowCount(rowCount);
    return new SimpleColumnarBatch(projectedRoot);
  }
}
