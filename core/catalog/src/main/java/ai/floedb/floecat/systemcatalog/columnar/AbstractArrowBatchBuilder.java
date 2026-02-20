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

import ai.floedb.floecat.arrow.ColumnarBatch;
import ai.floedb.floecat.arrow.SimpleColumnarBatch;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/** Base helper for the scanner batch builders. */
public abstract class AbstractArrowBatchBuilder {

  private final VectorSchemaRoot root;
  private int rowCount;
  private boolean consumed;

  protected AbstractArrowBatchBuilder(Schema schema, BufferAllocator allocator) {
    this.root = VectorSchemaRoot.create(schema, allocator);
    this.root.allocateNew();
  }

  protected VectorSchemaRoot root() {
    return root;
  }

  protected List<FieldVector> vectors() {
    return root.getFieldVectors();
  }

  protected int rowCount() {
    return rowCount;
  }

  protected void incrementRow() {
    rowCount++;
  }

  public ColumnarBatch buildBatch() {
    setValueCounts();
    root.setRowCount(rowCount);
    consumed = true;
    return new SimpleColumnarBatch(root);
  }

  public void release() {
    if (!consumed) {
      consumed = true;
      root.close();
    }
  }

  public boolean isEmpty() {
    return rowCount == 0;
  }

  protected void setValueCounts() {
    for (FieldVector vector : root.getFieldVectors()) {
      vector.setValueCount(rowCount);
    }
  }
}
