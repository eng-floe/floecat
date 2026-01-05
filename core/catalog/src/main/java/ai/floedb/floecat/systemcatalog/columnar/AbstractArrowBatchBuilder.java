package ai.floedb.floecat.systemcatalog.columnar;

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
