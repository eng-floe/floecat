package ai.floedb.floecat.systemcatalog.columnar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;

/** Columnar projection operator that reuses existing vectors via {@link TransferPair}s. */
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

    List<FieldVector> selected = new ArrayList<>();
    for (String column : requiredColumns) {
      if (column == null) {
        continue;
      }
      FieldVector vector = vectorsByName.get(column.toLowerCase(Locale.ROOT));
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

    batch.close();

    VectorSchemaRoot projectedRoot = new VectorSchemaRoot(selected);
    projectedRoot.setRowCount(root.getRowCount());
    return new SimpleColumnarBatch(projectedRoot);
  }
}
