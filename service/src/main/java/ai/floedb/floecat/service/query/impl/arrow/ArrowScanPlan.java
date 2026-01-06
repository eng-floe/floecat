package ai.floedb.floecat.service.query.impl.arrow;

import ai.floedb.floecat.systemcatalog.columnar.ColumnarBatch;
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Simple holder for the Arrow schema and the lazily produced batch stream.
 *
 * <p>Callers must close the plan to release the underlying stream resources.
 */
public final class ArrowScanPlan implements AutoCloseable {
  private final Schema schema;
  private final Stream<ColumnarBatch> batches;

  ArrowScanPlan(Schema schema, Stream<ColumnarBatch> batches) {
    this.schema = schema;
    this.batches = batches;
  }

  public Schema schema() {
    return schema;
  }

  public Iterator<ColumnarBatch> iterator() {
    return batches.iterator();
  }

  @Override
  public void close() {
    batches.close();
  }
}
