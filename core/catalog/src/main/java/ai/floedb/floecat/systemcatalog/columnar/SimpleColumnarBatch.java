package ai.floedb.floecat.systemcatalog.columnar;

import java.util.Objects;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Simple implementation of {@link ColumnarBatch} that owns a {@link VectorSchemaRoot}. */
public final class SimpleColumnarBatch implements ColumnarBatch {

  private final VectorSchemaRoot root;
  private boolean closed;

  public SimpleColumnarBatch(VectorSchemaRoot root) {
    this.root = Objects.requireNonNull(root, "root");
  }

  @Override
  public VectorSchemaRoot root() {
    return root;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      root.close();
    }
  }
}
