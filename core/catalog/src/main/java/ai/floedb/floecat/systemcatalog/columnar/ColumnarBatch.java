package ai.floedb.floecat.systemcatalog.columnar;

import org.apache.arrow.vector.VectorSchemaRoot;

/** Represents a reference-counted batch of columnar data produced by a system scanner. */
public interface ColumnarBatch extends AutoCloseable {

  /** Returns the Arrow schema + vectors backing this batch. */
  VectorSchemaRoot root();

  /** Releases the underlying Arrow vectors. */
  @Override
  void close();
}
