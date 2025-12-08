package ai.floedb.floecat.catalog.system_objects.util;

import ai.floedb.floecat.catalog.system_objects.spi.SystemObjectRow;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Helper for converting Object[] rows into Arrow vectors.
 *
 * <p>Engine plugins may implement Arrow-native scanners without using this helper.
 */
public final class ArrowConversion {

  private ArrowConversion() {}

  public static VectorSchemaRoot fill(VectorSchemaRoot root, Iterable<SystemObjectRow> rows) {
    // TODO: efficient vector writers (no boxing, reuse writers)
    int rowIndex = 0;
    for (SystemObjectRow row : rows) {
      // TODO: implement fast-path conversion
      rowIndex++;
    }
    root.setRowCount(rowIndex);
    return root;
  }
}
