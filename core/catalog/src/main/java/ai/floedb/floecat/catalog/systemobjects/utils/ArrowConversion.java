package ai.floedb.floecat.catalog.systemobjects.utils;

import ai.floedb.floecat.catalog.systemobjects.spi.SystemObjectRow;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Helper for converting Object[] rows into Arrow vectors.
 *
 * <p>Engine plugins may implement Arrow-native scanners without using this helper.
 */
public final class ArrowConversion {

  private ArrowConversion() {}

  public static VectorSchemaRoot fill(VectorSchemaRoot root, Iterable<SystemObjectRow> rows) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(rows, "rows");

    var vectors = root.getFieldVectors();
    int columnCount = vectors.size();
    int rowIndex = 0;

    for (SystemObjectRow row : rows) {
      Object[] values = row.values();
      if (values.length != columnCount) {
        throw new IllegalArgumentException(
            "row column count mismatch: expected=" + columnCount + " actual=" + values.length);
      }
      for (int column = 0; column < columnCount; column++) {
        writeValue(vectors.get(column), rowIndex, values[column]);
      }
      rowIndex++;
    }

    for (FieldVector vector : vectors) {
      vector.setValueCount(rowIndex);
    }
    root.setRowCount(rowIndex);
    return root;
  }

  private static void writeValue(FieldVector vector, int idx, Object value) {
    if (value == null) {
      vector.setNull(idx);
      return;
    }
    if (vector instanceof VarCharVector varChar) {
      byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
      varChar.setSafe(idx, bytes);
      return;
    }
    if (vector instanceof IntVector intVec) {
      intVec.setSafe(idx, ((Number) value).intValue());
      return;
    }
    if (vector instanceof BigIntVector bigInt) {
      bigInt.setSafe(idx, ((Number) value).longValue());
      return;
    }
    if (vector instanceof Float4Vector float4) {
      float4.setSafe(idx, ((Number) value).floatValue());
      return;
    }
    if (vector instanceof Float8Vector float8) {
      float8.setSafe(idx, ((Number) value).doubleValue());
      return;
    }
    if (vector instanceof BitVector bit) {
      bit.setSafe(idx, (((Boolean) value) ? 1 : 0));
      return;
    }
    throw new IllegalArgumentException(
        "unsupported vector type " + vector.getClass().getSimpleName());
  }
}
