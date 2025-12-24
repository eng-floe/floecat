package ai.floedb.floecat.systemcatalog.columnar;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.arrow.vector.VarCharVector;

/** Lightweight helpers for writing values into Arrow vectors. */
public final class ArrowValueWriters {

  private ArrowValueWriters() {}

  public static void writeVarChar(VarCharVector vector, int index, String value) {
    Objects.requireNonNull(vector, "vector");
    if (value == null) {
      vector.setNull(index);
      return;
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    vector.setSafe(index, bytes);
  }
}
