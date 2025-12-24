package ai.floedb.floecat.systemcatalog.columnar;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/** Utilities for converting {@link SchemaColumn} metadata into Arrow schemas. */
public final class ArrowSchemaUtil {

  private ArrowSchemaUtil() {}

  public static Schema toArrowSchema(List<SchemaColumn> columns) {
    Objects.requireNonNull(columns, "columns");
    List<Field> fields = new ArrayList<>(columns.size());
    for (SchemaColumn column : columns) {
      fields.add(toField(column));
    }
    return new Schema(fields);
  }

  private static Field toField(SchemaColumn column) {
    return new Field(column.getName(), fieldType(column), List.of());
  }

  private static FieldType fieldType(SchemaColumn column) {
    ArrowType arrowType = arrowType(column.getLogicalType());
    return new FieldType(column.getNullable(), arrowType, null);
  }

  private static ArrowType arrowType(String logicalType) {
    if (logicalType == null) {
      return new ArrowType.Utf8();
    }
    String normalized = logicalType.toUpperCase(Locale.ROOT).trim();
    if (normalized.endsWith("[]")) {
      return new ArrowType.Utf8();
    }
    return switch (normalized) {
      case "INT", "INTEGER" -> new ArrowType.Int(32, true);
      case "BIGINT" -> new ArrowType.Int(64, true);
      case "SMALLINT" -> new ArrowType.Int(16, true);
      case "FLOAT", "FLOAT4", "REAL" -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case "DOUBLE", "FLOAT8" -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "BOOLEAN", "BOOL" -> ArrowType.Bool.INSTANCE;
      default -> new ArrowType.Utf8();
    };
  }
}
