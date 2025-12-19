package ai.floedb.floecat.systemcatalog.columnar;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.util.ArrowConversion;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Adapts a row-wise {@link SystemObjectRow} stream into Arrow-native {@link ColumnarBatch} chunks.
 */
public final class RowStreamToArrowBatchAdapter {

  private final BufferAllocator allocator;
  private final Schema arrowSchema;
  private final boolean[] stringifyArrays;
  private final int batchSize;

  public RowStreamToArrowBatchAdapter(
      BufferAllocator allocator, List<SchemaColumn> schema, int batchSize) {
    this.allocator = Objects.requireNonNull(allocator, "allocator");
    Objects.requireNonNull(schema, "schema");
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    this.batchSize = batchSize;
    List<Field> fields = new ArrayList<>(schema.size());
    this.stringifyArrays = new boolean[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      SchemaColumn column = schema.get(i);
      fields.add(new Field(column.getName(), fieldType(column), List.of()));
      stringifyArrays[i] = isArrayType(column.getLogicalType());
    }
    this.arrowSchema = new Schema(fields);
  }

  public Stream<ColumnarBatch> adapt(Stream<SystemObjectRow> rows) {
    Objects.requireNonNull(rows, "rows");
    Iterator<SystemObjectRow> iterator = rows.iterator();
    Spliterator<ColumnarBatch> spliterator =
        new Spliterators.AbstractSpliterator<ColumnarBatch>(
            Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL) {
          @Override
          public boolean tryAdvance(Consumer<? super ColumnarBatch> action) {
            List<SystemObjectRow> batch = new ArrayList<>(batchSize);
            while (iterator.hasNext() && batch.size() < batchSize) {
              batch.add(normalize(iterator.next()));
            }
            if (batch.isEmpty()) {
              return false;
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
            ArrowConversion.fill(root, batch);
            action.accept(new SimpleColumnarBatch(root));
            return true;
          }
        };
    return StreamSupport.stream(spliterator, false);
  }

  private SystemObjectRow normalize(SystemObjectRow row) {
    Object[] values = row.values();
    Object[] normalized = Arrays.copyOf(values, values.length);
    for (int i = 0; i < values.length && i < stringifyArrays.length; i++) {
      if (stringifyArrays[i]) {
        normalized[i] = stringify(values[i]);
      }
    }
    return new SystemObjectRow(normalized);
  }

  private static Object stringify(Object value) {
    if (value == null) {
      return null;
    }
    Class<?> type = value.getClass();
    if (!type.isArray()) {
      return value;
    }
    if (type == String[].class) {
      return Arrays.toString((String[]) value);
    }
    if (type == int[].class) {
      return Arrays.toString((int[]) value);
    }
    if (type == long[].class) {
      return Arrays.toString((long[]) value);
    }
    if (type == short[].class) {
      return Arrays.toString((short[]) value);
    }
    if (type == byte[].class) {
      return Arrays.toString((byte[]) value);
    }
    if (type == boolean[].class) {
      return Arrays.toString((boolean[]) value);
    }
    if (type == float[].class) {
      return Arrays.toString((float[]) value);
    }
    if (type == double[].class) {
      return Arrays.toString((double[]) value);
    }
    return Arrays.deepToString((Object[]) value);
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

  private static boolean isArrayType(String logicalType) {
    if (logicalType == null) {
      return false;
    }
    return logicalType.contains("[]");
  }
}
