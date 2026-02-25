/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.arrow;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/** Reflection-backed builders for {@link ArrowRecordWriter} over Java record types. */
public final class ArrowRecordWriters {

  private ArrowRecordWriters() {}

  public static <T extends Record> ArrowRecordWriter<T> fromRecordClass(Class<T> recordClass) {
    SchemaAndWriters<T> sw = buildSchemaAndWriters(recordClass);
    return new ArrowRecordWriter<>() {
      @Override
      public Schema schema() {
        return sw.schema();
      }

      @Override
      public void write(VectorSchemaRoot root, List<T> rows) {
        root.allocateNew();
        int i = 0;
        for (T row : rows) {
          for (int c = 0; c < sw.writers().size(); c++) {
            sw.writers().get(c).write(root, i, row);
          }
          i++;
        }
        for (FieldVector v : root.getFieldVectors()) {
          v.setValueCount(rows.size());
        }
        root.setRowCount(rows.size());
      }
    };
  }

  private record SchemaAndWriters<T>(Schema schema, List<ComponentWriter<T>> writers) {}

  private interface ComponentWriter<T> {
    void write(VectorSchemaRoot root, int rowIndex, T row);
  }

  private static <T extends Record> SchemaAndWriters<T> buildSchemaAndWriters(Class<T> rc) {
    if (!rc.isRecord()) {
      throw new IllegalArgumentException("Not a record: " + rc.getName());
    }

    RecordComponent[] comps = rc.getRecordComponents();
    List<Field> fields = new ArrayList<>(comps.length);
    List<ComponentWriter<T>> writers = new ArrayList<>(comps.length);

    MethodHandles.Lookup lookup = MethodHandles.lookup();

    for (RecordComponent comp : comps) {
      String name =
          Optional.ofNullable(comp.getAnnotation(ArrowFieldName.class))
              .map(ArrowFieldName::value)
              .orElse(toSnakeCase(comp.getName()));
      Class<?> jt = comp.getType();

      Field field;
      ComponentWriter<T> writer;

      MethodHandle getter;
      try {
        getter = lookup.findVirtual(rc, comp.getAccessor().getName(), MethodType.methodType(jt));
      } catch (NoSuchMethodException | IllegalAccessException e) {
        throw new RuntimeException("Failed to bind accessor for " + name, e);
      }

      if (jt == String.class) {
        field = field(name, new ArrowType.Utf8());
        writer =
            (root, i, row) -> {
              VarCharVector v = (VarCharVector) root.getVector(name);
              String s = get(getter, row, String.class);
              if (s == null) {
                v.setNull(i);
              } else {
                byte[] b = s.getBytes(StandardCharsets.UTF_8);
                v.setSafe(i, b, 0, b.length);
              }
            };
      } else if (jt == byte[].class) {
        field = field(name, new ArrowType.Binary());
        writer =
            (root, i, row) -> {
              VarBinaryVector v = (VarBinaryVector) root.getVector(name);
              byte[] b = get(getter, row, byte[].class);
              if (b == null) {
                v.setNull(i);
              } else {
                v.setSafe(i, b, 0, b.length);
              }
            };
      } else if (jt == boolean.class || jt == Boolean.class) {
        field = field(name, new ArrowType.Bool());
        writer =
            (root, i, row) -> {
              BitVector v = (BitVector) root.getVector(name);
              Boolean val = getBoxed(getter, row, Boolean.class);
              if (val == null) {
                v.setNull(i);
              } else {
                v.setSafe(i, val ? 1 : 0);
              }
            };
      } else if (jt == int.class || jt == Integer.class) {
        field = field(name, new ArrowType.Int(32, true));
        writer =
            (root, i, row) -> {
              IntVector v = (IntVector) root.getVector(name);
              Integer val = getBoxed(getter, row, Integer.class);
              if (val == null) {
                v.setNull(i);
              } else {
                v.setSafe(i, val);
              }
            };
      } else if (jt == long.class || jt == Long.class) {
        field = field(name, new ArrowType.Int(64, true));
        writer =
            (root, i, row) -> {
              BigIntVector v = (BigIntVector) root.getVector(name);
              Long val = getBoxed(getter, row, Long.class);
              if (val == null) {
                v.setNull(i);
              } else {
                v.setSafe(i, val);
              }
            };
      } else if (jt == double.class || jt == Double.class) {
        field = field(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        writer =
            (root, i, row) -> {
              Float8Vector v = (Float8Vector) root.getVector(name);
              Double val = getBoxed(getter, row, Double.class);
              if (val == null) {
                v.setNull(i);
              } else {
                v.setSafe(i, val);
              }
            };
      } else if (jt == Instant.class) {
        field =
            new Field(
                name,
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                null);
        writer =
            (root, i, row) -> {
              TimeStampMicroTZVector v = (TimeStampMicroTZVector) root.getVector(name);
              Instant inst = get(getter, row, Instant.class);
              if (inst == null) {
                v.setNull(i);
              } else {
                long micros = inst.getEpochSecond() * 1_000_000L + inst.getNano() / 1_000L;
                v.setSafe(i, micros);
              }
            };
      } else if (jt == LocalDate.class) {
        field = new Field(name, FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
        writer =
            (root, i, row) -> {
              DateDayVector v = (DateDayVector) root.getVector(name);
              LocalDate d = get(getter, row, LocalDate.class);
              if (d == null) {
                v.setNull(i);
              } else {
                v.setSafe(i, (int) d.toEpochDay());
              }
            };
      } else if (jt == UUID.class) {
        field = new Field(name, FieldType.nullable(new ArrowType.FixedSizeBinary(16)), null);
        writer =
            (root, i, row) -> {
              FixedSizeBinaryVector v = (FixedSizeBinaryVector) root.getVector(name);
              UUID u = get(getter, row, UUID.class);
              if (u == null) {
                v.setNull(i);
              } else {
                ByteBuffer bb = ByteBuffer.allocate(16);
                bb.putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits());
                v.setSafe(i, bb.array());
              }
            };
      } else {
        throw new IllegalArgumentException(
            "Unsupported component type for " + name + ": " + jt.getName());
      }

      fields.add(field);
      writers.add(writer);
    }

    return new SchemaAndWriters<>(new Schema(fields), writers);
  }

  private static Field field(String name, ArrowType type) {
    return new Field(name, FieldType.nullable(type), null);
  }

  @SuppressWarnings("unchecked")
  private static <R> R get(MethodHandle mh, Object target, Class<R> cls) {
    try {
      return (R) mh.invoke(target);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private static <R> R getBoxed(MethodHandle mh, Object target, Class<R> cls) {
    try {
      Object v = mh.invoke(target);
      return (v == null) ? null : cls.cast(v);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private static String toSnakeCase(String name) {
    return name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
  }
}
