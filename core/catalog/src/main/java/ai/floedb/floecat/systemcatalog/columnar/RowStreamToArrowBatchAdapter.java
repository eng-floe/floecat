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

package ai.floedb.floecat.systemcatalog.columnar;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.util.ArrowConversion;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Adapts a row-wise {@link SystemObjectRow} stream into Arrow-native {@link ColumnarBatch} chunks.
 */
public final class RowStreamToArrowBatchAdapter {

  private final BufferAllocator allocator;
  private final Schema arrowSchema;
  private final boolean[] stringifyArrays;
  private final int batchSize;
  private final boolean hasArrayColumns;

  public RowStreamToArrowBatchAdapter(
      BufferAllocator allocator, List<SchemaColumn> schema, int batchSize) {
    this.allocator = Objects.requireNonNull(allocator, "allocator");
    Objects.requireNonNull(schema, "schema");
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    this.batchSize = batchSize;
    boolean hasArray = false;
    this.stringifyArrays = new boolean[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      boolean arrayType = isArrayType(schema.get(i).getLogicalType());
      stringifyArrays[i] = arrayType;
      if (arrayType) {
        hasArray = true;
      }
    }
    this.hasArrayColumns = hasArray;
    this.arrowSchema = ArrowSchemaUtil.toArrowSchema(schema);
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
            try {
              root.allocateNew();
              ArrowConversion.fill(root, batch);
              action.accept(new SimpleColumnarBatch(root));
              return true;
            } catch (Exception e) {
              root.close();
              throw new IllegalStateException("Failed to adapt row stream to Arrow batch", e);
            }
          }
        };
    return StreamSupport.stream(spliterator, false).onClose(rows::close);
  }

  private SystemObjectRow normalize(SystemObjectRow row) {
    if (!hasArrayColumns) {
      return row;
    }
    Object[] values = row.values();
    boolean needsCopy = false;
    for (int i = 0; i < values.length && i < stringifyArrays.length; i++) {
      if (stringifyArrays[i] && isArray(values[i])) {
        needsCopy = true;
        break;
      }
    }
    if (!needsCopy) {
      return row;
    }
    Object[] normalized = Arrays.copyOf(values, values.length);
    for (int i = 0; i < normalized.length && i < stringifyArrays.length; i++) {
      if (stringifyArrays[i]) {
        normalized[i] = stringify(normalized[i]);
      }
    }
    return new SystemObjectRow(normalized);
  }

  private static boolean isArray(Object value) {
    return value != null && value.getClass().isArray();
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

  private static boolean isArrayType(String logicalType) {
    if (logicalType == null) {
      return false;
    }
    return logicalType.contains("[]");
  }
}
