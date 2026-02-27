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

package ai.floedb.floecat.connector.common;

import ai.floedb.floecat.types.LogicalComparators;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.TemporalCoercions;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;

public final class ParquetFooterStats {

  public static final class ColAgg {
    public long nulls = 0;
    public Object min = null;
    public Object max = null;

    void mergeMin(Object v, LogicalType lt) {
      if (lt != null && !LogicalComparators.isOrderable(lt)) {
        return;
      }
      if (v == null) {
        return;
      }
      if (min == null) {
        min = v;
        return;
      }

      if (lt != null) {
        if (LogicalComparators.compare(lt, v, min) < 0) {
          min = v;
        }
      } else {
        @SuppressWarnings({"rawtypes", "unchecked"})
        int cmp = ((Comparable) v).compareTo(min);
        if (cmp < 0) {
          min = v;
        }
      }
    }

    void mergeMax(Object v, LogicalType lt) {
      if (lt != null && !LogicalComparators.isOrderable(lt)) {
        return;
      }
      if (v == null) {
        return;
      }
      if (max == null) {
        max = v;
        return;
      }
      if (lt != null) {
        if (LogicalComparators.compare(lt, v, max) > 0) {
          max = v;
        }
      } else {
        @SuppressWarnings({"rawtypes", "unchecked"})
        int cmp = ((Comparable) v).compareTo(max);
        if (cmp > 0) {
          max = v;
        }
      }
    }
  }

  public static final class BasicFileStats {
    public final long rowCount;
    public final Map<String, ColAgg> cols;

    BasicFileStats(long rowCount, Map<String, ColAgg> cols) {
      this.rowCount = rowCount;
      this.cols = cols;
    }
  }

  public static BasicFileStats read(
      InputFile in, Set<String> includeNames, Map<String, LogicalType> nameToLogical) {

    try (ParquetFileReader r = ParquetFileReader.open(new InputFileAdapter(in))) {
      List<BlockMetaData> rowGroups = r.getFooter().getBlocks();
      long totalRows = 0L;
      Map<String, ColAgg> agg = new LinkedHashMap<>();

      for (BlockMetaData rg : rowGroups) {
        totalRows += rg.getRowCount();

        for (ColumnChunkMetaData c : rg.getColumns()) {
          String colName = String.join(".", c.getPath().toArray());
          if (!includeNames.isEmpty() && !includeNames.contains(colName)) {
            continue;
          }

          Statistics<?> s = c.getStatistics();
          if (s == null) {
            continue;
          }

          LogicalType lt = nameToLogical.get(colName);
          ColAgg a = agg.computeIfAbsent(colName, k -> new ColAgg());

          long numNulls = s.getNumNulls();
          a.nulls += (numNulls < 0 ? 0 : numNulls);

          Object minV = statToValue(s, c, lt, true);
          a.mergeMin(minV, lt);

          Object maxV = statToValue(s, c, lt, false);
          a.mergeMax(maxV, lt);
        }
      }

      return new BasicFileStats(totalRows, agg);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read parquet footer stats", e);
    }
  }

  private static final class InputFileAdapter implements org.apache.parquet.io.InputFile {
    private final InputFile delegate;

    InputFileAdapter(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() throws IOException {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      return delegate.newStream();
    }
  }

  private static Object statToValue(
      Statistics<?> s, ColumnChunkMetaData c, LogicalType logical, boolean isMin) {

    var lta = c.getPrimitiveType().getLogicalTypeAnnotation();

    Object v = isMin ? s.genericGetMin() : s.genericGetMax();
    if (v == null) {
      return null;
    }

    if (logical != null) {
      if (logical.kind() == LogicalKind.TIME) {
        return timeStatValue(lta, v);
      }
      if (logical.kind() == LogicalKind.TIMESTAMP) {
        return timestampStatValue(lta, v, false);
      }
      if (logical.kind() == LogicalKind.TIMESTAMPTZ) {
        return timestampStatValue(lta, v, true);
      }
    }

    if (lta instanceof StringLogicalTypeAnnotation) {
      if (v instanceof Binary b) {
        return b.toStringUsingUTF8();
      }
      if (v instanceof CharSequence cs) {
        return cs.toString();
      }
    }

    if (lta instanceof DecimalLogicalTypeAnnotation dec) {
      int scale = dec.getScale();

      if (v instanceof BigDecimal bd) {
        return bd.setScale(scale);
      }
      if (v instanceof Number n) {
        return new BigDecimal(BigInteger.valueOf(n.longValue()), scale);
      }
      if (v instanceof Binary b) {
        byte[] bytes = b.getBytes();
        BigInteger unscaled = new BigInteger(bytes);
        return new BigDecimal(unscaled, scale);
      }
      if (v instanceof CharSequence cs) {
        return new BigDecimal(cs.toString()).setScale(scale);
      }
    }

    if (lta instanceof DateLogicalTypeAnnotation) {
      if (v instanceof Integer i) {
        return LocalDate.ofEpochDay(i.longValue());
      }
      if (v instanceof Number n) {
        return LocalDate.ofEpochDay(n.longValue());
      }
    }

    if (v instanceof Binary b) {
      return b.getBytes();
    }

    return v;
  }

  private static Object timeStatValue(Object lta, Object v) {
    if (!(lta instanceof TimeLogicalTypeAnnotation timeAnno)) {
      throw new IllegalArgumentException("TIME stats require Parquet TIME logical annotation");
    }
    if (!(v instanceof Number n)) {
      throw new IllegalArgumentException("TIME stats must be numeric, got " + v.getClass());
    }
    long raw = n.longValue();
    long nanos = toTimeNanos(raw, timeAnno.getUnit());
    if (nanos < 0 || nanos >= TemporalCoercions.NANOS_PER_DAY) {
      // Invalid time-of-day; drop stats rather than wrapping to a different value.
      return null;
    }
    return LocalTime.ofNanoOfDay(nanos);
  }

  private static Object timestampStatValue(Object lta, Object v, boolean adjustedToUtc) {
    if (!(lta instanceof TimestampLogicalTypeAnnotation tsAnno)) {
      throw new IllegalArgumentException(
          "TIMESTAMP stats require Parquet TIMESTAMP logical annotation");
    }
    if (tsAnno.isAdjustedToUTC() != adjustedToUtc) {
      throw new IllegalArgumentException(
          "TIMESTAMP stats annotation mismatch: adjustedToUTC="
              + tsAnno.isAdjustedToUTC()
              + " expected="
              + adjustedToUtc);
    }
    if (!(v instanceof Number n)) {
      throw new IllegalArgumentException("TIMESTAMP stats must be numeric, got " + v.getClass());
    }
    Instant instant = instantFromUnit(n.longValue(), tsAnno.getUnit());
    if (adjustedToUtc) {
      return instant;
    }
    // Parquet TIMESTAMP without UTC normalization is encoded as epoch-based counts.
    // We interpret those counts as UTC wall-clock when constructing LocalDateTime.
    return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
  }

  private static long toTimeNanos(long value, TimeUnit unit) {
    return switch (unit) {
      case MILLIS -> value * 1_000_000L;
      case MICROS -> value * 1_000L;
      case NANOS -> value;
    };
  }

  private static Instant instantFromUnit(long value, TimeUnit unit) {
    return switch (unit) {
      case MILLIS -> Instant.ofEpochMilli(value);
      case MICROS -> {
        long secs = Math.floorDiv(value, 1_000_000L);
        long micros = Math.floorMod(value, 1_000_000L);
        yield Instant.ofEpochSecond(secs, micros * 1_000L);
      }
      case NANOS -> {
        long secs = Math.floorDiv(value, 1_000_000_000L);
        long nanos = Math.floorMod(value, 1_000_000_000L);
        yield Instant.ofEpochSecond(secs, nanos);
      }
    };
  }
}
