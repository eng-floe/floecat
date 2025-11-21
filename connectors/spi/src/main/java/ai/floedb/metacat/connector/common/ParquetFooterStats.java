package ai.floedb.metacat.connector.common;

import ai.floedb.metacat.types.LogicalComparators;
import ai.floedb.metacat.types.LogicalType;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
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

public final class ParquetFooterStats {

  public static final class ColAgg {
    public long nulls = 0;
    public Object min = null;
    public Object max = null;

    void mergeMin(Object v, LogicalType lt) {
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
}
