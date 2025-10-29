package ai.floedb.metacat.connector.common;

import ai.floedb.metacat.connector.common.ndv.DataSketchesHll;
import ai.floedb.metacat.connector.common.ndv.Hll;
import ai.floedb.metacat.connector.common.ndv.NdvProvider;
import ai.floedb.metacat.types.LogicalComparators;
import ai.floedb.metacat.types.LogicalType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

public final class GenericStatsEngine<K> implements StatsEngine<K> {

  private final Planner<K> planner;
  private final NdvProvider ndvProvider;
  private final Map<K, String> columnNames;
  private final Map<K, LogicalType> logicalTypes;

  public GenericStatsEngine(
      Planner<K> planner,
      NdvProvider ndvProvider,
      Map<K, String> columnNames,
      Map<K, LogicalType> logicalTypes) {
    this.planner = Objects.requireNonNull(planner, "planner");
    this.ndvProvider = ndvProvider;
    this.columnNames = columnNames == null ? Map.of() : Map.copyOf(columnNames);
    this.logicalTypes = logicalTypes == null ? Map.of() : Map.copyOf(logicalTypes);
  }

  public GenericStatsEngine(Planner<K> planner) {
    this(planner, null, Map.of(), Map.of());
  }

  @Override
  public Optional<String> columnNameFor(K colKey) {
    return Optional.ofNullable(columnNames.get(colKey));
  }

  @Override
  public Optional<LogicalType> logicalTypeFor(K colKey) {
    return Optional.ofNullable(logicalTypes.get(colKey));
  }

  @Override
  public Result<K> compute() {
    final Acc<K> accumulator = new Acc<>();

    for (K column : planner.columns()) {
      final var logicalType = logicalTypeFor(column).orElse(null);
      final var name = columnNameFor(column).orElse(null);
      accumulator.columns.put(column, new ColAcc(logicalType, name));
    }

    long fileCount = 0;
    long totalRows = 0;
    long totalBytes = 0;

    final Map<K, Hll> ndvByKey = new LinkedHashMap<>();
    final Map<String, Hll> ndvByName = new LinkedHashMap<>();
    if (ndvProvider != null) {
      for (var column : accumulator.columns.entrySet()) {
        final K key = column.getKey();
        final ColAcc columnAgg = column.getValue();
        if (columnAgg.name == null || columnAgg.name.isBlank()) {
          continue;
        }

        final Hll sink = hllFactoryCreate();
        ndvByKey.put(key, sink);
        ndvByName.put(columnAgg.name, sink);
      }
    }

    for (PlannedFile<K> file : planner) {
      fileCount++;
      totalRows += file.rowCount();
      totalBytes += file.sizeBytes();

      mergeCounts(accumulator.columns, file.valueCounts(), ColAcc::addValueCount);
      mergeCounts(accumulator.columns, file.nullCounts(), ColAcc::addNullCount);
      mergeCounts(accumulator.columns, file.nanCounts(), ColAcc::addNanCount);

      mergeBounds(accumulator.columns, file.lowerBounds(), true);
      mergeBounds(accumulator.columns, file.upperBounds(), false);

      if (ndvProvider != null && !ndvByName.isEmpty()) {
        try {
          ndvProvider.contributeNdv(file.path(), ndvByName);
        } catch (Exception ndve) {
          // ignore
        }
      }
    }

    if (ndvProvider != null) {
      for (var e : accumulator.columns.entrySet()) {
        final K key = e.getKey();
        final ColAcc ca = e.getValue();
        final Hll sink = ndvByKey.get(key);
        if (sink != null) {
          ca.ndvHll = toBytesSafe(sink);
        }
      }
    }

    final Map<K, ColumnAgg> outCols = new LinkedHashMap<>(accumulator.columns.size());
    for (var column : accumulator.columns.entrySet())
      outCols.put(column.getKey(), column.getValue().freeze());

    return new ResultImpl<>(totalRows, totalBytes, fileCount, Collections.unmodifiableMap(outCols));
  }

  private static final class Acc<K> {
    final Map<K, ColAcc> columns = new LinkedHashMap<>();
  }

  private static final class ColAcc {
    final LogicalType type;
    final String name;

    Long ndvExact;
    byte[] ndvHll;
    Map<String, String> ndvParams;

    Long valueCount;
    Long nullCount;
    Long nanCount;

    Object min;
    Object max;

    ColAcc(LogicalType type, String name) {
      this.type = type;
      this.name = name;
    }

    void addValueCount(long v) {
      valueCount = sum(valueCount, v);
    }

    void addNullCount(long v) {
      nullCount = sum(nullCount, v);
    }

    void addNanCount(long v) {
      nanCount = sum(nanCount, v);
    }

    void minWith(Object v) {
      if (v == null) {
        return;
      }

      if (min == null) {
        min = v;
        return;
      }

      if (type == null) {
        if (v instanceof Comparable<?> c && min.getClass().isInstance(v)) {
          @SuppressWarnings("unchecked")
          int cmp = ((Comparable<Object>) c).compareTo(min);
          if (cmp < 0) min = v;
        }
      } else if (LogicalComparators.compare(type, v, min) < 0) {
        min = v;
      }
    }

    void maxWith(Object v) {
      if (v == null) {
        return;
      }

      if (max == null) {
        max = v;
        return;
      }

      if (type == null) {
        if (v instanceof Comparable<?> c && max.getClass().isInstance(v)) {
          @SuppressWarnings("unchecked")
          int cmp = ((Comparable<Object>) c).compareTo(max);
          if (cmp > 0) max = v;
        }
      } else if (LogicalComparators.compare(type, v, max) > 0) {
        max = v;
      }
    }

    private static Long sum(Long a, long b) {
      return a == null ? b : a + b;
    }

    ColumnAgg freeze() {
      final Long fExact = ndvExact;
      final byte[] fHll = ndvHll;
      final Map<String, String> fParams = ndvParams == null ? null : Map.copyOf(ndvParams);
      final Long fVal = valueCount;
      final Long fNull = nullCount;
      final Long fNan = nanCount;
      final Object fMin = min;
      final Object fMax = max;

      return new ColumnAgg() {
        @Override
        public Long ndvExact() {
          return fExact;
        }

        @Override
        public byte[] ndvHll() {
          return fHll;
        }

        @Override
        public Long valueCount() {
          return fVal;
        }

        @Override
        public Long nullCount() {
          return fNull;
        }

        @Override
        public Long nanCount() {
          return fNan;
        }

        @Override
        public Object min() {
          return fMin;
        }

        @Override
        public Object max() {
          return fMax;
        }
      };
    }
  }

  private static final class ResultImpl<K> implements Result<K> {
    private final long rows;
    private final long bytes;
    private final long files;
    private final Map<K, ColumnAgg> cols;

    ResultImpl(long rows, long bytes, long files, Map<K, ColumnAgg> cols) {
      this.rows = rows;
      this.bytes = bytes;
      this.files = files;
      this.cols = cols;
    }

    @Override
    public long totalRowCount() {
      return rows;
    }

    @Override
    public long totalSizeBytes() {
      return bytes;
    }

    @Override
    public long fileCount() {
      return files;
    }

    @Override
    public Map<K, ColumnAgg> columns() {
      return cols;
    }
  }

  private static <K> void mergeCounts(
      Map<K, ColAcc> columnAggs, Map<K, Long> files, BiConsumer<ColAcc, Long> apply) {
    if (files == null || files.isEmpty()) {
      return;
    }

    for (var file : files.entrySet()) {
      final ColAcc columnAgg = columnAggs.get(file.getKey());
      final Long v = file.getValue();
      if (columnAgg != null && v != null) {
        apply.accept(columnAgg, v);
      }
    }
  }

  private static <K> void mergeBounds(
      Map<K, ColAcc> columnAggs, Map<K, Object> files, boolean isLower) {
    if (files == null || files.isEmpty()) {
      return;
    }

    for (var file : files.entrySet()) {
      final ColAcc columnAgg = columnAggs.get(file.getKey());
      if (columnAgg == null) continue;
      if (isLower) {
        columnAgg.minWith(file.getValue());
      } else {
        columnAgg.maxWith(file.getValue());
      }
    }
  }

  private static Hll hllFactoryCreate() {
    // return new HllImpl(14);
    return new DataSketchesHll();
  }

  private static byte[] toBytesSafe(Hll h) {
    try {
      return h == null ? null : h.toBytes();
    } catch (Throwable ignore) {
      return null;
    }
  }
}
