package ai.floedb.floecat.connector.common;

import ai.floedb.floecat.connector.common.ndv.ColumnNdv;
import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.types.LogicalCoercions;
import ai.floedb.floecat.types.LogicalComparators;
import ai.floedb.floecat.types.LogicalType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

public final class GenericStatsEngine<K> implements StatsEngine<K> {

  private final Planner<K> planner;
  private final NdvProvider ndvProvider;
  private final NdvProvider bootstrapNdv;
  private final Map<K, String> columnNames;
  private final Map<K, LogicalType> logicalTypes;

  public GenericStatsEngine(
      Planner<K> planner,
      NdvProvider ndvProvider,
      NdvProvider bootstrapNdv,
      Map<K, String> columnNames,
      Map<K, LogicalType> logicalTypes) {
    this.planner = Objects.requireNonNull(planner);
    this.ndvProvider = ndvProvider;
    this.bootstrapNdv = bootstrapNdv;
    this.columnNames = columnNames == null ? Map.of() : Map.copyOf(columnNames);
    this.logicalTypes = logicalTypes == null ? Map.of() : Map.copyOf(logicalTypes);
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
    final Acc<K> acc = new Acc<>();
    for (K column : planner.columns()) {
      acc.columns.put(
          column,
          new ColAcc(logicalTypeFor(column).orElse(null), columnNameFor(column).orElse(null)));
    }

    long files = 0, rows = 0, bytes = 0;

    final Map<K, ColumnNdv> ndvByKey = new LinkedHashMap<>();
    final Map<String, ColumnNdv> ndvByName = new LinkedHashMap<>();
    if (ndvProvider != null || bootstrapNdv != null) {
      for (var columnAggMap : acc.columns.entrySet()) {
        var columnAgg = columnAggMap.getValue();
        var name = columnAgg.name;
        if (name == null || name.isBlank()) {
          continue;
        }

        ColumnNdv shared = new ColumnNdv();
        columnAgg.ndv = shared;
        ndvByKey.put(columnAggMap.getKey(), shared);
        ndvByName.put(name, shared);
      }
    }

    if (bootstrapNdv != null && !ndvByName.isEmpty()) {
      try {
        bootstrapNdv.contributeNdv("<bootstrap>", ndvByName);
      } catch (Exception ignore) {
        // ignore
      }
    }

    final List<FileAgg<K>> fileAggs = new ArrayList<>();

    for (PlannedFile<K> file : planner) {
      files++;
      rows += file.rowCount();
      bytes += file.sizeBytes();

      mergeCounts(acc.columns, file.valueCounts(), ColAcc::addValueCount);
      mergeCounts(acc.columns, file.nullCounts(), ColAcc::addNullCount);
      mergeCounts(acc.columns, file.nanCounts(), ColAcc::addNanCount);
      mergeBounds(acc.columns, file.lowerBounds(), true);
      mergeBounds(acc.columns, file.upperBounds(), false);

      if (ndvProvider != null && !ndvByName.isEmpty()) {
        try {
          ndvProvider.contributeNdv(file.path(), ndvByName);
        } catch (Exception ignore) {
          // ignore and continue
        }
      }

      Map<K, ColumnAgg> fileCols = buildFileColumnAggs(file);
      fileAggs.add(
          new FileAggImpl<>(
              file.path(),
              file.rowCount(),
              file.sizeBytes(),
              fileCols,
              file.partitionDataJson(),
              file.partitionSpecId()));
    }

    final Map<K, ColumnAgg> out = new LinkedHashMap<>(acc.columns.size());
    for (var columnNameNdv : acc.columns.entrySet()) {
      var columnAcc = columnNameNdv.getValue();
      if (columnAcc.ndv != null) {
        columnAcc.ndv.finalizeTheta();
      }
      out.put(columnNameNdv.getKey(), columnAcc.freeze());
    }

    return new ResultImpl<>(
        rows,
        bytes,
        files,
        Collections.unmodifiableMap(out),
        Collections.unmodifiableList(fileAggs));
  }

  private static final class Acc<K> {
    final Map<K, ColAcc> columns = new LinkedHashMap<>();
  }

  private static final class ColAcc {
    final LogicalType type;
    final String name;

    Long valueCount;
    Long nullCount;
    Long nanCount;
    Object min;
    Object max;
    Long ndvExact;
    ColumnNdv ndv;

    ColAcc(LogicalType type, String name) {
      this.type = type;
      this.name = name;
    }

    void addValueCount(long value) {
      valueCount = sum(valueCount, value);
    }

    void addNullCount(long value) {
      nullCount = sum(nullCount, value);
    }

    void addNanCount(long value) {
      nanCount = sum(nanCount, value);
    }

    void minWith(Object value) {
      if (value == null) {
        return;
      }
      Object normalized = canonical(value);
      if (normalized == null) {
        return;
      }
      if (min == null) {
        min = normalized;
        return;
      }
      Object current = canonical(min);
      if (type == null) {
        if (normalized instanceof Comparable<?> c
            && current != null
            && current.getClass().isInstance(normalized)) {
          @SuppressWarnings("unchecked")
          int cmp = ((Comparable<Object>) c).compareTo(current);
          if (cmp < 0) {
            min = normalized;
          }
        }
      } else if (LogicalComparators.compare(type, normalized, current) < 0) {
        min = normalized;
      }
    }

    void maxWith(Object value) {
      if (value == null) {
        return;
      }
      Object normalized = canonical(value);
      if (normalized == null) {
        return;
      }
      if (max == null) {
        max = normalized;
        return;
      }
      Object current = canonical(max);
      if (type == null) {
        if (normalized instanceof Comparable<?> c
            && current != null
            && current.getClass().isInstance(normalized)) {
          @SuppressWarnings("unchecked")
          int cmp = ((Comparable<Object>) c).compareTo(current);
          if (cmp > 0) {
            max = normalized;
          }
        }
      } else if (LogicalComparators.compare(type, normalized, current) > 0) {
        max = normalized;
      }
    }

    private Object canonical(Object value) {
      if (value == null) {
        return null;
      }
      if (type == null) {
        return value;
      }
      try {
        return LogicalComparators.normalize(type, value);
      } catch (RuntimeException e) {
        try {
          return LogicalCoercions.coerceStatValue(type, value);
        } catch (RuntimeException ignore) {
          return value;
        }
      }
    }

    private static Long sum(Long a, long b) {
      return a == null ? b : a + b;
    }

    ColumnAgg freeze() {
      final Long fExact = ndvExact;
      final ColumnNdv fNdv = ndv;
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
        public ColumnNdv ndv() {
          return fNdv;
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

  private static final class FileAggImpl<K> implements FileAgg<K> {
    private final String path;
    private final long rowCount;
    private final long sizeBytes;
    private final Map<K, ColumnAgg> columns;
    private final String partitionDataJson;
    private final int partitionSpecId;

    FileAggImpl(
        String path,
        long rowCount,
        long sizeBytes,
        Map<K, ColumnAgg> columns,
        String partitionDataJson,
        int partitionSpecId) {
      this.path = path;
      this.rowCount = rowCount;
      this.sizeBytes = sizeBytes;
      this.columns = columns;
      this.partitionDataJson = partitionDataJson;
      this.partitionSpecId = partitionSpecId;
    }

    @Override
    public String path() {
      return path;
    }

    @Override
    public long rowCount() {
      return rowCount;
    }

    @Override
    public long sizeBytes() {
      return sizeBytes;
    }

    @Override
    public Map<K, ColumnAgg> columns() {
      return columns;
    }

    @Override
    public String partitionDataJson() {
      return partitionDataJson;
    }

    @Override
    public int partitionSpecId() {
      return partitionSpecId;
    }
  }

  private Map<K, ColumnAgg> buildFileColumnAggs(PlannedFile<K> file) {
    Map<K, ColumnAgg> out = new LinkedHashMap<>();

    Map<K, Long> valueCounts = file.valueCounts();
    Map<K, Long> nullCounts = file.nullCounts();
    Map<K, Long> nanCounts = file.nanCounts();
    Map<K, Object> lowers = file.lowerBounds();
    Map<K, Object> uppers = file.upperBounds();

    Set<K> keys = new LinkedHashSet<>();
    if (valueCounts != null) keys.addAll(valueCounts.keySet());
    if (nullCounts != null) keys.addAll(nullCounts.keySet());
    if (nanCounts != null) keys.addAll(nanCounts.keySet());
    if (lowers != null) keys.addAll(lowers.keySet());
    if (uppers != null) keys.addAll(uppers.keySet());

    for (K key : keys) {
      Long vc = valueCounts != null ? valueCounts.get(key) : null;
      Long nc = nullCounts != null ? nullCounts.get(key) : null;
      Long nn = nanCounts != null ? nanCounts.get(key) : null;
      Object lo = lowers != null ? lowers.get(key) : null;
      Object hi = uppers != null ? uppers.get(key) : null;

      final Long fVc = vc;
      final Long fNc = nc;
      final Long fNn = nn;
      final Object fLo = lo;
      final Object fHi = hi;

      ColumnAgg colAgg =
          new ColumnAgg() {
            @Override
            public Long ndvExact() {
              return null;
            }

            @Override
            public ColumnNdv ndv() {
              return null;
            }

            @Override
            public Long valueCount() {
              return fVc;
            }

            @Override
            public Long nullCount() {
              return fNc;
            }

            @Override
            public Long nanCount() {
              return fNn;
            }

            @Override
            public Object min() {
              return fLo;
            }

            @Override
            public Object max() {
              return fHi;
            }
          };

      out.put(key, colAgg);
    }

    return Collections.unmodifiableMap(out);
  }

  private static final class ResultImpl<K> implements Result<K> {
    private final long rows;
    private final long bytes;
    private final long files;
    private final Map<K, ColumnAgg> cols;
    private final List<FileAgg<K>> fileAggs;

    ResultImpl(
        long rows, long bytes, long files, Map<K, ColumnAgg> cols, List<FileAgg<K>> fileAggs) {
      this.rows = rows;
      this.bytes = bytes;
      this.files = files;
      this.cols = cols;
      this.fileAggs = fileAggs;
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

    @Override
    public List<FileAgg<K>> files() {
      return fileAggs;
    }
  }

  private static <K> void mergeCounts(
      Map<K, ColAcc> columnAggs, Map<K, Long> files, BiConsumer<ColAcc, Long> apply) {
    if (files == null || files.isEmpty()) {
      return;
    }

    for (var file : files.entrySet()) {
      final ColAcc columnAgg = columnAggs.get(file.getKey());
      final Long value = file.getValue();
      if (columnAgg != null && value != null) {
        apply.accept(columnAgg, value);
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
      if (columnAgg == null) {
        continue;
      }

      if (isLower) {
        columnAgg.minWith(file.getValue());
      } else {
        columnAgg.maxWith(file.getValue());
      }
    }
  }
}
