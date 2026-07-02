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

import ai.floedb.floecat.connector.common.ndv.ColumnNdv;
import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.connector.common.ndv.ParquetAvgWidthProvider;
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
import org.jboss.logging.Logger;

public final class GenericStatsEngine<K> implements StatsEngine<K> {
  private static final Logger LOG = Logger.getLogger(GenericStatsEngine.class);

  private final Planner<K> planner;
  private final NdvProvider ndvProvider;
  private final NdvProvider bootstrapNdv;
  private final ParquetAvgWidthProvider avgWidthProvider;
  private final Map<K, String> columnNames;
  private final Map<K, LogicalType> logicalTypes;
  private final int thetaNominalEntries;

  public GenericStatsEngine(
      Planner<K> planner,
      NdvProvider ndvProvider,
      NdvProvider bootstrapNdv,
      Map<K, String> columnNames,
      Map<K, LogicalType> logicalTypes) {
    this(planner, ndvProvider, bootstrapNdv, null, columnNames, logicalTypes);
  }

  public GenericStatsEngine(
      Planner<K> planner,
      NdvProvider ndvProvider,
      NdvProvider bootstrapNdv,
      ParquetAvgWidthProvider avgWidthProvider,
      Map<K, String> columnNames,
      Map<K, LogicalType> logicalTypes) {
    this(
        planner,
        ndvProvider,
        bootstrapNdv,
        avgWidthProvider,
        columnNames,
        logicalTypes,
        ColumnNdv.DEFAULT_NOMINAL_ENTRIES);
  }

  public GenericStatsEngine(
      Planner<K> planner,
      NdvProvider ndvProvider,
      NdvProvider bootstrapNdv,
      ParquetAvgWidthProvider avgWidthProvider,
      Map<K, String> columnNames,
      Map<K, LogicalType> logicalTypes,
      int thetaNominalEntries) {
    this.planner = Objects.requireNonNull(planner);
    this.ndvProvider = ndvProvider;
    this.bootstrapNdv = bootstrapNdv;
    this.avgWidthProvider = avgWidthProvider;
    this.columnNames = columnNames == null ? Map.of() : Map.copyOf(columnNames);
    this.logicalTypes = logicalTypes == null ? Map.of() : Map.copyOf(logicalTypes);
    // Size the merge union to the configured per-file theta_k so cross-file merges don't downsample
    // below it (Union defaults to k=4096 otherwise).
    this.thetaNominalEntries = thetaNominalEntries;
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

    // Column names are not unique across a nested schema: Iceberg exposes leaf names, so struct
    // fields such as a.name and b.name collide. The NDV/avg-width providers are name-keyed, so a
    // duplicated name cannot be attributed to a single column K without cross-contaminating
    // distinct columns. Exclude ambiguous names from name-keyed capture — those columns get no
    // sketch/width rather than wrong ones. Unique names (the common flat-schema case) are
    // unaffected.
    final Set<String> ambiguousNames = duplicatedColumnNames(acc.columns);
    if (!ambiguousNames.isEmpty()) {
      LOG.warnf(
          "Skipping NDV/avg-width capture for columns with non-unique names: %s", ambiguousNames);
    }

    final Map<K, ColumnNdv> ndvByKey = new LinkedHashMap<>();
    final Map<String, ColumnNdv> ndvByName = new LinkedHashMap<>();
    final Map<String, ParquetAvgWidthProvider.AvgWidthAcc> avgWidthByName = new LinkedHashMap<>();
    if (ndvProvider != null || bootstrapNdv != null) {
      for (var columnAggMap : acc.columns.entrySet()) {
        var columnAgg = columnAggMap.getValue();
        var name = columnAgg.name;
        if (name == null || name.isBlank() || ambiguousNames.contains(name)) {
          continue;
        }

        ColumnNdv shared = new ColumnNdv(thetaNominalEntries);
        columnAgg.ndv = shared;
        ndvByKey.put(columnAggMap.getKey(), shared);
        ndvByName.put(name, shared);
      }
    }

    if (avgWidthProvider != null) {
      for (var columnAggMap : acc.columns.entrySet()) {
        var name = columnAggMap.getValue().name;
        if (name != null && !name.isBlank() && !ambiguousNames.contains(name)) {
          var widthAcc = new ParquetAvgWidthProvider.AvgWidthAcc();
          columnAggMap.getValue().avgWidthAcc = widthAcc;
          avgWidthByName.put(name, widthAcc);
        }
      }
    }

    if (bootstrapNdv != null && !ndvByName.isEmpty()) {
      try {
        bootstrapNdv.contributeNdv("<bootstrap>", ndvByName);
      } catch (Exception ignore) {
        // ignore
      }
    }

    /*
     * Puffin-only NDV path: when ndvProvider == null (no per-file Parquet scan) but
     * bootstrapNdv covered all columns (e.g. Iceberg Puffin stats), the cumulative
     * ndvByName already has the correct table-level sketch.  Finalize it now and reuse
     * it as the "per-file" NDV for every file record, so FileGroupTargetStatsRollup can
     * merge it.
     *
     * Correctness: theta-union of N identical sketches is idempotent (S ∪ S = S),
     * so the rollup result is the bootstrap sketch — which is correct.
     *
     * When ndvProvider != null, finalization happens after the file loop (normal path);
     * finalizeTheta() is a no-op if thetaUnion is already null.
     */
    Map<K, ColumnNdv> bootstrapPerFileNdv = null;
    if (ndvProvider == null && bootstrapNdv != null && !ndvByName.isEmpty()) {
      for (var n : ndvByName.values()) {
        n.finalizeTheta();
      }
      bootstrapPerFileNdv = new LinkedHashMap<>();
      for (var colEntry : acc.columns.entrySet()) {
        ColumnNdv n = ndvByName.get(colEntry.getValue().name);
        if (n != null && !n.sketches.isEmpty()) {
          bootstrapPerFileNdv.put(colEntry.getKey(), n);
        }
      }
    }

    final List<FileAgg<K>> fileAggs = new ArrayList<>();

    for (PlannedFile<K> file : planner) {
      files++;
      rows += file.rowCount();
      bytes += file.sizeBytes();

      mergeCounts(acc.columns, file.rowCounts(), ColAcc::addRowCount);
      mergeCounts(acc.columns, file.nullCounts(), ColAcc::addNullCount);
      mergeCounts(acc.columns, file.nanCounts(), ColAcc::addNanCount);
      mergeBounds(acc.columns, file.lowerBounds(), true);
      mergeBounds(acc.columns, file.upperBounds(), false);

      /*
       * Per-file NDV: fresh ColumnNdv accumulators per file; merge serialized theta
       * bytes into the cumulative union; pass finalized per-file ColumnNdv to
       * buildFileColumnAggs() so file records carry a mergeable theta sketch.
       * Falls back to bootstrapPerFileNdv when ndvProvider is absent (Puffin path).
       */
      Map<K, ColumnNdv> perFileNdvByKey = null;
      if (ndvProvider != null && !ndvByName.isEmpty()) {
        try {
          Map<String, ColumnNdv> perFileByName = new LinkedHashMap<>();
          for (String name : ndvByName.keySet()) {
            perFileByName.put(name, new ColumnNdv(thetaNominalEntries));
          }
          ndvProvider.contributeNdv(file.path(), perFileByName);
          /* Finalize per-file sketches, then merge their bytes into the cumulative union. */
          for (var e : perFileByName.entrySet()) {
            e.getValue().finalizeTheta();
            ColumnNdv cumulative = ndvByName.get(e.getKey());
            if (cumulative != null) {
              cumulative.mergeApproxMetadata(e.getValue());
              if (!e.getValue().sketches.isEmpty()) {
                cumulative.mergeTheta(e.getValue().sketches.get(0).data);
              }
            }
          }
          /* Build K-keyed map for buildFileColumnAggs. */
          perFileNdvByKey =
              rekeyByColName(acc.columns, perFileByName, n -> n.sketches.isEmpty() ? null : n);
        } catch (Exception ignore) {
          // NDV is best-effort; fall through with null perFileNdvByKey
        }
      }

      /*
       * Compute per-file avg_width using fresh per-file accumulators, then merge those
       * into the cumulative accumulators so the table-level ColumnAgg stays correct.
       * This gives us per-file avg_width values to include in the FileAgg records,
       * which FileGroupTargetStatsRollup can then weighted-average into column-level stats.
       */
      Map<K, Long> perFileAvgWidths = null;
      if (avgWidthProvider != null && !avgWidthByName.isEmpty() && isParquet(file)) {
        LOG.debugf(
            "[avg_width] starting footer scan for %s (provider=%s sinks=%s)",
            file.path(), avgWidthProvider.getClass().getSimpleName(), avgWidthByName.keySet());
        try {
          Map<String, ParquetAvgWidthProvider.AvgWidthAcc> perFileByName = new LinkedHashMap<>();
          for (String name : avgWidthByName.keySet()) {
            perFileByName.put(name, new ParquetAvgWidthProvider.AvgWidthAcc());
          }
          avgWidthProvider.contributeAvgWidth(file.path(), perFileByName);
          long matched =
              perFileByName.values().stream().filter(a -> a.avgWidthBytes() != null).count();
          if (matched == 0) {
            LOG.warnf(
                "[avg_width] no columns matched in footer for %s (sinks=%s)",
                file.path(), perFileByName.keySet());
          } else {
            LOG.debugf(
                "[avg_width] %d/%d columns matched in %s",
                matched, perFileByName.size(), file.path());
          }
          /* Merge per-file into cumulative. */
          for (var e : perFileByName.entrySet()) {
            avgWidthByName.get(e.getKey()).merge(e.getValue());
          }
          /* Build K-keyed map for buildFileColumnAggs (null from avgWidthBytes = skip). */
          perFileAvgWidths =
              rekeyByColName(
                  acc.columns, perFileByName, ParquetAvgWidthProvider.AvgWidthAcc::avgWidthBytes);
        } catch (Exception ex) {
          // avg_width is best-effort; log the failure so it is not silently swallowed
          LOG.warnf("avg_width footer scan failed for file %s: %s", file.path(), ex.getMessage());
        }
      }

      /* Use per-file NDV from scan, or bootstrap sketch when no scan was done (Puffin path). */
      Map<K, ColumnNdv> ndvForFile =
          perFileNdvByKey != null ? perFileNdvByKey : bootstrapPerFileNdv;
      Map<K, ColumnAgg> fileCols = buildFileColumnAggs(file, ndvForFile, perFileAvgWidths);
      fileAggs.add(
          new FileAggImpl<>(
              file.path(),
              file.format(),
              file.rowCount(),
              file.sizeBytes(),
              fileCols,
              file.partitionDataJson(),
              file.partitionSpecId(),
              file.sequenceNumber()));
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

    Long rowCount;
    Long nullCount;
    Long nanCount;
    Object min;
    Object max;
    Long ndvExact;
    ColumnNdv ndv;
    ParquetAvgWidthProvider.AvgWidthAcc avgWidthAcc;

    ColAcc(LogicalType type, String name) {
      this.type = type;
      this.name = name;
    }

    void addRowCount(long value) {
      rowCount = sum(rowCount, value);
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
      final Long fRow = rowCount;
      final Long fNull = nullCount;
      final Long fNan = nanCount;
      final Object fMin = min;
      final Object fMax = max;
      final Long fAvgWidth = avgWidthAcc != null ? avgWidthAcc.avgWidthBytes() : null;

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
        public Long rowCount() {
          return fRow;
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

        @Override
        public Long avgWidthBytes() {
          return fAvgWidth;
        }
      };
    }
  }

  private static final class FileAggImpl<K> implements FileAgg<K> {
    private final String path;
    private final String format;
    private final long rowCount;
    private final long sizeBytes;
    private final Map<K, ColumnAgg> columns;
    private final String partitionDataJson;
    private final int partitionSpecId;
    private final Long sequenceNumber;

    FileAggImpl(
        String path,
        String format,
        long rowCount,
        long sizeBytes,
        Map<K, ColumnAgg> columns,
        String partitionDataJson,
        int partitionSpecId,
        Long sequenceNumber) {
      this.path = path;
      this.format = format;
      this.rowCount = rowCount;
      this.sizeBytes = sizeBytes;
      this.columns = columns;
      this.partitionDataJson = partitionDataJson;
      this.partitionSpecId = partitionSpecId;
      this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String path() {
      return path;
    }

    @Override
    public String format() {
      return format;
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

    @Override
    public Long sequenceNumber() {
      return sequenceNumber;
    }
  }

  /**
   * Builds per-file column aggregates for one Parquet file.
   *
   * @param file the planned file whose footer metrics to use
   * @param perFileNdv K-keyed finalized ColumnNdv for this file only, or null if unavailable.
   *     Carries a serialized theta sketch that FileGroupTargetStatsRollup can merge.
   * @param perFileAvgWidths K-keyed avg_width_bytes for this file only, or null if unavailable
   */
  private Map<K, ColumnAgg> buildFileColumnAggs(
      PlannedFile<K> file, Map<K, ColumnNdv> perFileNdv, Map<K, Long> perFileAvgWidths) {
    Map<K, ColumnAgg> out = new LinkedHashMap<>();

    Map<K, Long> rowCounts = file.rowCounts();
    Map<K, Long> nullCounts = file.nullCounts();
    Map<K, Long> nanCounts = file.nanCounts();
    Map<K, Object> lowers = file.lowerBounds();
    Map<K, Object> uppers = file.upperBounds();

    Set<K> keys = new LinkedHashSet<>();
    keys.addAll(planner.columns()); // ensure ALL schema columns present, even without metrics
    if (rowCounts != null) keys.addAll(rowCounts.keySet());
    if (nullCounts != null) keys.addAll(nullCounts.keySet());
    if (nanCounts != null) keys.addAll(nanCounts.keySet());
    if (lowers != null) keys.addAll(lowers.keySet());
    if (uppers != null) keys.addAll(uppers.keySet());

    for (K key : keys) {
      out.put(
          key,
          new FileColumnAgg(
              rowCounts != null ? rowCounts.get(key) : null,
              nullCounts != null ? nullCounts.get(key) : null,
              nanCounts != null ? nanCounts.get(key) : null,
              lowers != null ? lowers.get(key) : null,
              uppers != null ? uppers.get(key) : null,
              perFileNdv != null ? perFileNdv.get(key) : null,
              perFileAvgWidths != null ? perFileAvgWidths.get(key) : null));
    }

    return Collections.unmodifiableMap(out);
  }

  /**
   * Per-file column aggregate: holds the footer-derived metrics for one column in one Parquet file.
   * ndvExact is always null at the file level (only the table-level ColAcc.freeze() can produce
   * exact NDV from small distinct counts).
   */
  /**
   * Re-keys a name-keyed accumulator map into the planner's K-keyed result map. {@code mapper}
   * converts an accumulator entry to the desired value type, returning {@code null} to skip that
   * column (e.g. when the value is not yet computed). Used to convert per-file NDV and avg_width
   * maps from column-name keys (providers) to the planner's opaque K keys (buildFileColumnAggs).
   */
  /**
   * The avg-width provider reads a Parquet footer, so it must only run on Parquet data files.
   * Unlike NDV (wrapped in a suffix-filtering provider at the connector), avg-width is passed
   * through directly, so an unfiltered scan would throw and warn per file on ORC/Avro tables. Trust
   * {@code format()} when present, otherwise fall back to the path suffix.
   */
  private static boolean isParquet(PlannedFile<?> file) {
    String format = file.format();
    if (format != null && !format.isBlank()) {
      return "PARQUET".equalsIgnoreCase(format);
    }
    String path = file.path();
    return path != null && (path.endsWith(".parquet") || path.endsWith(".parq"));
  }

  /** Returns column names shared by more than one column key (ambiguous for name-keyed capture). */
  private static <K> Set<String> duplicatedColumnNames(Map<K, ColAcc> columns) {
    Map<String, Integer> counts = new LinkedHashMap<>();
    for (ColAcc column : columns.values()) {
      if (column.name != null && !column.name.isBlank()) {
        counts.merge(column.name, 1, Integer::sum);
      }
    }
    Set<String> duplicates = new LinkedHashSet<>();
    counts.forEach(
        (name, count) -> {
          if (count > 1) {
            duplicates.add(name);
          }
        });
    return duplicates;
  }

  private static <K, A, V> Map<K, V> rekeyByColName(
      Map<K, ColAcc> columns, Map<String, A> byName, java.util.function.Function<A, V> mapper) {
    Map<K, V> out = new LinkedHashMap<>();
    for (var e : columns.entrySet()) {
      A acc = byName.get(e.getValue().name);
      if (acc != null) {
        V val = mapper.apply(acc);
        if (val != null) {
          out.put(e.getKey(), val);
        }
      }
    }
    return out;
  }

  private record FileColumnAgg(
      Long rowCount,
      Long nullCount,
      Long nanCount,
      Object min,
      Object max,
      ColumnNdv ndv,
      Long avgWidthBytes)
      implements ColumnAgg {
    @Override
    public Long ndvExact() {
      return null;
    }
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
