package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.connector.common.ParquetFooterStats;
import ai.floedb.floecat.connector.common.PlannedFile;
import ai.floedb.floecat.connector.common.Planner;
import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.types.LogicalCoercions;
import ai.floedb.floecat.types.LogicalComparators;
import ai.floedb.floecat.types.LogicalType;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.parquet.io.InputFile;

final class DeltaPlanner implements Planner<String> {
  private static final String PARQUET_FORMAT = "PARQUET";

  private final List<PlannedFile<String>> files = new ArrayList<>();
  private final Map<String, LogicalType> nameToLogical = new LinkedHashMap<>();
  private final NdvProvider ndvProvider;
  private final Set<String> columnSet;
  private final List<DeletionVectorDescriptor> diskDeletionVectors = new ArrayList<>();
  private boolean hasInlineDeletionVectors = false;

  DeltaPlanner(
      Engine engine,
      Function<String, InputFile> parquetInput,
      String tableRoot,
      long version,
      Set<String> includeColumns,
      Map<String, LogicalType> nameToType,
      NdvProvider ndvProvider,
      boolean includeStats) {

    this.ndvProvider = ndvProvider;

    final Table table = Table.forPath(engine, Objects.requireNonNull(tableRoot));
    final Snapshot snapshot = table.getSnapshotAsOfVersion(engine, version);

    final StructType schema = snapshot.getSchema();
    if (nameToType == null || nameToType.isEmpty()) {
      nameToLogical.putAll(DeltaTypeMapper.deltaTypeMap(schema));
    } else {
      nameToLogical.putAll(nameToType);
    }

    if (includeColumns == null || includeColumns.isEmpty()) {
      this.columnSet = Collections.unmodifiableSet(new LinkedHashSet<>(nameToLogical.keySet()));
    } else {
      LinkedHashSet<String> filtered =
          includeColumns.stream()
              .filter(nameToLogical::containsKey)
              .collect(Collectors.toCollection(LinkedHashSet::new));
      this.columnSet = Collections.unmodifiableSet(filtered);
    }

    final ScanBuilder sb = snapshot.getScanBuilder();
    final Scan scan = sb.build();

    try (CloseableIterator<FilteredColumnarBatch> it = scan.getScanFiles(engine)) {
      while (it.hasNext()) {
        FilteredColumnarBatch batch = it.next();
        try (var rows = batch.getRows()) {
          while (rows.hasNext()) {
            Row scanFileRow = rows.next();

            FileStatus fs = InternalScanFileUtils.getAddFileStatus(scanFileRow);
            String path = fs.getPath();
            long sizeBytes = fs.getSize();

            long rowCount = 0L;
            Map<String, Long> valueCounts = null;
            Map<String, Long> nullCounts = null;
            Map<String, Long> nanCounts = null;
            Map<String, Object> mins = null;
            Map<String, Object> maxs = null;
            String partitionJson = "{\"partitionValues\":[]}";

            if (includeStats) {
              AddFile add =
                  new AddFile(scanFileRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
              add.getDeletionVector()
                  .ifPresent(
                      dv -> {
                        if (dv.isInline()) {
                          hasInlineDeletionVectors = true;
                        } else if (dv.isOnDisk()) {
                          diskDeletionVectors.add(dv);
                        }
                      });
              partitionJson = encodePartition(add);
              Optional<DataFileStatistics> optStats = add.getStats();
              if (optStats.isPresent()) {
                DataFileStatistics stats = optStats.get();

                rowCount = stats.getNumRecords();

                Map<Column, Long> nc = stats.getNullCount();
                if (nc != null && !nc.isEmpty()) {
                  nullCounts = new LinkedHashMap<>(nc.size());
                  for (var e : nc.entrySet()) {
                    String colName = firstName(e.getKey());
                    Long n = e.getValue();
                    if (colName != null && n != null) nullCounts.put(colName, n);
                  }
                }

                if (rowCount > 0 && nullCounts != null && !nullCounts.isEmpty()) {
                  valueCounts = new LinkedHashMap<>(nullCounts.size());
                  for (var e : nullCounts.entrySet()) {
                    long ncVal = e.getValue() == null ? 0L : e.getValue();
                    valueCounts.put(e.getKey(), Math.max(0L, rowCount - ncVal));
                  }
                }

                Map<Column, Literal> minsMap = stats.getMinValues();
                if (minsMap != null && !minsMap.isEmpty()) {
                  mins = new LinkedHashMap<>(minsMap.size());
                  for (var e : minsMap.entrySet()) {
                    String colName = firstName(e.getKey());
                    if (colName != null) {
                      var lt = nameToLogical.get(colName);
                      Object raw = (e.getValue() == null) ? null : e.getValue().getValue();
                      Object typed = (lt == null) ? raw : LogicalComparators.normalize(lt, raw);
                      mins.put(colName, typed);
                    }
                  }
                }

                Map<Column, Literal> maxsMap = stats.getMaxValues();
                if (maxsMap != null && !maxsMap.isEmpty()) {
                  maxs = new LinkedHashMap<>(maxsMap.size());
                  for (var e : maxsMap.entrySet()) {
                    String colName = firstName(e.getKey());
                    if (colName != null) {
                      var lt = nameToLogical.get(colName);
                      Object raw = (e.getValue() == null) ? null : e.getValue().getValue();
                      Object typed = (lt == null) ? raw : LogicalComparators.normalize(lt, raw);
                      maxs.put(colName, typed);
                    }
                  }
                }
              } else {
                var in = parquetInput.apply(path);

                var bfs = ParquetFooterStats.read(in, columnSet, nameToLogical);

                rowCount = bfs.rowCount;
                nullCounts = new LinkedHashMap<>();
                mins = new LinkedHashMap<>();
                maxs = new LinkedHashMap<>();
                valueCounts = new LinkedHashMap<>();

                for (var e : bfs.cols.entrySet()) {
                  String name = e.getKey();
                  var c = e.getValue();
                  nullCounts.put(name, c.nulls);
                  if (rowCount > 0) valueCounts.put(name, Math.max(0L, rowCount - c.nulls));
                  var lt = nameToLogical.get(name);
                  if (lt != null) {
                    if (c.min != null) {
                      mins.put(name, LogicalCoercions.coerceStatValue(lt, c.min));
                    }
                    if (c.max != null) {
                      maxs.put(name, LogicalCoercions.coerceStatValue(lt, c.max));
                    }
                  }
                }
              }
            }

            files.add(
                new PlannedFile<>(
                    path,
                    PARQUET_FORMAT,
                    rowCount,
                    sizeBytes,
                    valueCounts,
                    nullCounts,
                    nanCounts,
                    mins,
                    maxs,
                    partitionJson,
                    0,
                    null));
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Delta planning failed (version " + version + ")", e);
    }
  }

  boolean hasDeletionVectors() {
    return !diskDeletionVectors.isEmpty() || hasInlineDeletionVectors;
  }

  boolean hasInlineDeletionVectors() {
    return hasInlineDeletionVectors;
  }

  List<DeletionVectorDescriptor> deletionVectors() {
    return Collections.unmodifiableList(diskDeletionVectors);
  }

  private String encodePartition(AddFile add) {
    try {
      Map<String, String> partitionValues = toPartitionMap(add.getPartitionValues());
      if (partitionValues == null || partitionValues.isEmpty()) {
        return "{\"partitionValues\":[]}";
      }
      StringBuilder sb = new StringBuilder();
      sb.append("{\"partitionValues\":[");
      int i = 0;
      for (var entry : partitionValues.entrySet()) {
        if (i++ > 0) {
          sb.append(',');
        }
        sb.append("{\"id\":").append('\"').append(escape(entry.getKey())).append('\"');
        sb.append(",\"value\":").append(toJsonValue(entry.getValue())).append('}');
      }
      sb.append("]}");
      return sb.toString();
    } catch (Exception e) {
      return "";
    }
  }

  private String toJsonValue(String value) {
    if (value == null) {
      return "null";
    }
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      return value.toLowerCase();
    }
    try {
      Double.parseDouble(value);
      return value;
    } catch (NumberFormatException ignore) {
      // fall through
    }
    return "\"" + escape(value) + "\"";
  }

  private String escape(String s) {
    return String.valueOf(s).replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private Map<String, String> toPartitionMap(MapValue mapValue) {
    if (mapValue == null) {
      return Map.of();
    }
    var keys = mapValue.getKeys();
    var vals = mapValue.getValues();
    int size = mapValue.getSize();
    Map<String, String> out = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      if (keys.isNullAt(i)) {
        continue;
      }
      String key = keys.getString(i);
      String value = vals.isNullAt(i) ? null : vals.getString(i);
      out.put(key, value);
    }
    return out;
  }

  @Override
  public Map<String, String> columnNamesByKey() {
    var out = new LinkedHashMap<String, String>(columnSet.size());
    for (String name : columnSet) {
      out.put(name, name);
    }
    return Collections.unmodifiableMap(out);
  }

  @Override
  public Map<String, LogicalType> logicalTypesByKey() {
    var out = new LinkedHashMap<String, LogicalType>(columnSet.size());
    for (String name : columnSet) {
      var lt = nameToLogical.get(name);
      if (lt != null) {
        out.put(name, lt);
      }
    }
    return Collections.unmodifiableMap(out);
  }

  @Override
  public Iterator<PlannedFile<String>> iterator() {
    return files.iterator();
  }

  @Override
  public void close() {}

  @Override
  public Function<String, String> nameOf() {
    return Function.identity();
  }

  @Override
  public Function<String, LogicalType> typeOf() {
    return nameToLogical::get;
  }

  @Override
  public NdvProvider ndvProvider() {
    return ndvProvider;
  }

  @Override
  public Set<String> columns() {
    return columnSet;
  }

  private static String firstName(Column c) {
    try {
      String[] names = (c == null) ? null : c.getNames();
      return (names == null || names.length == 0) ? null : names[0];
    } catch (Throwable ignore) {
      return null;
    }
  }
}
