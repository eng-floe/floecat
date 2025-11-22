package ai.floedb.metacat.connector.iceberg.impl;

import ai.floedb.metacat.connector.common.PlannedFile;
import ai.floedb.metacat.connector.common.Planner;
import ai.floedb.metacat.connector.common.ndv.NdvProvider;
import ai.floedb.metacat.types.LogicalType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

final class IcebergPlanner implements Planner<Integer> {

  private final List<PlannedFile<Integer>> files = new ArrayList<>();
  private final Map<Integer, String> idToName = new HashMap<>();
  private final Map<Integer, LogicalType> idToLogical = new HashMap<>();
  private final Map<Integer, Type> idToIceType = new HashMap<>();
  private final NdvProvider ndvProvider;
  private final Set<Integer> columnSet;
  private final Schema schema;

  IcebergPlanner(
      Table table,
      long snapshotId,
      Set<Integer> colIds,
      NdvProvider ndvProvider,
      boolean planFiles) {
    this.ndvProvider = ndvProvider;

    Snapshot snap = table.snapshot(snapshotId);
    int schemaId = (snap != null) ? snap.schemaId() : table.schema().schemaId();
    this.schema = Optional.ofNullable(table.schemas().get(schemaId)).orElse(table.schema());

    for (Types.NestedField field : schema.columns()) {
      idToName.put(field.fieldId(), field.name());
      idToLogical.put(field.fieldId(), IcebergTypeMapper.toLogical(field.type()));
      idToIceType.put(field.fieldId(), field.type());
    }

    this.columnSet =
        (colIds == null || colIds.isEmpty())
            ? Collections.unmodifiableSet(new LinkedHashSet<>(idToName.keySet()))
            : Collections.unmodifiableSet(new LinkedHashSet<>(colIds));

    if (planFiles) {
      TableScan scan = table.newScan().useSnapshot(snapshotId).includeColumnStats();
      try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
        for (FileScanTask task : tasks) {
          files.add(toPlanned(task.file()));
        }
      } catch (Exception e) {
        throw new RuntimeException("Iceberg planning failed (snapshot " + snapshotId + ")", e);
      }
    }
  }

  IcebergPlanner(Table table, long snapshotId, Set<Integer> colIds, NdvProvider ndvProvider) {
    this(table, snapshotId, colIds, ndvProvider, true);
  }

  @Override
  public Map<Integer, String> columnNamesByKey() {
    return idToName.entrySet().stream()
        .filter(e -> columnSet == null || columnSet.contains(e.getKey()))
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Map<Integer, LogicalType> logicalTypesByKey() {
    return idToLogical.entrySet().stream()
        .filter(e -> columnSet == null || columnSet.contains(e.getKey()))
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Iterator<PlannedFile<Integer>> iterator() {
    return files.iterator();
  }

  @Override
  public void close() {}

  @Override
  public Function<Integer, String> nameOf() {
    return idToName::get;
  }

  @Override
  public Function<Integer, LogicalType> typeOf() {
    return idToLogical::get;
  }

  @Override
  public NdvProvider ndvProvider() {
    return ndvProvider;
  }

  @Override
  public Set<Integer> columns() {
    return columnSet;
  }

  private PlannedFile<Integer> toPlanned(DataFile dataFile) {
    Map<Integer, Long> valueCounts = dataFile.valueCounts();
    Map<Integer, Long> nullCounts = dataFile.nullValueCounts();
    Map<Integer, Long> nanCounts = dataFile.nanValueCounts();

    Map<Integer, Object> lowers = decodeBounds(dataFile.lowerBounds());
    Map<Integer, Object> uppers = decodeBounds(dataFile.upperBounds());

    return new PlannedFile<>(
        dataFile.location().toString(),
        dataFile.recordCount(),
        dataFile.fileSizeInBytes(),
        valueCounts,
        nullCounts,
        nanCounts,
        lowers,
        uppers);
  }

  private Map<Integer, Object> decodeBounds(Map<Integer, ByteBuffer> raw) {
    if (raw == null || raw.isEmpty()) {
      return null;
    }

    Map<Integer, Object> out = new HashMap<>(raw.size() * 2);
    for (var column : raw.entrySet()) {
      Integer id = column.getKey();
      Type type = idToIceType.get(id);
      if (type != null) {
        out.put(id, Conversions.fromByteBuffer(type, column.getValue()));
      } else {
        ByteBuffer dup = column.getValue().duplicate();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        out.put(id, new String(bytes));
      }
    }
    return out;
  }
}
