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

package ai.floedb.floecat.connector.iceberg.impl;

import ai.floedb.floecat.connector.common.PlannedFile;
import ai.floedb.floecat.connector.common.Planner;
import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.types.LogicalCoercions;
import ai.floedb.floecat.types.LogicalType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

final class IcebergPlanner implements Planner<Integer> {
  private static final long MICROS_PER_DAY = 86_400_000_000L;
  private static final long MICROS_PER_SECOND = 1_000_000L;
  private static final long NANOS_PER_SECOND = 1_000_000_000L;

  private final List<PlannedFile<Integer>> files = new ArrayList<>();
  private final Map<Integer, String> idToName = new HashMap<>();
  private final Map<Integer, LogicalType> idToLogical = new HashMap<>();
  private final Map<Integer, Type> idToIceType = new HashMap<>();
  private final NdvProvider ndvProvider;
  private final Set<Integer> columnSet;
  private final Schema schema;
  private final Map<Integer, PartitionSpec> specsById;
  private final PartitionSpec defaultSpec;

  IcebergPlanner(
      Table table,
      long snapshotId,
      Set<Integer> colIds,
      NdvProvider ndvProvider,
      boolean planFiles) {
    this.ndvProvider = ndvProvider;

    Snapshot snap = table.snapshot(snapshotId);
    Integer snapSchemaId = snap != null ? snap.schemaId() : null;
    int schemaId =
        snapSchemaId != null && snapSchemaId > 0 ? snapSchemaId : table.schema().schemaId();
    this.schema = Optional.ofNullable(table.schemas().get(schemaId)).orElse(table.schema());
    this.specsById = table.specs();
    this.defaultSpec = table.spec();

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

    PartitionSpec spec = specsById.getOrDefault(dataFile.specId(), defaultSpec);
    String partJson = partitionJson(spec, dataFile.partition());
    Long sequenceNumber = dataFile.fileSequenceNumber();
    if (sequenceNumber != null && sequenceNumber <= 0) {
      sequenceNumber = null;
    }

    return new PlannedFile<>(
        dataFile.location().toString(),
        dataFile.format().name(),
        dataFile.recordCount(),
        dataFile.fileSizeInBytes(),
        valueCounts,
        nullCounts,
        nanCounts,
        lowers,
        uppers,
        partJson,
        spec == null ? 0 : spec.specId(),
        sequenceNumber);
  }

  private Map<Integer, Object> decodeBounds(Map<Integer, ByteBuffer> raw) {
    if (raw == null || raw.isEmpty()) {
      return null;
    }

    Map<Integer, Object> out = new HashMap<>(raw.size() * 2);
    for (var column : raw.entrySet()) {
      Integer id = column.getKey();
      Type type = idToIceType.get(id);
      Object decoded = null;
      if (type != null) {
        decoded = Conversions.fromByteBuffer(type, column.getValue());
      } else {
        ByteBuffer dup = column.getValue().duplicate();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        decoded = new String(bytes, StandardCharsets.UTF_8);
      }
      decoded = canonicalizeDecodedBound(type, decoded);
      LogicalType logicalType = idToLogical.get(id);
      Object canonical =
          logicalType != null ? LogicalCoercions.coerceStatValue(logicalType, decoded) : decoded;
      if (canonical != null) {
        out.put(id, canonical);
      }
    }
    return out.isEmpty() ? null : out;
  }

  static Object canonicalizeDecodedBound(Type type, Object decoded) {
    if (type == null || decoded == null || !(decoded instanceof Number n)) {
      return decoded;
    }
    return switch (type.typeId()) {
      case TIME -> canonicalTimeBound(n.longValue());
      case TIMESTAMP -> canonicalTimestampMicrosBound(type, n.longValue());
      case TIMESTAMP_NANO -> canonicalTimestampNanosBound(type, n.longValue());
      default -> decoded;
    };
  }

  private static LocalTime canonicalTimeBound(long micros) {
    if (micros < 0 || micros >= MICROS_PER_DAY) {
      return null;
    }
    return LocalTime.ofNanoOfDay(micros * 1_000L);
  }

  private static Object canonicalTimestampMicrosBound(Type type, long micros) {
    long seconds = Math.floorDiv(micros, MICROS_PER_SECOND);
    long microsRemainder = Math.floorMod(micros, MICROS_PER_SECOND);
    Instant instant = Instant.ofEpochSecond(seconds, microsRemainder * 1_000L);
    return convertTimestampBound(type, instant);
  }

  private static Object canonicalTimestampNanosBound(Type type, long nanos) {
    long seconds = Math.floorDiv(nanos, NANOS_PER_SECOND);
    long nanosRemainder = Math.floorMod(nanos, NANOS_PER_SECOND);
    Instant instant = Instant.ofEpochSecond(seconds, nanosRemainder);
    return convertTimestampBound(type, instant);
  }

  private static Object convertTimestampBound(Type type, Instant instant) {
    if (type instanceof Types.TimestampType ts) {
      return ts.shouldAdjustToUTC() ? instant : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
    if (type instanceof Types.TimestampNanoType ts) {
      return ts.shouldAdjustToUTC() ? instant : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
    return instant;
  }

  private String partitionJson(PartitionSpec spec, StructLike partition) {
    if (spec == null || partition == null || spec.fields().isEmpty()) {
      return "{\"partitionValues\":[]}";
    }
    try {
      List<Map<String, Object>> values = new ArrayList<>(spec.fields().size());
      for (int i = 0; i < spec.fields().size(); i++) {
        PartitionField field = spec.fields().get(i);
        Object value = partition.get(i, Object.class);
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("id", field.sourceId());
        entry.put("value", value);
        values.add(entry);
      }
      Map<String, Object> root = new LinkedHashMap<>();
      root.put("partitionValues", values);
      return new ObjectMapper().writeValueAsString(root);
    } catch (Exception e) {
      return "";
    }
  }
}
