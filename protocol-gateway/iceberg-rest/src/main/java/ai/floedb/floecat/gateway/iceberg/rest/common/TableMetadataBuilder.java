package ai.floedb.floecat.gateway.iceberg.rest.common;

import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.maxPartitionFieldId;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.normalizeSortOrder;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.partitionSpecFromRequest;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.partitionSpecsFromMetadata;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.schemaFromRequest;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.schemaFromTable;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.schemasFromMetadata;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.sortOrderFromRequest;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.sortOrdersFromMetadata;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMapper.metadataLog;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMapper.nonNullMapList;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMapper.partitionStatistics;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMapper.refs;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMapper.sanitizeStatistics;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMapper.snapshotLog;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMapper.snapshots;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMapper.statistics;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asInteger;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asLong;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.maybeInt;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public final class TableMetadataBuilder {
  private TableMetadataBuilder() {}

  public static TableMetadataView fromCatalog(
      String tableName,
      Table table,
      Map<String, String> props,
      IcebergMetadata metadata,
      List<Snapshot> snapshots) {
    String metadataLocation = resolveMetadataLocation(metadata);
    return buildMetadata(tableName, table, props, metadata, snapshots, metadataLocation);
  }

  public static TableMetadataView fromCreateRequest(
      String tableName, Table table, TableRequests.Create request) {
    return initialMetadata(tableName, table, request);
  }

  private static TableMetadataView buildMetadata(
      String tableName,
      Table table,
      Map<String, String> props,
      IcebergMetadata metadata,
      List<Snapshot> snapshots,
      String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      metadataLocation = MetadataLocationUtil.metadataLocation(props);
    }
    String location = table.hasUpstream() ? table.getUpstream().getUri() : props.get("location");
    location = resolveTableLocation(location, metadataLocation);
    Long lastUpdatedMs =
        (metadata != null && metadata.getLastUpdatedMs() > 0)
            ? Long.valueOf(metadata.getLastUpdatedMs())
            : null;
    Long currentSnapshotId =
        metadata != null && metadata.getCurrentSnapshotId() > 0
            ? Long.valueOf(metadata.getCurrentSnapshotId())
            : null;
    Long lastSequenceNumber =
        metadata != null && metadata.getLastSequenceNumber() >= 0
            ? Long.valueOf(metadata.getLastSequenceNumber())
            : null;
    Integer lastColumnId =
        metadata != null && metadata.getLastColumnId() >= 0
            ? Integer.valueOf(metadata.getLastColumnId())
            : null;
    Integer currentSchemaId =
        metadata != null && metadata.getCurrentSchemaId() >= 0
            ? Integer.valueOf(metadata.getCurrentSchemaId())
            : null;
    Integer defaultSpecId =
        metadata != null && metadata.getDefaultSpecId() >= 0
            ? Integer.valueOf(metadata.getDefaultSpecId())
            : null;
    Integer lastPartitionId =
        metadata != null && metadata.getLastPartitionId() >= 0
            ? Integer.valueOf(metadata.getLastPartitionId())
            : null;
    Integer defaultSortOrderId =
        metadata != null && metadata.getDefaultSortOrderId() >= 0
            ? Integer.valueOf(metadata.getDefaultSortOrderId())
            : null;
    String tableUuid =
        metadata != null && metadata.getTableUuid() != null && !metadata.getTableUuid().isBlank()
            ? metadata.getTableUuid()
            : null;
    Integer formatVersion =
        metadata != null && metadata.getFormatVersion() > 0
            ? Integer.valueOf(metadata.getFormatVersion())
            : null;
    if (formatVersion == null || formatVersion < 1) {
      formatVersion = maybeInt(props.get("format-version"));
    }
    if (formatVersion == null || formatVersion < 1) {
      formatVersion = 1;
    }
    if (tableUuid == null) {
      String candidate = props.get("table-uuid");
      if (candidate != null && !candidate.isBlank()) {
        tableUuid = candidate;
      }
    }
    if (tableUuid == null && table != null && table.hasResourceId()) {
      String candidate = table.getResourceId().getId();
      if (candidate != null && !candidate.isBlank()) {
        tableUuid = candidate;
      }
    }
    if (tableUuid == null || tableUuid.isBlank()) {
      tableUuid = tableName;
    }
    if (currentSchemaId == null) {
      currentSchemaId = maybeInt(props.get("current-schema-id"));
    }
    if (lastColumnId == null) {
      lastColumnId = maybeInt(props.get("last-column-id"));
    }
    if (defaultSpecId == null) {
      defaultSpecId = maybeInt(props.get("default-spec-id"));
    }
    if (lastPartitionId == null) {
      lastPartitionId = maybeInt(props.get("last-partition-id"));
    }
    if (defaultSortOrderId == null) {
      defaultSortOrderId = maybeInt(props.get("default-sort-order-id"));
    }
    if (lastUpdatedMs == null) {
      lastUpdatedMs = asLong(props.get("last-updated-ms"));
    }
    if (lastUpdatedMs == null || lastUpdatedMs <= 0) {
      lastUpdatedMs = System.currentTimeMillis();
    }
    if (currentSnapshotId == null) {
      currentSnapshotId = asLong(props.get("current-snapshot-id"));
    }
    if (lastSequenceNumber == null) {
      lastSequenceNumber = asLong(props.get("last-sequence-number"));
    }
    if (lastSequenceNumber == null) {
      lastSequenceNumber = 0L;
    }
    List<Map<String, Object>> schemaList = schemasFromMetadata(metadata);
    if (schemaList.isEmpty()) {
      try {
        Map<String, Object> schema = schemaFromTable(table);
        schemaList = List.of(schema);
        if (currentSchemaId == null) {
          currentSchemaId = asInteger(schema.get("schema-id"));
        }
        if (lastColumnId == null) {
          lastColumnId = asInteger(schema.get("last-column-id"));
        }
      } catch (IllegalArgumentException e) {
        // ignore; fall back to metadata-derived defaults only
      }
    }
    List<Map<String, Object>> specList = partitionSpecsFromMetadata(metadata);
    if (specList.isEmpty()) {
      Map<String, Object> spec = defaultPartitionSpec();
      specList = List.of(spec);
      if (defaultSpecId == null) {
        defaultSpecId = asInteger(spec.get("spec-id"));
      }
      if (lastPartitionId == null) {
        lastPartitionId = maxPartitionFieldId(spec);
      }
    }
    List<Map<String, Object>> sortOrderList = sortOrdersFromMetadata(metadata);
    if (sortOrderList.isEmpty()) {
      Map<String, Object> order = defaultSortOrder();
      sortOrderList = List.of(order);
      if (defaultSortOrderId == null) {
        defaultSortOrderId = asInteger(order.get("order-id"));
      }
    }
    if (!sortOrderList.isEmpty()) {
      sortOrderList.forEach(order -> normalizeSortOrder(order));
    }
    List<Map<String, Object>> statisticsList = sanitizeStatistics(statistics(metadata));
    List<Map<String, Object>> partitionStatisticsList =
        nonNullMapList(partitionStatistics(metadata));
    List<Snapshot> orderedSnapshots =
        snapshots == null || snapshots.size() < 2
            ? snapshots
            : snapshots.stream()
                .sorted(
                    Comparator.comparingLong(Snapshot::getSequenceNumber)
                        .thenComparingLong(Snapshot::getSnapshotId))
                .toList();
    Long maxSnapshotSequence = maxSnapshotSequence(orderedSnapshots);
    if (maxSnapshotSequence != null
        && (lastSequenceNumber == null || lastSequenceNumber < maxSnapshotSequence)) {
      lastSequenceNumber = maxSnapshotSequence;
    }
    Map<String, Object> refs = refs(metadata);
    refs = mergePropertyRefs(props, refs);
    syncProperty(props, "table-uuid", tableUuid);
    MetadataLocationUtil.setMetadataLocation(props, metadataLocation);
    syncProperty(props, "current-snapshot-id", currentSnapshotId);
    syncProperty(props, "last-sequence-number", lastSequenceNumber);
    syncProperty(props, "current-schema-id", currentSchemaId);
    syncProperty(props, "last-column-id", lastColumnId);
    syncProperty(props, "default-spec-id", defaultSpecId);
    syncProperty(props, "last-partition-id", lastPartitionId);
    syncProperty(props, "default-sort-order-id", defaultSortOrderId);
    return new TableMetadataView(
        formatVersion,
        tableUuid,
        location,
        metadataLocation,
        lastUpdatedMs,
        props,
        lastColumnId,
        currentSchemaId,
        defaultSpecId,
        lastPartitionId,
        defaultSortOrderId,
        currentSnapshotId,
        lastSequenceNumber,
        schemaList,
        specList,
        sortOrderList,
        refs,
        snapshotLog(metadata),
        metadataLog(metadata),
        statisticsList,
        partitionStatisticsList,
        snapshots(orderedSnapshots));
  }

  private static TableMetadataView initialMetadata(
      String tableName, Table table, TableRequests.Create request) {
    if (request == null) {
      throw new IllegalArgumentException("create request is required");
    }
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    if (request.properties() != null) {
      request
          .properties()
          .forEach(
              (k, v) -> {
                if (k != null && v != null && !"metadata-location".equals(k)) {
                  props.put(k, v);
                }
              });
    }
    String metadataLoc = metadataLocationFromRequest(request);
    String metadataLocation = null;
    if (metadataLoc != null && !metadataLoc.isBlank()) {
      MetadataLocationUtil.setMetadataLocation(props, metadataLoc);
      syncWriteMetadataPath(props, metadataLoc);
      metadataLocation = metadataLoc;
    }
    String location =
        Optional.ofNullable(request.location()).filter(s -> !s.isBlank()).orElse(null);
    long lastUpdatedMs;
    if (table.hasCreatedAt()) {
      lastUpdatedMs =
          table.getCreatedAt().getSeconds() * 1000 + table.getCreatedAt().getNanos() / 1_000_000;
    } else {
      Long updatedMs =
          request.properties() == null ? null : asLong(request.properties().get("last-updated-ms"));
      if (updatedMs == null || updatedMs <= 0) {
        lastUpdatedMs = System.currentTimeMillis();
      } else {
        lastUpdatedMs = updatedMs;
      }
    }
    Map<String, Object> schema = schemaFromRequest(request);
    Integer schemaId = asInteger(schema.get("schema-id"));
    Integer lastColumnId = asInteger(schema.get("last-column-id"));
    if (schemaId == null || lastColumnId == null) {
      throw new IllegalArgumentException("schema requires schema-id and last-column-id");
    }
    Map<String, Object> partitionSpec =
        request.partitionSpec() == null || request.partitionSpec().isNull()
            ? defaultPartitionSpec()
            : partitionSpecFromRequest(request);
    Integer defaultSpecId = asInteger(partitionSpec.get("spec-id"));
    if (defaultSpecId == null) {
      throw new IllegalArgumentException("partition-spec requires spec-id");
    }
    Integer lastPartitionId = maxPartitionFieldId(partitionSpec);
    Map<String, Object> sortOrder =
        request.writeOrder() == null || request.writeOrder().isNull()
            ? defaultSortOrder()
            : sortOrderFromRequest(request);
    Integer defaultSortOrderId = asInteger(sortOrder.get("order-id"));
    if (defaultSortOrderId == null) {
      throw new IllegalArgumentException("write-order requires sort-order-id");
    }
    Integer formatVersion = maybeInt(props.get("format-version"));
    if (formatVersion == null || formatVersion < 1) {
      formatVersion = 1;
    }
    props.putIfAbsent("format-version", formatVersion.toString());
    props.putIfAbsent("current-schema-id", schemaId.toString());
    props.putIfAbsent("last-column-id", lastColumnId.toString());
    props.putIfAbsent("default-spec-id", defaultSpecId.toString());
    props.putIfAbsent("last-partition-id", lastPartitionId.toString());
    props.putIfAbsent("default-sort-order-id", defaultSortOrderId.toString());
    long lastSequenceNumber = 0L;
    props.putIfAbsent("last-sequence-number", Long.toString(lastSequenceNumber));
    return new TableMetadataView(
        formatVersion,
        table.hasResourceId() ? table.getResourceId().getId() : tableName,
        resolveTableLocation(location, metadataLocation),
        metadataLocation,
        lastUpdatedMs,
        props,
        lastColumnId,
        schemaId,
        defaultSpecId,
        lastPartitionId,
        defaultSortOrderId,
        null,
        lastSequenceNumber,
        List.of(schema),
        List.of(partitionSpec),
        List.of(sortOrder),
        Map.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of());
  }

  private static Map<String, Object> defaultPartitionSpec() {
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("spec-id", 0);
    spec.put("fields", List.of());
    return spec;
  }

  private static Map<String, Object> defaultSortOrder() {
    Map<String, Object> order = new LinkedHashMap<>();
    order.put("order-id", 0);
    order.put("fields", List.of());
    return order;
  }

  private static String resolveMetadataLocation(IcebergMetadata metadata) {
    return metadataLocationFromField(metadata);
  }

  private static String metadataLocationFromField(IcebergMetadata metadata) {
    if (metadata == null) {
      return null;
    }
    String directLocation = metadata.getMetadataLocation();
    if (directLocation != null && !directLocation.isBlank()) {
      return directLocation;
    }
    return null;
  }

  private static String resolveTableLocation(String location, String metadataLocation) {
    if (location == null || location.isBlank()) {
      return null;
    }
    return location;
  }

  private static String metadataLocationFromRequest(TableRequests.Create request) {
    if (request == null || request.properties() == null || request.properties().isEmpty()) {
      return null;
    }
    String location = request.properties().get("metadata-location");
    return (location == null || location.isBlank()) ? null : location;
  }

  private static Map<String, Object> mergePropertyRefs(
      Map<String, String> props, Map<String, Object> refs) {
    if (props == null || props.isEmpty()) {
      return refs;
    }
    String encoded = props.remove(RefPropertyUtil.PROPERTY_KEY);
    Map<String, Map<String, Object>> stored = RefPropertyUtil.decode(encoded);
    if (stored.isEmpty()) {
      return refs;
    }
    Map<String, Object> merged =
        refs == null || refs.isEmpty() ? new LinkedHashMap<>() : new LinkedHashMap<>(refs);
    stored.forEach(
        (name, refMap) -> {
          if (refMap != null && !refMap.isEmpty()) {
            merged.put(name, new LinkedHashMap<>(refMap));
          }
        });
    return merged;
  }

  private static void syncProperty(Map<String, String> props, String key, Object value) {
    if (props == null || key == null || value == null) {
      return;
    }
    props.put(key, value.toString());
  }

  private static void syncWriteMetadataPath(Map<String, String> props, String metadataLocation) {
    String directory = MetadataLocationUtil.canonicalMetadataDirectory(metadataLocation);
    if (directory == null || directory.isBlank()) {
      return;
    }
    props.put("write.metadata.path", directory);
  }

  private static Long maxSnapshotSequence(List<Snapshot> snapshots) {
    if (snapshots == null || snapshots.isEmpty()) {
      return null;
    }
    long max = -1L;
    for (Snapshot snapshot : snapshots) {
      if (snapshot == null) {
        continue;
      }
      long seq = snapshot.getSequenceNumber();
      if (seq > max) {
        max = seq;
      }
    }
    return max > 0 ? max : null;
  }

  private static String nextMetadataFileName() {
    return String.format("%05d-%s.metadata.json", 0, UUID.randomUUID());
  }
}
