package ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table;

import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SchemaMapper.defaultPartitionSpec;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SchemaMapper.defaultSortOrder;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SchemaMapper.maxPartitionFieldId;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SchemaMapper.partitionSpecFromRequest;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SchemaMapper.partitionSpecsFromMetadata;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SchemaMapper.schemaFromRequest;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SchemaMapper.schemaFromTable;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SchemaMapper.schemasFromMetadata;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SchemaMapper.sortOrderFromRequest;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SchemaMapper.sortOrdersFromMetadata;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SnapshotMapper.metadataLog;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SnapshotMapper.nonNullMapList;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SnapshotMapper.partitionStatistics;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SnapshotMapper.refs;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SnapshotMapper.sanitizeStatistics;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SnapshotMapper.snapshotLog;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SnapshotMapper.snapshots;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.SnapshotMapper.statistics;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.TableMappingUtil.asInteger;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.TableMappingUtil.asLong;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.TableMappingUtil.asObjectMap;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.TableMappingUtil.firstNonNull;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.TableMappingUtil.maybeInt;
import static ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table.TableMappingUtil.maybeLong;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public final class TableMetadataBuilder {
  private TableMetadataBuilder() {}

  public static TableMetadataView fromCatalog(
      String tableName,
      Table table,
      Map<String, String> props,
      IcebergMetadata metadata,
      List<Snapshot> snapshots) {
    String metadataLocation = resolveMetadataLocation(props, metadata);
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
    if (metadata == null) {
      return synthesizeMetadataFromTable(tableName, table, props, metadataLocation, snapshots);
    }
    String location = table.hasUpstream() ? table.getUpstream().getUri() : props.get("location");
    location = resolveTableLocation(location, metadataLocation);
    Long lastUpdatedMs =
        table.hasCreatedAt()
            ? table.getCreatedAt().getSeconds() * 1000 + table.getCreatedAt().getNanos() / 1_000_000
            : Instant.now().toEpochMilli();
    Long currentSnapshotId =
        metadata.getCurrentSnapshotId() > 0
            ? Long.valueOf(metadata.getCurrentSnapshotId())
            : maybeLong(props.get("current-snapshot-id"));
    Long lastSequenceNumber =
        metadata != null
            ? Long.valueOf(metadata.getLastSequenceNumber())
            : maybeLong(props.get("last-sequence-number"));
    Integer lastColumnId =
        metadata.getLastColumnId() >= 0
            ? metadata.getLastColumnId()
            : maybeInt(props.get("last-column-id"));
    Integer currentSchemaId =
        metadata.getCurrentSchemaId() >= 0
            ? metadata.getCurrentSchemaId()
            : maybeInt(props.get("current-schema-id"));
    Integer defaultSpecId =
        metadata.getDefaultSpecId() >= 0
            ? metadata.getDefaultSpecId()
            : maybeInt(props.get("default-spec-id"));
    Integer lastPartitionId =
        metadata.getLastPartitionId() >= 0
            ? metadata.getLastPartitionId()
            : maybeInt(props.get("last-partition-id"));
    Integer defaultSortOrderId =
        metadata.getDefaultSortOrderId() >= 0
            ? metadata.getDefaultSortOrderId()
            : maybeInt(props.get("default-sort-order-id"));
    String tableUuid =
        (metadata != null && metadata.getTableUuid() != null && !metadata.getTableUuid().isBlank())
            ? metadata.getTableUuid()
            : Optional.ofNullable(props.get("table-uuid"))
                .orElseGet(() -> table.hasResourceId() ? table.getResourceId().getId() : tableName);
    if (lastColumnId == null) {
      lastColumnId = 0;
    }
    if (currentSchemaId == null) {
      currentSchemaId = 0;
    }
    if (defaultSpecId == null) {
      defaultSpecId = 0;
    }
    if (lastPartitionId == null) {
      lastPartitionId = 0;
    }
    if (defaultSortOrderId == null) {
      defaultSortOrderId = 0;
    }
    if (lastSequenceNumber == null) {
      lastSequenceNumber = 0L;
    }
    Integer formatVersion = formatVersion(props);
    List<Map<String, Object>> schemaList = schemasFromMetadata(metadata);
    if (schemaList.isEmpty()) {
      Map<String, Object> fallback = schemaFromTable(table);
      schemaList = List.of(fallback);
      Integer fallbackId = asInteger(fallback.get("schema-id"));
      if (fallbackId != null && fallbackId >= 0) {
        currentSchemaId = fallbackId;
      }
    }
    List<Map<String, Object>> specList = partitionSpecsFromMetadata(metadata);
    if (specList.isEmpty()) {
      Map<String, Object> fallbackSpec = defaultPartitionSpec();
      specList = List.of(fallbackSpec);
      Integer fallbackSpecId = asInteger(fallbackSpec.get("spec-id"));
      if (fallbackSpecId != null && fallbackSpecId >= 0) {
        defaultSpecId = fallbackSpecId;
      }
    }
    List<Map<String, Object>> sortOrderList = sortOrdersFromMetadata(metadata);
    if (sortOrderList.isEmpty()) {
      Map<String, Object> fallbackOrder = defaultSortOrder();
      sortOrderList = List.of(fallbackOrder);
      Integer fallbackOrderId = asInteger(fallbackOrder.get("sort-order-id"));
      if (fallbackOrderId != null && fallbackOrderId >= 0) {
        defaultSortOrderId = fallbackOrderId;
      }
    }
    List<Map<String, Object>> statisticsList = sanitizeStatistics(statistics(metadata));
    List<Map<String, Object>> partitionStatisticsList =
        nonNullMapList(partitionStatistics(metadata));
    if (snapshots != null && !snapshots.isEmpty()) {
      long maxSequence = snapshots.stream().mapToLong(Snapshot::getSequenceNumber).max().orElse(0L);
      if (maxSequence > lastSequenceNumber) {
        lastSequenceNumber = maxSequence;
      }
    }
    Map<String, Object> refs = refs(metadata);
    if (metadata != null && !refs.isEmpty()) {
      Set<Long> snapshotIds =
          snapshots.stream().map(Snapshot::getSnapshotId).collect(Collectors.toSet());
      refs.entrySet()
          .removeIf(
              entry -> {
                Map<String, Object> refMap = asObjectMap(entry.getValue());
                if (refMap == null) {
                  return true;
                }
                Long refSnapshot = asLong(refMap.get("snapshot-id"));
                return refSnapshot == null || !snapshotIds.contains(refSnapshot);
              });
      Map<String, Object> mainRef = asObjectMap(refs.get("main"));
      if (mainRef != null) {
        Long mainSnapshot = asLong(mainRef.get("snapshot-id"));
        if (mainSnapshot != null && mainSnapshot > 0) {
          currentSnapshotId = mainSnapshot;
        }
      }
    }
    if ((currentSnapshotId == null || currentSnapshotId <= 0)
        && snapshots != null
        && !snapshots.isEmpty()) {
      currentSnapshotId = snapshots.get(0).getSnapshotId();
    }
    syncProperty(props, "table-uuid", tableUuid);
    syncDualProperty(props, "metadata-location", metadataLocation);
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
        snapshots(snapshots));
  }

  private static TableMetadataView synthesizeMetadataFromTable(
      String tableName,
      Table table,
      Map<String, String> props,
      String metadataLocation,
      List<Snapshot> snapshots) {
    String resolvedMetadata =
        resolveInitialMetadataLocation(tableName, table, props, metadataLocation);
    props.put("metadata-location", resolvedMetadata);
    props.put("metadata_location", resolvedMetadata);
    Map<String, Object> schema = schemaFromTable(table);
    Integer schemaId = asInteger(schema.get("schema-id"));
    Integer lastColumnId = asInteger(schema.get("last-column-id"));
    Map<String, Object> partitionSpec = defaultPartitionSpec();
    Integer defaultSpecId = asInteger(partitionSpec.get("spec-id"));
    Integer lastPartitionId = 0;
    Map<String, Object> sortOrder = defaultSortOrder();
    Integer defaultSortOrderId = asInteger(sortOrder.get("sort-order-id"));
    String location =
        defaultLocation(
            table.hasUpstream() ? table.getUpstream().getUri() : props.get("location"),
            resolvedMetadata);
    long lastUpdatedMs =
        table.hasCreatedAt()
            ? table.getCreatedAt().getSeconds() * 1000 + table.getCreatedAt().getNanos() / 1_000_000
            : Instant.now().toEpochMilli();
    props.putIfAbsent("format-version", "2");
    props.putIfAbsent("current-schema-id", schemaId.toString());
    props.putIfAbsent("last-column-id", lastColumnId.toString());
    props.putIfAbsent("default-spec-id", defaultSpecId.toString());
    props.putIfAbsent("last-partition-id", Integer.toString(lastPartitionId));
    props.putIfAbsent("default-sort-order-id", defaultSortOrderId.toString());
    long maxSequence =
        snapshots == null || snapshots.isEmpty()
            ? 0L
            : snapshots.stream().mapToLong(Snapshot::getSequenceNumber).max().orElse(0L);
    long currentSnapshotId =
        snapshots == null || snapshots.isEmpty() ? -1L : snapshots.get(0).getSnapshotId();
    props.put("current-snapshot-id", Long.toString(currentSnapshotId < 0 ? 0 : currentSnapshotId));
    props.put("last-sequence-number", Long.toString(maxSequence));
    return new TableMetadataView(
        formatVersion(props),
        table.hasResourceId() ? table.getResourceId().getId() : tableName,
        location,
        resolvedMetadata,
        lastUpdatedMs,
        props,
        lastColumnId,
        schemaId,
        defaultSpecId,
        lastPartitionId,
        defaultSortOrderId,
        currentSnapshotId,
        maxSequence,
        List.of(schema),
        List.of(partitionSpec),
        List.of(sortOrder),
        Map.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        snapshots(snapshots));
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
                if (k != null
                    && v != null
                    && !"metadata-location".equals(k)
                    && !"metadata_location".equals(k)) {
                  props.put(k, v);
                }
              });
    }
    String locationOverride =
        Optional.ofNullable(request.location())
            .filter(s -> !s.isBlank())
            .orElseGet(() -> props.get("location"));
    String metadataLoc = metadataLocationFromRequest(request);
    if (metadataLoc == null || metadataLoc.isBlank()) {
      metadataLoc = metadataLocationFromTableLocation(locationOverride);
    }
    if (metadataLoc == null || metadataLoc.isBlank()) {
      metadataLoc = defaultMetadataLocation(table, tableName);
    }
    props.put("metadata-location", metadataLoc);
    props.put("metadata_location", metadataLoc);
    final String metadataLocation = metadataLoc;
    String location =
        Optional.ofNullable(request.location())
            .filter(s -> !s.isBlank())
            .orElseGet(() -> defaultLocation(props.get("location"), metadataLocation));
    if (location == null || location.isBlank()) {
      throw new IllegalArgumentException("location is required");
    }
    long lastUpdatedMs =
        table.hasCreatedAt()
            ? table.getCreatedAt().getSeconds() * 1000 + table.getCreatedAt().getNanos() / 1_000_000
            : Instant.now().toEpochMilli();
    Map<String, Object> schema = schemaFromRequest(request);
    Integer schemaId = asInteger(schema.get("schema-id"));
    Integer lastColumnId = asInteger(schema.get("last-column-id"));
    if (schemaId == null || lastColumnId == null) {
      throw new IllegalArgumentException("schema requires schema-id and last-column-id");
    }
    Map<String, Object> partitionSpec = partitionSpecFromRequest(request);
    Integer defaultSpecId = asInteger(partitionSpec.get("spec-id"));
    if (defaultSpecId == null) {
      defaultSpecId = 0;
      partitionSpec.put("spec-id", defaultSpecId);
    }
    Integer lastPartitionId = maxPartitionFieldId(partitionSpec);
    Map<String, Object> sortOrder = sortOrderFromRequest(request);
    Integer defaultSortOrderId =
        asInteger(firstNonNull(sortOrder.get("sort-order-id"), sortOrder.get("order-id")));
    if (defaultSortOrderId == null) {
      defaultSortOrderId = 0;
      sortOrder.put("sort-order-id", defaultSortOrderId);
    }
    props.putIfAbsent("format-version", "2");
    props.putIfAbsent("current-schema-id", schemaId.toString());
    props.putIfAbsent("last-column-id", lastColumnId.toString());
    props.putIfAbsent("default-spec-id", defaultSpecId.toString());
    props.putIfAbsent("last-partition-id", lastPartitionId.toString());
    props.putIfAbsent("default-sort-order-id", defaultSortOrderId.toString());
    props.putIfAbsent("current-snapshot-id", "0");
    props.putIfAbsent("last-sequence-number", "0");
    return new TableMetadataView(
        formatVersion(props),
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
        0L,
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

  private static Integer formatVersion(Map<String, String> props) {
    Integer version = maybeInt(props.get("format-version"));
    if (version == null || version < 1) {
      version = 2;
    }
    return version;
  }

  private static String resolveMetadataLocation(
      Map<String, String> props, IcebergMetadata metadata) {
    if (metadata != null && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    return props.getOrDefault("metadata-location", props.get("metadata_location"));
  }

  private static String resolveInitialMetadataLocation(
      String tableName, Table table, Map<String, String> props, String metadataLocation) {
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      return metadataLocation;
    }
    String tableLocation =
        table.hasUpstream() ? table.getUpstream().getUri() : props.get("location");
    if (tableLocation != null && !tableLocation.isBlank()) {
      String base =
          tableLocation.endsWith("/")
              ? tableLocation.substring(0, tableLocation.length() - 1)
              : tableLocation;
      return base + "/metadata/" + nextMetadataFileName();
    }
    return defaultMetadataLocation(table, tableName);
  }

  private static String resolveTableLocation(String location, String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return location;
    }
    if (location != null && !location.isBlank() && !location.startsWith("https://glue")) {
      return location;
    }
    int idx = metadataLocation.indexOf("/metadata/");
    if (idx > 0) {
      return metadataLocation.substring(0, idx);
    }
    int slash = metadataLocation.lastIndexOf('/');
    if (slash > 0) {
      return metadataLocation.substring(0, slash);
    }
    return metadataLocation;
  }

  private static String metadataLocationFromRequest(TableRequests.Create request) {
    if (request == null || request.properties() == null || request.properties().isEmpty()) {
      return null;
    }
    String location = request.properties().get("metadata-location");
    if (location == null || location.isBlank()) {
      location = request.properties().get("metadata_location");
    }
    return (location == null || location.isBlank()) ? null : location;
  }

  private static String metadataLocationFromTableLocation(String tableLocation) {
    if (tableLocation == null || tableLocation.isBlank()) {
      return null;
    }
    String base =
        tableLocation.endsWith("/")
            ? tableLocation.substring(0, tableLocation.length() - 1)
            : tableLocation;
    String dir = base + "/metadata/";
    return dir + nextMetadataFileName();
  }

  private static String defaultMetadataLocation(Table table, String tableName) {
    String suffix;
    if (table != null && table.hasResourceId() && table.getResourceId().getId() != null) {
      suffix = table.getResourceId().getId();
    } else {
      suffix = tableName;
    }
    return "floecat:///tables/" + suffix + "/metadata/" + nextMetadataFileName();
  }

  private static String defaultLocation(String current, String metadataLocation) {
    if (current != null && !current.isBlank()) {
      return current;
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return null;
    }
    int idx = metadataLocation.indexOf("/metadata/");
    if (idx > 0) {
      return metadataLocation.substring(0, idx);
    }
    int slash = metadataLocation.lastIndexOf('/');
    if (slash > 0) {
      return metadataLocation.substring(0, slash);
    }
    return metadataLocation;
  }

  private static void syncDualProperty(Map<String, String> props, String key, Object value) {
    if (key == null) {
      return;
    }
    syncProperty(props, key, value);
    if (key.indexOf('-') >= 0) {
      syncProperty(props, key.replace('-', '_'), value);
    }
  }

  private static void syncProperty(Map<String, String> props, String key, Object value) {
    if (props == null || key == null || value == null) {
      return;
    }
    props.put(key, value.toString());
  }

  private static String nextMetadataFileName() {
    return String.format("%05d-%s.metadata.json", 0, UUID.randomUUID());
  }
}
