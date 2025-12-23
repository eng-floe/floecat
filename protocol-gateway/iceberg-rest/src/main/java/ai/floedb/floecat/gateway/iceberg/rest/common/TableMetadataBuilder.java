package ai.floedb.floecat.gateway.iceberg.rest.common;

import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.maxPartitionFieldId;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.normalizeSortOrder;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.partitionSpecFromRequest;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.partitionSpecsFromMetadata;
import static ai.floedb.floecat.gateway.iceberg.rest.common.SchemaMapper.schemaFromRequest;
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
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asObjectMap;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.firstNonNull;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.maybeInt;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.maybeLong;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    String location = table.hasUpstream() ? table.getUpstream().getUri() : props.get("location");
    location = resolveTableLocation(location, metadataLocation);
    Long lastUpdatedMs =
        (metadata != null && metadata.getLastUpdatedMs() > 0)
            ? Long.valueOf(metadata.getLastUpdatedMs())
            : maybeLong(props.get("last-updated-ms"));
    Long currentSnapshotId =
        metadata != null && metadata.getCurrentSnapshotId() > 0
            ? Long.valueOf(metadata.getCurrentSnapshotId())
            : maybeLong(props.get("current-snapshot-id"));
    Long lastSequenceNumber =
        metadata != null && metadata.getLastSequenceNumber() >= 0
            ? Long.valueOf(metadata.getLastSequenceNumber())
            : maybeLong(props.get("last-sequence-number"));
    Integer lastColumnId =
        metadata != null && metadata.getLastColumnId() >= 0
            ? Integer.valueOf(metadata.getLastColumnId())
            : maybeInt(props.get("last-column-id"));
    Integer currentSchemaId =
        metadata != null && metadata.getCurrentSchemaId() >= 0
            ? Integer.valueOf(metadata.getCurrentSchemaId())
            : maybeInt(props.get("current-schema-id"));
    Integer defaultSpecId =
        metadata != null && metadata.getDefaultSpecId() >= 0
            ? Integer.valueOf(metadata.getDefaultSpecId())
            : maybeInt(props.get("default-spec-id"));
    Integer lastPartitionId =
        metadata != null && metadata.getLastPartitionId() >= 0
            ? Integer.valueOf(metadata.getLastPartitionId())
            : maybeInt(props.get("last-partition-id"));
    Integer defaultSortOrderId =
        metadata != null && metadata.getDefaultSortOrderId() >= 0
            ? Integer.valueOf(metadata.getDefaultSortOrderId())
            : maybeInt(props.get("default-sort-order-id"));
    String tableUuid =
        metadata != null && metadata.getTableUuid() != null && !metadata.getTableUuid().isBlank()
            ? metadata.getTableUuid()
            : props.get("table-uuid");
    Integer formatVersion =
        metadata != null && metadata.getFormatVersion() > 0
            ? Integer.valueOf(metadata.getFormatVersion())
            : maybeInt(props.get("format-version"));
    List<Map<String, Object>> schemaList = schemasFromMetadata(metadata);
    List<Map<String, Object>> specList = partitionSpecsFromMetadata(metadata);
    List<Map<String, Object>> sortOrderList = sortOrdersFromMetadata(metadata);
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
    Snapshot latestSnapshot = null;
    if (orderedSnapshots != null && !orderedSnapshots.isEmpty()) {
      latestSnapshot =
          orderedSnapshots.stream()
              .max(
                  Comparator.comparingLong(Snapshot::getSequenceNumber)
                      .thenComparingLong(Snapshot::getSnapshotId))
              .orElse(null);
      if (latestSnapshot != null) {
        long latestSequence = latestSnapshot.getSequenceNumber();
        if (lastSequenceNumber == null || latestSequence > lastSequenceNumber) {
          lastSequenceNumber = latestSequence;
        }
      }
    }
    Map<String, Object> refs = refs(metadata);
    refs = mergePropertyRefs(props, refs);
    if (metadata != null && !refs.isEmpty()) {
      Set<Long> snapshotIds = new HashSet<>();
      if (orderedSnapshots != null) {
        snapshotIds.addAll(
            orderedSnapshots.stream().map(Snapshot::getSnapshotId).collect(Collectors.toSet()));
      }
      if (!snapshotIds.isEmpty()) {
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
      }
      Map<String, Object> mainRef = asObjectMap(refs.get("main"));
      if (mainRef != null) {
        Long mainSnapshot = asLong(mainRef.get("snapshot-id"));
        if (mainSnapshot != null && mainSnapshot > 0) {
          currentSnapshotId = mainSnapshot;
        }
      }
    }
    if (latestSnapshot != null) {
      long latestSnapshotId = latestSnapshot.getSnapshotId();
      if (latestSnapshotId > 0 && !Objects.equals(currentSnapshotId, latestSnapshotId)) {
        currentSnapshotId = latestSnapshotId;
        refs = ensureMainRef(refs, latestSnapshotId);
      }
    } else if ((currentSnapshotId == null || currentSnapshotId <= 0)
        && snapshots != null
        && !snapshots.isEmpty()) {
      currentSnapshotId = snapshots.get(0).getSnapshotId();
    }
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
    MetadataLocationUtil.setMetadataLocation(props, metadataLoc);
    syncWriteMetadataPath(props, metadataLoc);
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
    String metadataLocation = metadataLocationFromMetadataLog(metadata);
    if (metadataLocation == null || metadataLocation.isBlank()) {
      metadataLocation = metadataLocationFromField(metadata);
    }
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      return metadataLocation;
    }
    String propertyLocation = MetadataLocationUtil.metadataLocation(props);
    if (propertyLocation != null && !propertyLocation.isBlank()) {
      return propertyLocation;
    }
    return propertyLocation;
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

  private static String metadataLocationFromMetadataLog(IcebergMetadata metadata) {
    if (metadata == null || metadata.getMetadataLogCount() == 0) {
      return null;
    }
    var entries = metadata.getMetadataLogList();
    for (int idx = entries.size() - 1; idx >= 0; idx--) {
      String candidate = entries.get(idx).getFile();
      if (candidate != null && !candidate.isBlank()) {
        return candidate;
      }
    }
    return null;
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

  private static Map<String, Object> ensureMainRef(Map<String, Object> refs, long snapshotId) {
    Map<String, Object> updated =
        (refs == null || refs.isEmpty()) ? new LinkedHashMap<>() : new LinkedHashMap<>(refs);
    Map<String, Object> mainRef = asObjectMap(updated.get("main"));
    Map<String, Object> newMain =
        mainRef == null ? new LinkedHashMap<>() : new LinkedHashMap<>(mainRef);
    newMain.put("snapshot-id", snapshotId);
    newMain.putIfAbsent("type", "branch");
    updated.put("main", newMain);
    return updated;
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

  private static String nextMetadataFileName() {
    return String.format("%05d-%s.metadata.json", 0, UUID.randomUUID());
  }
}
