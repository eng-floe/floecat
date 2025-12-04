package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.IcebergBlobMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergMetadataLogEntry;
import ai.floedb.metacat.catalog.rpc.IcebergPartitionStatisticsFile;
import ai.floedb.metacat.catalog.rpc.IcebergSchema;
import ai.floedb.metacat.catalog.rpc.IcebergSnapshotLogEntry;
import ai.floedb.metacat.catalog.rpc.IcebergSortField;
import ai.floedb.metacat.catalog.rpc.IcebergSortOrder;
import ai.floedb.metacat.catalog.rpc.IcebergStatisticsFile;
import ai.floedb.metacat.catalog.rpc.PartitionField;
import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

final class TableResponseMapper {
  private static final ObjectMapper JSON = new ObjectMapper();

  private TableResponseMapper() {}

  static LoadTableResultDto toLoadResult(
      String tableName,
      Table table,
      IcebergMetadata metadata,
      List<Snapshot> snapshots,
      Map<String, String> configOverrides,
      List<StorageCredentialDto> storageCredentials) {
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    String metadataLocation = resolveMetadataLocation(props, metadata);
    TableMetadataView metadataView =
        toMetadata(tableName, table, props, metadata, snapshots, metadataLocation);
    return new LoadTableResultDto(
        metadataLocation, metadataView, configOverrides, storageCredentials);
  }

  static CommitTableResponseDto toCommitResponse(
      String tableName, Table table, IcebergMetadata metadata, List<Snapshot> snapshots) {
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    String metadataLocation = resolveMetadataLocation(props, metadata);
    return new CommitTableResponseDto(
        metadataLocation,
        toMetadata(tableName, table, props, metadata, snapshots, metadataLocation));
  }

  static LoadTableResultDto toLoadResultFromCreate(
      String tableName,
      Table table,
      TableRequests.Create request,
      Map<String, String> configOverrides,
      List<StorageCredentialDto> storageCredentials) {
    if (request == null) {
      throw new IllegalArgumentException("create request is required");
    }
    TableMetadataView metadataView = initialMetadata(tableName, table, request);
    return new LoadTableResultDto(
        metadataView.metadataLocation(), metadataView, configOverrides, storageCredentials);
  }

  private static TableMetadataView initialMetadata(
      String tableName, Table table, TableRequests.Create request) {
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
    Map<String, Object> schema = schemaMap(request);
    Integer schemaId = asInteger(schema.get("schema-id"));
    Integer lastColumnId = asInteger(schema.get("last-column-id"));
    if (schemaId == null || lastColumnId == null) {
      throw new IllegalArgumentException("schema requires schema-id and last-column-id");
    }
    Map<String, Object> partitionSpec = partitionSpecMap(request);
    Integer defaultSpecId = asInteger(partitionSpec.get("spec-id"));
    if (defaultSpecId == null) {
      defaultSpecId = 0;
      partitionSpec.put("spec-id", defaultSpecId);
    }
    Integer lastPartitionId = maxPartitionFieldId(partitionSpec);
    Map<String, Object> sortOrder = sortOrderMap(request);
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

  private static Map<String, Object> schemaMap(TableRequests.Create request) {
    JsonNode node = request.schema();
    if (node == null || node.isNull()) {
      String schemaJson = request.schemaJson();
      if (schemaJson != null && !schemaJson.isBlank()) {
        try {
          node = JSON.readTree(schemaJson);
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException("schemaJson is invalid", e);
        }
      }
    }
    if (node == null || node.isNull()) {
      throw new IllegalArgumentException("schema is required");
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("schema must be an object");
    }
    Map<String, Object> schema =
        new LinkedHashMap<>(JSON.convertValue(node, new TypeReference<Map<String, Object>>() {}));
    try {
      normalizeSchema(schema);
      return schema;
    } catch (IllegalArgumentException e) {
      return defaultTableSchema();
    }
  }

  private static TableMetadataView synthesizeMetadataFromTable(
      String tableName, Table table, Map<String, String> props, String metadataLocation) {
    return synthesizeMetadataFromTable(tableName, table, props, metadataLocation, List.of());
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
      return base + "/metadata/" + String.format("%05d-%s.metadata.json", 0, UUID.randomUUID());
    }
    return defaultMetadataLocation(table, tableName);
  }

  private static Map<String, Object> schemaFromTable(Table table) {
    String schemaJson = table.getSchemaJson();
    if (schemaJson == null || schemaJson.isBlank()) {
      return defaultTableSchema();
    }
    JsonNode node;
    try {
      node = JSON.readTree(schemaJson);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("schemaJson is invalid", e);
    }
    if (node == null || !node.isObject()) {
      throw new IllegalArgumentException("schemaJson must be an object");
    }
    Map<String, Object> schema =
        new LinkedHashMap<>(JSON.convertValue(node, new TypeReference<Map<String, Object>>() {}));
    try {
      normalizeSchema(schema);
      return schema;
    } catch (IllegalArgumentException e) {
      return defaultTableSchema();
    }
  }

  private static Map<String, Object> defaultTableSchema() {
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("schema-id", 0);
    schema.put("last-column-id", 1);
    schema.put("type", "struct");
    Map<String, Object> field = new LinkedHashMap<>();
    field.put("id", 1);
    field.put("name", "placeholder");
    field.put("type", "string");
    field.put("required", false);
    schema.put("fields", new ArrayList<>(List.of(field)));
    normalizeSchema(schema);
    return schema;
  }

  private static Map<String, Object> defaultPartitionSpec() {
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("spec-id", 0);
    spec.put("fields", List.of());
    return spec;
  }

  private static Map<String, Object> defaultSortOrder() {
    Map<String, Object> order = new LinkedHashMap<>();
    order.put("sort-order-id", 0);
    order.put("order-id", 0);
    order.put("fields", List.of());
    normalizeSortOrder(order);
    return order;
  }

  private static Map<String, Object> asObjectMap(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return null;
    }
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      out.put(entry.getKey().toString(), entry.getValue());
    }
    return out;
  }

  private static Long asLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    String text = value.toString();
    if (text.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException e) {
      return null;
    }
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

  private static void normalizeSchema(Map<String, Object> schema) {
    List<Map<String, Object>> fields = normalizeSchemaFields(schema);
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("schema.fields is required");
    }
    int nextFieldId = 1;
    int maxFieldId = 0;
    for (Map<String, Object> field : fields) {
      Object fieldIdSource =
          firstNonNull(
              field.get("id"), firstNonNull(field.get("field-id"), field.get("source-id")));
      Integer fieldId = asInteger(fieldIdSource);
      if (fieldId == null || fieldId <= 0) {
        fieldId = nextFieldId++;
      } else if (fieldId >= nextFieldId) {
        nextFieldId = fieldId + 1;
      }
      maxFieldId = Math.max(maxFieldId, fieldId);
      field.put("id", fieldId);
    }
    Integer schemaId = asInteger(schema.get("schema-id"));
    if (schemaId == null || schemaId < 0) {
      schemaId = 0;
      schema.put("schema-id", schemaId);
    }
    Integer lastColumnId = asInteger(schema.get("last-column-id"));
    if (lastColumnId == null || lastColumnId < maxFieldId) {
      lastColumnId = maxFieldId;
      schema.put("last-column-id", lastColumnId);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> normalizeSchemaFields(Map<String, Object> schema) {
    Object rawFields = schema.get("fields");
    if (!(rawFields instanceof List<?> list)) {
      throw new IllegalArgumentException("schema.fields is required");
    }
    List<Map<String, Object>> normalized = new ArrayList<>();
    for (Object entry : list) {
      if (!(entry instanceof Map<?, ?> fieldMap)) {
        throw new IllegalArgumentException("schema.fields entries must be objects");
      }
      Map<String, Object> mutable = new LinkedHashMap<>();
      for (Map.Entry<?, ?> e : fieldMap.entrySet()) {
        if (e.getKey() != null) {
          mutable.put(e.getKey().toString(), e.getValue());
        }
      }
      normalized.add(mutable);
    }
    schema.put("fields", normalized);
    return normalized;
  }

  private static Map<String, Object> partitionSpecMap(TableRequests.Create request) {
    JsonNode node = request.partitionSpec();
    if (node == null || node.isNull()) {
      Map<String, Object> defaults = new LinkedHashMap<>();
      defaults.put("spec-id", 0);
      defaults.put("fields", List.of());
      return defaults;
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("partition-spec must be an object");
    }
    return new LinkedHashMap<>(
        JSON.convertValue(node, new TypeReference<Map<String, Object>>() {}));
  }

  private static Map<String, Object> sortOrderMap(TableRequests.Create request) {
    JsonNode node = request.writeOrder();
    if (node == null || node.isNull()) {
      Map<String, Object> defaults = new LinkedHashMap<>();
      defaults.put("sort-order-id", 0);
      defaults.put("fields", List.of());
      normalizeSortOrder(defaults);
      return defaults;
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("write-order must be an object");
    }
    Map<String, Object> order =
        new LinkedHashMap<>(JSON.convertValue(node, new TypeReference<Map<String, Object>>() {}));
    normalizeSortOrder(order);
    return order;
  }

  @SuppressWarnings("unchecked")
  private static Integer maxPartitionFieldId(Map<String, Object> spec) {
    Object fields = spec.get("fields");
    if (!(fields instanceof List<?> list)) {
      return 0;
    }
    int max = 0;
    for (Object entry : list) {
      if (entry instanceof Map<?, ?> map) {
        Integer fieldId = asInteger(map.get("field-id"));
        if (fieldId == null) {
          fieldId = asInteger(map.get("source-id"));
        }
        if (fieldId != null && fieldId > max) {
          max = fieldId;
        }
      }
    }
    return max;
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

  private static String defaultMetadataLocation(Table table, String tableName) {
    String suffix;
    if (table != null && table.hasResourceId() && table.getResourceId().getId() != null) {
      suffix = table.getResourceId().getId();
    } else {
      suffix = tableName;
    }
    return "metacat:///tables/" + suffix + "/metadata/" + nextMetadataFileName();
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

  private static String nextMetadataFileName() {
    return String.format("%05d-%s.metadata.json", 0, UUID.randomUUID());
  }

  private static TableMetadataView toMetadata(
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
        metadata != null && metadata.getCurrentSnapshotId() > 0
            ? Long.valueOf(metadata.getCurrentSnapshotId())
            : maybeLong(props.get("current-snapshot-id"));
    Long lastSequenceNumber =
        metadata != null
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
        Optional.ofNullable(props.get("table-uuid"))
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
    List<Map<String, Object>> schemaList = schemas(metadata);
    if (schemaList.isEmpty()) {
      Map<String, Object> fallback = schemaFromTable(table);
      schemaList = List.of(fallback);
      Integer fallbackId = asInteger(fallback.get("schema-id"));
      if (fallbackId != null && fallbackId >= 0) {
        currentSchemaId = fallbackId;
      }
    }
    List<Map<String, Object>> specList = specs(metadata);
    if (specList.isEmpty()) {
      Map<String, Object> fallbackSpec = defaultPartitionSpec();
      specList = List.of(fallbackSpec);
      Integer fallbackSpecId = asInteger(fallbackSpec.get("spec-id"));
      if (fallbackSpecId != null && fallbackSpecId >= 0) {
        defaultSpecId = fallbackSpecId;
      }
    }
    List<Map<String, Object>> sortOrderList = sortOrders(metadata);
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
    var refs = refs(metadata);
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

  private static List<Map<String, Object>> schemas(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergSchema schema : metadata.getSchemasList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("schema-id", schema.getSchemaId());
      Object schemaObj = parseSchema(schema.getSchemaJson());
      if (schemaObj instanceof Map<?, ?> mapObj) {
        entry.putAll((Map<? extends String, ?>) mapObj);
      }
      if (schema.getIdentifierFieldIdsCount() > 0) {
        entry.put("identifier-field-ids", schema.getIdentifierFieldIdsList());
      }
      if (schema.getLastColumnId() > 0) {
        entry.put("last-column-id", schema.getLastColumnId());
      }
      out.add(entry);
    }
    return out;
  }

  private static Object parseSchema(String json) {
    if (json == null || json.isBlank()) {
      return Map.of();
    }
    try {
      return JSON.readValue(json, Object.class);
    } catch (JsonProcessingException e) {
      return Map.of();
    }
  }

  private static List<Map<String, Object>> specs(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (PartitionSpecInfo spec : metadata.getPartitionSpecsList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("spec-id", spec.getSpecId());
      if (!spec.getSpecName().isBlank()) {
        entry.put("spec-name", spec.getSpecName());
      }
      List<Map<String, Object>> fields = new ArrayList<>();
      for (PartitionField field : spec.getFieldsList()) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("field-id", field.getFieldId());
        f.put("name", field.getName());
        f.put("transform", field.getTransform());
        fields.add(f);
      }
      entry.put("fields", fields);
      out.add(entry);
    }
    return out;
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

  private static List<Map<String, Object>> sortOrders(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergSortOrder order : metadata.getSortOrdersList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("order-id", order.getSortOrderId());
      List<Map<String, Object>> fields = new ArrayList<>();
      for (IcebergSortField field : order.getFieldsList()) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("source-field-id", field.getSourceFieldId());
        f.put("transform", field.getTransform());
        f.put("direction", field.getDirection());
        f.put("null-order", field.getNullOrder());
        fields.add(f);
      }
      entry.put("fields", fields);
      out.add(entry);
    }
    return out;
  }

  private static Map<String, Object> refs(IcebergMetadata metadata) {
    if (metadata == null || metadata.getRefsCount() == 0) {
      return Map.of();
    }
    Map<String, Object> out = new LinkedHashMap<>();
    metadata
        .getRefsMap()
        .forEach(
            (name, ref) -> {
              Map<String, Object> entry = new LinkedHashMap<>();
              entry.put("snapshot-id", ref.getSnapshotId());
              entry.put("type", ref.getType());
              if (ref.hasMaxReferenceAgeMs()) {
                entry.put("max-reference-age-ms", ref.getMaxReferenceAgeMs());
              }
              if (ref.hasMaxSnapshotAgeMs()) {
                entry.put("max-snapshot-age-ms", ref.getMaxSnapshotAgeMs());
              }
              if (ref.hasMinSnapshotsToKeep()) {
                entry.put("min-snapshots-to-keep", ref.getMinSnapshotsToKeep());
              }
              out.put(name, entry);
            });
    return out;
  }

  private static List<Map<String, Object>> snapshotLog(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergSnapshotLogEntry entry : metadata.getSnapshotLogList()) {
      Map<String, Object> log = new LinkedHashMap<>();
      log.put("timestamp-ms", entry.getTimestampMs());
      log.put("snapshot-id", entry.getSnapshotId());
      out.add(log);
    }
    return out;
  }

  private static List<Map<String, Object>> metadataLog(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergMetadataLogEntry entry : metadata.getMetadataLogList()) {
      Map<String, Object> log = new LinkedHashMap<>();
      log.put("timestamp-ms", entry.getTimestampMs());
      log.put("metadata-file", entry.getFile());
      out.add(log);
    }
    return out;
  }

  private static List<Map<String, Object>> statistics(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergStatisticsFile file : metadata.getStatisticsList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("snapshot-id", file.getSnapshotId());
      entry.put("statistics-path", file.getStatisticsPath());
      entry.put("file-size-in-bytes", file.getFileSizeInBytes());
      entry.put("file-footer-size-in-bytes", file.getFileFooterSizeInBytes());
      if (!file.getBlobMetadataList().isEmpty()) {
        List<Map<String, Object>> blobs = new ArrayList<>();
        for (IcebergBlobMetadata blob : file.getBlobMetadataList()) {
          Map<String, Object> blobEntry = new LinkedHashMap<>();
          blobEntry.put("type", blob.getType());
          blobEntry.put("snapshot-id", blob.getSnapshotId());
          blobEntry.put("sequence-number", blob.getSequenceNumber());
          blobEntry.put("fields", blob.getFieldsList());
          if (!blob.getPropertiesMap().isEmpty()) {
            blobEntry.put("properties", blob.getPropertiesMap());
          }
          blobs.add(blobEntry);
        }
        entry.put("blob-metadata", blobs);
      }
      out.add(entry);
    }
    return out;
  }

  private static List<Map<String, Object>> sanitizeStatistics(
      List<Map<String, Object>> statistics) {
    if (statistics == null || statistics.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> sanitized = new ArrayList<>(statistics.size());
    for (Map<String, Object> entry : statistics) {
      Map<String, Object> copy = entry == null ? new LinkedHashMap<>() : new LinkedHashMap<>(entry);
      Object blobs = copy.get("blob-metadata");
      if (!(blobs instanceof List<?>)) {
        copy.put("blob-metadata", List.of());
      }
      sanitized.add(copy);
    }
    return sanitized;
  }

  private static List<Map<String, Object>> nonNullMapList(List<Map<String, Object>> value) {
    if (value == null || value.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> copy = new ArrayList<>(value.size());
    for (Map<String, Object> entry : value) {
      copy.add(entry == null ? Map.of() : new LinkedHashMap<>(entry));
    }
    return copy;
  }

  private static List<Map<String, Object>> partitionStatistics(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergPartitionStatisticsFile file : metadata.getPartitionStatisticsList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("snapshot-id", file.getSnapshotId());
      entry.put("statistics-path", file.getStatisticsPath());
      entry.put("file-size-in-bytes", file.getFileSizeInBytes());
      out.add(entry);
    }
    return out;
  }

  private static List<Map<String, Object>> snapshots(List<Snapshot> snapshots) {
    if (snapshots == null || snapshots.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (Snapshot snapshot : snapshots) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("snapshot-id", snapshot.getSnapshotId());
      if (snapshot.getParentSnapshotId() > 0) {
        entry.put("parent-snapshot-id", snapshot.getParentSnapshotId());
      }
      long sequenceNumber = snapshot.getSequenceNumber();
      if (sequenceNumber > 0) {
        entry.put("sequence-number", sequenceNumber);
      }
      if (snapshot.hasUpstreamCreatedAt()) {
        entry.put("timestamp-ms", snapshot.getUpstreamCreatedAt().getSeconds() * 1000L);
      } else if (snapshot.hasIngestedAt()) {
        entry.put("timestamp-ms", snapshot.getIngestedAt().getSeconds() * 1000L);
      }
      if (!snapshot.getManifestList().isBlank()) {
        entry.put("manifest-list", snapshot.getManifestList());
      }
      int schemaId = snapshot.getSchemaId();
      if (schemaId >= 0) {
        entry.put("schema-id", schemaId);
      }
      if (!snapshot.getSummaryMap().isEmpty()) {
        entry.put("summary", snapshot.getSummaryMap());
      }
      out.add(entry);
    }
    return out;
  }

  private static Long maybeLong(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private static Integer maybeInt(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private static Integer asInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    String text = value.toString();
    if (text.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Object firstNonNull(Object first, Object second) {
    return first != null ? first : second;
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

  private static void normalizeSortOrder(Map<String, Object> order) {
    Integer orderId = asInteger(firstNonNull(order.get("sort-order-id"), order.get("order-id")));
    if (orderId == null || orderId < 0) {
      orderId = 0;
    }
    order.put("sort-order-id", orderId);
    order.put("order-id", orderId);
    Object fieldsObj = order.get("fields");
    if (!(fieldsObj instanceof List<?> list)) {
      order.put("fields", List.of());
      return;
    }
    List<Map<String, Object>> normalized = new ArrayList<>();
    for (Object entry : list) {
      if (!(entry instanceof Map<?, ?> mapEntry)) {
        continue;
      }
      Map<String, Object> field = new LinkedHashMap<>();
      for (Map.Entry<?, ?> e : mapEntry.entrySet()) {
        if (e.getKey() != null) {
          field.put(e.getKey().toString(), e.getValue());
        }
      }
      if (!field.containsKey("source-id") && field.containsKey("source")) {
        field.put("source-id", field.get("source"));
      }
      if (!field.containsKey("transform")) {
        field.put("transform", "identity");
      }
      if (!field.containsKey("direction")) {
        field.put("direction", "ASC");
      }
      if (!field.containsKey("null-order")) {
        field.put("null-order", "NULLS_FIRST");
      }
      normalized.add(field);
    }
    order.put("fields", normalized);
  }
}
