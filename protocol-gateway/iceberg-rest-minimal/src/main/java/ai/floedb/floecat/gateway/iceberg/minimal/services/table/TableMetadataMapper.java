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

package ai.floedb.floecat.gateway.iceberg.minimal.services.table;

import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadataLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSnapshotLogEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;

public final class TableMetadataMapper {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String DATA_SOURCE_FORMAT = "data_source_format";
  private static final String DELTA_SOURCE = "DELTA";
  private static final String NAME_MAPPING_PROPERTY = "schema.name-mapping.default";

  private TableMetadataMapper() {}

  public static Map<String, Object> loadMetadata(
      Table table, Snapshot snapshot, ObjectMapper mapper) {
    IcebergMetadata metadata =
        decode(snapshot == null ? null : snapshot.getFormatMetadataMap().get("iceberg"));
    if (metadata != null) {
      return loadMetadataFromIceberg(table, metadata, snapshot, mapper);
    }
    Map<String, Object> view = new LinkedHashMap<>();
    Integer formatVersion = propertyInt(table, "format-version");
    view.put("format-version", formatVersion == null ? 2 : formatVersion);
    putIfNonNull(view, "table-uuid", propertyString(table, "table-uuid"));
    putIfNonNull(view, "location", propertyString(table, "location", "storage_location"));
    putIfNonNull(view, "last-updated-ms", propertyLong(table, "last-updated-ms"));
    putIfNonNull(view, "last-column-id", propertyInt(table, "last-column-id"));
    putIfNonNull(view, "current-schema-id", propertyInt(table, "current-schema-id"));
    putIfNonNull(view, "default-spec-id", propertyInt(table, "default-spec-id"));
    putIfNonNull(view, "last-partition-id", propertyInt(table, "last-partition-id"));
    putIfNonNull(view, "default-sort-order-id", propertyInt(table, "default-sort-order-id"));
    putIfNonNull(view, "current-snapshot-id", propertyLong(table, "current-snapshot-id"));
    putIfNonNull(view, "last-sequence-number", propertyLong(table, "last-sequence-number"));
    view.put("schemas", List.of(parseJson(mapper, table.getSchemaJson())));
    view.put("partition-specs", List.of(defaultPartitionSpec()));
    view.put("sort-orders", List.of(defaultSortOrder()));
    view.put("refs", Map.of());
    view.put("snapshot-log", List.of());
    view.put("metadata-log", List.of());
    view.put("snapshots", snapshot == null ? List.of() : List.of(snapshot(snapshot)));
    Map<String, String> properties = new LinkedHashMap<>(table.getPropertiesMap());
    properties.remove("metadata-location");
    ensureDeltaNameMappingProperty(
        properties, castSchemaList(view.get("schemas")), propertyInt(table, "current-schema-id"));
    view.put("properties", properties);
    return Map.copyOf(view);
  }

  public static Map<String, Object> loadImportedMetadata(
      Table table, IcebergMetadata metadata, ObjectMapper mapper) {
    return loadMetadataFromIceberg(table, metadata, null, mapper);
  }

  public static Map<String, Object> loadHydratedMetadata(
      Table table,
      TableMetadata metadata,
      String metadataLocation,
      ObjectMapper mapper,
      List<Snapshot> snapshots) {
    Map<String, Object> view = new LinkedHashMap<>();
    Set<Long> resolvableSnapshotIds = new HashSet<>();
    List<Map<String, Object>> snapshotValues =
        snapshots == null
            ? List.of()
            : snapshots.stream()
                .map(
                    snapshot -> {
                      resolvableSnapshotIds.add(snapshot.getSnapshotId());
                      return snapshot(snapshot);
                    })
                .toList();
    view.put("format-version", metadata.formatVersion());
    if (metadata.uuid() != null && !metadata.uuid().isBlank()) {
      view.put("table-uuid", metadata.uuid());
    }
    putIfNonNull(view, "location", metadata.location());
    view.put("last-updated-ms", metadata.lastUpdatedMillis());
    view.put("last-column-id", metadata.lastColumnId());
    view.put("current-schema-id", metadata.currentSchemaId());
    view.put("default-spec-id", metadata.defaultSpecId());
    view.put("last-partition-id", metadata.lastAssignedPartitionId());
    view.put("default-sort-order-id", metadata.defaultSortOrderId());
    if (metadata.currentSnapshot() != null
        && resolvableSnapshotIds.contains(metadata.currentSnapshot().snapshotId())) {
      view.put("current-snapshot-id", metadata.currentSnapshot().snapshotId());
    }
    view.put("last-sequence-number", metadata.lastSequenceNumber());
    view.put(
        "schemas",
        metadata.schemas().stream()
            .map(schema -> parseJson(mapper, org.apache.iceberg.SchemaParser.toJson(schema)))
            .toList());
    view.put(
        "partition-specs",
        metadata.specs().stream()
            .map(
                spec -> {
                  Map<String, Object> values = new LinkedHashMap<>();
                  values.put("spec-id", spec.specId());
                  values.put(
                      "fields",
                      spec.fields().stream()
                          .map(
                              field -> {
                                Map<String, Object> fieldValues = new LinkedHashMap<>();
                                fieldValues.put("field-id", field.fieldId());
                                fieldValues.put("source-id", field.sourceId());
                                fieldValues.put("name", field.name());
                                fieldValues.put("transform", field.transform().toString());
                                return Map.copyOf(fieldValues);
                              })
                          .toList());
                  return Map.copyOf(values);
                })
            .toList());
    view.put(
        "sort-orders",
        metadata.sortOrders().stream()
            .map(
                order -> {
                  Map<String, Object> values = new LinkedHashMap<>();
                  values.put("order-id", order.orderId());
                  values.put(
                      "fields",
                      order.fields().stream()
                          .map(
                              field -> {
                                Map<String, Object> fieldValues = new LinkedHashMap<>();
                                fieldValues.put("source-id", field.sourceId());
                                fieldValues.put("transform", field.transform().toString());
                                fieldValues.put("direction", field.direction().name());
                                fieldValues.put("null-order", field.nullOrder().name());
                                return Map.copyOf(fieldValues);
                              })
                          .toList());
                  return Map.copyOf(values);
                })
            .toList());
    view.put(
        "refs",
        metadata.refs().entrySet().stream()
            .filter(
                entry ->
                    entry.getValue() != null
                        && snapshotIdIsDefined(entry.getValue().snapshotId())
                        && resolvableSnapshotIds.contains(entry.getValue().snapshotId()))
            .collect(
                LinkedHashMap::new,
                (map, entry) -> {
                  org.apache.iceberg.SnapshotRef ref = entry.getValue();
                  Map<String, Object> values = new LinkedHashMap<>();
                  values.put("snapshot-id", ref.snapshotId());
                  values.put("type", ref.type().name().toLowerCase());
                  if (ref.maxRefAgeMs() != null) {
                    values.put("max-ref-age-ms", ref.maxRefAgeMs());
                  }
                  if (ref.maxSnapshotAgeMs() != null) {
                    values.put("max-snapshot-age-ms", ref.maxSnapshotAgeMs());
                  }
                  if (ref.minSnapshotsToKeep() != null) {
                    values.put("min-snapshots-to-keep", ref.minSnapshotsToKeep());
                  }
                  map.put(entry.getKey(), Map.copyOf(values));
                },
                LinkedHashMap::putAll));
    view.put("snapshot-log", snapshotLog(metadata));
    view.put("metadata-log", metadataLog(metadata));
    view.put("snapshots", snapshotValues);
    Map<String, String> properties = new LinkedHashMap<>(table.getPropertiesMap());
    properties.remove("metadata-location");
    properties.put("metadata-location", metadataLocation);
    ensureDeltaNameMappingProperty(
        properties, castSchemaList(view.get("schemas")), metadata.currentSchemaId());
    view.put("properties", properties);
    return Map.copyOf(view);
  }

  public static String metadataLocation(Table table, Snapshot snapshot) {
    String property = table.getPropertiesOrDefault("metadata-location", "");
    if (!property.isBlank()) {
      return property;
    }
    return snapshotMetadataLocation(snapshot);
  }

  public static String snapshotMetadataLocation(Snapshot snapshot) {
    IcebergMetadata metadata =
        decode(snapshot == null ? null : snapshot.getFormatMetadataMap().get("iceberg"));
    if (metadata != null && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    return null;
  }

  private static IcebergMetadata decode(ByteString bytes) {
    if (bytes == null || bytes.isEmpty()) {
      return null;
    }
    try {
      return IcebergMetadata.parseFrom(bytes);
    } catch (Exception ignored) {
      return null;
    }
  }

  private static boolean hasDefinedCurrentSnapshotId(IcebergMetadata metadata, Snapshot snapshot) {
    if (metadata == null) {
      return false;
    }
    if (metadata.getCurrentSnapshotId() != 0L) {
      return metadata.getCurrentSnapshotId() > 0L;
    }
    if (snapshot != null && snapshot.getSnapshotId() == 0L) {
      return true;
    }
    return metadata.getRefsMap().values().stream()
        .anyMatch(ref -> ref != null && ref.getSnapshotId() == 0L);
  }

  private static boolean snapshotIdIsDefined(long snapshotId) {
    return snapshotId >= 0L;
  }

  private static Map<String, Object> loadMetadataFromIceberg(
      Table table, IcebergMetadata metadata, Snapshot snapshot, ObjectMapper mapper) {
    Map<String, Object> view = new LinkedHashMap<>();
    if (metadata != null) {
      view.put("format-version", metadata.getFormatVersion());
      if (!metadata.getTableUuid().isBlank()) {
        view.put("table-uuid", metadata.getTableUuid());
      }
      putIfNonNull(view, "location", propertyString(table, "location", "storage_location"));
      view.put("last-updated-ms", metadata.getLastUpdatedMs());
      view.put("last-column-id", metadata.getLastColumnId());
      view.put("current-schema-id", metadata.getCurrentSchemaId());
      view.put("default-spec-id", metadata.getDefaultSpecId());
      view.put("last-partition-id", metadata.getLastPartitionId());
      view.put("default-sort-order-id", metadata.getDefaultSortOrderId());
      if (hasDefinedCurrentSnapshotId(metadata, snapshot)) {
        view.put("current-snapshot-id", metadata.getCurrentSnapshotId());
      }
      view.put("last-sequence-number", metadata.getLastSequenceNumber());
      view.put(
          "schemas",
          metadata.getSchemasList().stream()
              .map(schema -> parseJson(mapper, schema.getSchemaJson()))
              .toList());
      view.put(
          "partition-specs",
          metadata.getPartitionSpecsList().stream()
              .map(TableMetadataMapper::partitionSpec)
              .toList());
      view.put(
          "sort-orders",
          metadata.getSortOrdersList().stream().map(TableMetadataMapper::sortOrder).toList());
      view.put(
          "refs",
          metadata.getRefsMap().entrySet().stream()
              .filter(
                  entry ->
                      entry.getValue() != null
                          && snapshotIdIsDefined(entry.getValue().getSnapshotId()))
              .collect(
                  LinkedHashMap::new,
                  (map, entry) -> map.put(entry.getKey(), ref(entry.getValue())),
                  LinkedHashMap::putAll));
      view.put("snapshot-log", snapshotLog(metadata));
      view.put("metadata-log", metadataLog(metadata));
      view.put("snapshots", snapshot == null ? List.of() : List.of(snapshot(snapshot)));
    }
    Map<String, String> properties = new LinkedHashMap<>(table.getPropertiesMap());
    properties.remove("metadata-location");
    ensureDeltaNameMappingProperty(
        properties,
        castSchemaList(view.get("schemas")),
        metadata == null ? null : metadata.getCurrentSchemaId());
    view.put("properties", properties);
    return Map.copyOf(view);
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> castSchemaList(Object value) {
    return value instanceof List<?> list ? (List<Map<String, Object>>) list : List.of();
  }

  private static void ensureDeltaNameMappingProperty(
      Map<String, String> properties,
      List<Map<String, Object>> schemaList,
      Integer currentSchemaId) {
    if (properties == null || properties.isEmpty()) {
      return;
    }
    String source = properties.get(DATA_SOURCE_FORMAT);
    if (source == null || !DELTA_SOURCE.equalsIgnoreCase(source)) {
      return;
    }
    String existing = properties.get(NAME_MAPPING_PROPERTY);
    if (existing != null && !existing.isBlank()) {
      return;
    }
    String mappingJson = buildNameMappingJson(schemaList, currentSchemaId);
    if (mappingJson != null && !mappingJson.isBlank()) {
      properties.put(NAME_MAPPING_PROPERTY, mappingJson);
    }
  }

  private static String buildNameMappingJson(
      List<Map<String, Object>> schemaList, Integer currentSchemaId) {
    Map<String, Object> schema = findSchema(schemaList, currentSchemaId);
    if (schema == null) {
      return null;
    }
    List<Map<String, Object>> fields = asFieldMapList(schema.get("fields"));
    if (fields.isEmpty()) {
      return null;
    }
    List<Map<String, Object>> mapping = buildNameMappingFields(fields);
    if (mapping.isEmpty()) {
      return null;
    }
    try {
      return JSON.writeValueAsString(mapping);
    } catch (Exception ignored) {
      return null;
    }
  }

  private static Map<String, Object> findSchema(
      List<Map<String, Object>> schemaList, Integer currentSchemaId) {
    if (schemaList == null || schemaList.isEmpty()) {
      return null;
    }
    if (currentSchemaId != null) {
      for (Map<String, Object> schema : schemaList) {
        Object schemaId = schema == null ? null : schema.get("schema-id");
        if (schemaId instanceof Number number && number.intValue() == currentSchemaId) {
          return schema;
        }
      }
    }
    return schemaList.getFirst();
  }

  private static List<Map<String, Object>> asFieldMapList(Object value) {
    if (!(value instanceof List<?> list) || list.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new java.util.ArrayList<>(list.size());
    for (Object entry : list) {
      if (!(entry instanceof Map<?, ?> map) || map.isEmpty()) {
        continue;
      }
      LinkedHashMap<String, Object> values = new LinkedHashMap<>();
      map.forEach(
          (k, v) -> {
            if (k != null) {
              values.put(String.valueOf(k), v);
            }
          });
      if (!values.isEmpty()) {
        out.add(Map.copyOf(values));
      }
    }
    return List.copyOf(out);
  }

  private static List<Map<String, Object>> buildNameMappingFields(
      List<Map<String, Object>> fields) {
    if (fields == null || fields.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new java.util.ArrayList<>(fields.size());
    for (Map<String, Object> field : fields) {
      Map<String, Object> mapped = buildNameMappingField(field);
      if (mapped != null) {
        out.add(mapped);
      }
    }
    return List.copyOf(out);
  }

  private static Map<String, Object> buildNameMappingField(Map<String, Object> field) {
    if (field == null || field.isEmpty()) {
      return null;
    }
    Object fieldId = field.get("id");
    Object name = field.get("name");
    if (!(fieldId instanceof Number number)
        || number.intValue() <= 0
        || !(name instanceof String fieldName)
        || fieldName.isBlank()) {
      return null;
    }

    Map<String, Object> mapped = new LinkedHashMap<>();
    mapped.put("field-id", number.intValue());
    mapped.put("names", List.of(fieldName));

    if (field.get("type") instanceof Map<?, ?> typeMap) {
      Object typeName = typeMap.get("type");
      if (typeName instanceof String type && "struct".equalsIgnoreCase(type)) {
        List<Map<String, Object>> nested =
            buildNameMappingFields(asFieldMapList(typeMap.get("fields")));
        if (!nested.isEmpty()) {
          mapped.put("fields", nested);
        }
      }
    }
    return Map.copyOf(mapped);
  }

  public static boolean requiresHydration(Map<String, Object> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return true;
    }
    if (!metadata.containsKey("format-version")
        || !metadata.containsKey("last-updated-ms")
        || !metadata.containsKey("last-column-id")
        || !metadata.containsKey("current-schema-id")
        || !metadata.containsKey("default-spec-id")
        || !metadata.containsKey("last-partition-id")
        || !metadata.containsKey("default-sort-order-id")
        || !metadata.containsKey("last-sequence-number")
        || !metadata.containsKey("partition-specs")
        || !metadata.containsKey("refs")) {
      return true;
    }
    Object location = metadata.get("location");
    if (!(location instanceof String value) || value.isBlank()) {
      return true;
    }
    if (!metadata.containsKey("table-uuid")) {
      return true;
    }
    Object schemas = metadata.get("schemas");
    if (!(schemas instanceof List<?> list) || list.isEmpty()) {
      return true;
    }
    Object firstSchema = list.getFirst();
    if (!(firstSchema instanceof Map<?, ?> schemaMap)) {
      return true;
    }
    Object schemaType = schemaMap.get("type");
    if (!(schemaType instanceof String type) || type.isBlank()) {
      return true;
    }
    return !metadata.containsKey("sort-orders");
  }

  public static Map<String, Object> overlaySnapshots(
      Map<String, Object> metadata, List<Snapshot> snapshots) {
    if (metadata == null || metadata.isEmpty()) {
      return metadata;
    }
    List<Map<String, Object>> snapshotValues =
        snapshots == null
            ? List.of()
            : snapshots.stream().map(TableMetadataMapper::snapshot).toList();
    Set<Long> snapshotIds = new HashSet<>();
    if (snapshots != null) {
      for (Snapshot snapshot : snapshots) {
        snapshotIds.add(snapshot.getSnapshotId());
      }
    }
    Map<String, Object> view = new LinkedHashMap<>(metadata);
    view.put("snapshots", snapshotValues);
    view.put("snapshot-log", filteredSnapshotLog(view.get("snapshot-log"), snapshotValues));

    Object currentSnapshotId = view.get("current-snapshot-id");
    if (currentSnapshotId instanceof Number number && !snapshotIds.contains(number.longValue())) {
      view.remove("current-snapshot-id");
    }

    Object refs = view.get("refs");
    if (refs instanceof Map<?, ?> refMap && !refMap.isEmpty()) {
      LinkedHashMap<String, Object> filteredRefs = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : refMap.entrySet()) {
        if (!(entry.getKey() instanceof String name)
            || !(entry.getValue() instanceof Map<?, ?> values)) {
          continue;
        }
        Object snapshotId = values.get("snapshot-id");
        if (snapshotId instanceof Number number && snapshotIds.contains(number.longValue())) {
          filteredRefs.put(name, entry.getValue());
        }
      }
      view.put("refs", Map.copyOf(filteredRefs));
    }
    return Map.copyOf(view);
  }

  private static List<Map<String, Object>> snapshotLog(IcebergMetadata metadata) {
    if (metadata == null || metadata.getSnapshotLogCount() == 0) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>(metadata.getSnapshotLogCount());
    for (IcebergSnapshotLogEntry entry : metadata.getSnapshotLogList()) {
      Map<String, Object> log = new LinkedHashMap<>();
      log.put("timestamp-ms", entry.getTimestampMs());
      log.put("snapshot-id", entry.getSnapshotId());
      out.add(Map.copyOf(log));
    }
    return List.copyOf(out);
  }

  private static List<Map<String, Object>> metadataLog(IcebergMetadata metadata) {
    if (metadata == null || metadata.getMetadataLogCount() == 0) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>(metadata.getMetadataLogCount());
    for (IcebergMetadataLogEntry entry : metadata.getMetadataLogList()) {
      Map<String, Object> log = new LinkedHashMap<>();
      log.put("timestamp-ms", entry.getTimestampMs());
      log.put("metadata-file", entry.getFile());
      out.add(Map.copyOf(log));
    }
    return List.copyOf(out);
  }

  private static List<Map<String, Object>> snapshotLog(TableMetadata metadata) {
    if (metadata == null || metadata.snapshotLog() == null || metadata.snapshotLog().isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>(metadata.snapshotLog().size());
    for (org.apache.iceberg.HistoryEntry entry : metadata.snapshotLog()) {
      Map<String, Object> log = new LinkedHashMap<>();
      log.put("timestamp-ms", entry.timestampMillis());
      log.put("snapshot-id", entry.snapshotId());
      out.add(Map.copyOf(log));
    }
    return List.copyOf(out);
  }

  private static List<Map<String, Object>> metadataLog(TableMetadata metadata) {
    if (metadata == null
        || metadata.previousFiles() == null
        || metadata.previousFiles().isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>(metadata.previousFiles().size());
    for (org.apache.iceberg.TableMetadata.MetadataLogEntry entry : metadata.previousFiles()) {
      Map<String, Object> log = new LinkedHashMap<>();
      log.put("timestamp-ms", entry.timestampMillis());
      log.put("metadata-file", entry.file());
      out.add(Map.copyOf(log));
    }
    return List.copyOf(out);
  }

  private static List<Map<String, Object>> filteredSnapshotLog(
      Object rawSnapshotLog, List<Map<String, Object>> snapshotValues) {
    if (!(rawSnapshotLog instanceof List<?> logs) || logs.isEmpty()) {
      return List.of();
    }
    Set<Long> snapshotIds = new HashSet<>();
    for (Map<String, Object> snapshotValue : snapshotValues) {
      Object snapshotId = snapshotValue == null ? null : snapshotValue.get("snapshot-id");
      if (snapshotId instanceof Number number) {
        snapshotIds.add(number.longValue());
      }
    }
    if (snapshotIds.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>(logs.size());
    for (Object rawEntry : logs) {
      if (!(rawEntry instanceof Map<?, ?> entry)) {
        continue;
      }
      Long snapshotId =
          entry.get("snapshot-id") instanceof Number number ? number.longValue() : null;
      if (snapshotId == null || !snapshotIds.contains(snapshotId)) {
        continue;
      }
      Object timestamp = entry.get("timestamp-ms");
      Map<String, Object> log = new LinkedHashMap<>();
      log.put("snapshot-id", snapshotId);
      if (timestamp instanceof Number) {
        log.put("timestamp-ms", ((Number) timestamp).longValue());
      }
      out.add(Map.copyOf(log));
    }
    return List.copyOf(out);
  }

  private static Object parseJson(ObjectMapper mapper, String json) {
    if (json == null || json.isBlank()) {
      return Map.of();
    }
    try {
      return mapper.readValue(json, Object.class);
    } catch (Exception e) {
      return Map.of("json", json);
    }
  }

  private static Map<String, Object> partitionSpec(PartitionSpecInfo spec) {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("spec-id", spec.getSpecId());
    values.put(
        "fields",
        spec.getFieldsList().stream()
            .map(
                field -> {
                  Map<String, Object> fieldValues = new LinkedHashMap<>();
                  fieldValues.put("field-id", field.getFieldId());
                  fieldValues.put("source-id", field.getFieldId());
                  fieldValues.put("name", field.getName());
                  fieldValues.put("transform", field.getTransform());
                  return Map.copyOf(fieldValues);
                })
            .toList());
    return Map.copyOf(values);
  }

  private static Map<String, Object> defaultPartitionSpec() {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("spec-id", 0);
    values.put("fields", List.of());
    return Map.copyOf(values);
  }

  private static Map<String, Object> sortOrder(
      ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder order) {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("order-id", order.getSortOrderId());
    values.put(
        "fields",
        order.getFieldsList().stream()
            .map(
                field -> {
                  Map<String, Object> fieldValues = new LinkedHashMap<>();
                  fieldValues.put("source-id", field.getSourceFieldId());
                  fieldValues.put("transform", field.getTransform());
                  fieldValues.put("direction", field.getDirection());
                  fieldValues.put("null-order", field.getNullOrder());
                  return Map.copyOf(fieldValues);
                })
            .toList());
    return Map.copyOf(values);
  }

  private static Map<String, Object> defaultSortOrder() {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("order-id", 0);
    values.put("fields", List.of());
    return Map.copyOf(values);
  }

  private static Map<String, Object> ref(IcebergRef ref) {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("snapshot-id", ref.getSnapshotId());
    values.put("type", ref.getType());
    if (ref.hasMaxReferenceAgeMs()) {
      values.put("max-ref-age-ms", ref.getMaxReferenceAgeMs());
    }
    if (ref.hasMaxSnapshotAgeMs()) {
      values.put("max-snapshot-age-ms", ref.getMaxSnapshotAgeMs());
    }
    if (ref.hasMinSnapshotsToKeep()) {
      values.put("min-snapshots-to-keep", ref.getMinSnapshotsToKeep());
    }
    return Map.copyOf(values);
  }

  private static Map<String, Object> snapshot(Snapshot snapshot) {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("snapshot-id", snapshot.getSnapshotId());
    if (snapshot.hasParentSnapshotId()) {
      values.put("parent-snapshot-id", snapshot.getParentSnapshotId());
    }
    if (snapshot.getSequenceNumber() > 0L) {
      values.put("sequence-number", snapshot.getSequenceNumber());
    }
    values.put(
        "timestamp-ms",
        snapshot.hasUpstreamCreatedAt()
            ? snapshot.getUpstreamCreatedAt().getSeconds() * 1000L
            : 0L);
    values.put(
        "manifest-list", snapshot.getManifestList().isBlank() ? "" : snapshot.getManifestList());
    if (snapshot.getSchemaId() >= 0) {
      values.put("schema-id", snapshot.getSchemaId());
    }
    Map<String, String> summary = new LinkedHashMap<>();
    if (snapshot.getSummaryCount() > 0) {
      summary.putAll(snapshot.getSummaryMap());
    }
    summary.putIfAbsent("operation", "append");
    values.put("summary", Map.copyOf(summary));
    return Map.copyOf(values);
  }

  private static Long propertyLong(Table table, String key) {
    String value = table.getPropertiesOrDefault(key, "");
    if (value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Integer propertyInt(Table table, String key) {
    String value = table.getPropertiesOrDefault(key, "");
    if (value.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String propertyString(Table table, String... keys) {
    for (String key : keys) {
      String value = table.getPropertiesOrDefault(key, "");
      if (!value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  private static void putIfNonNull(Map<String, Object> map, String key, Object value) {
    if (value != null) {
      map.put(key, value);
    }
  }
}
