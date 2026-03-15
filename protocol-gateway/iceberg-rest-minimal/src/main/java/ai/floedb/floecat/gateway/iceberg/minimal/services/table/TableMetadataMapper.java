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
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;

public final class TableMetadataMapper {
  private TableMetadataMapper() {}

  public static Map<String, Object> loadMetadata(
      Table table, Snapshot snapshot, ObjectMapper mapper) {
    IcebergMetadata metadata =
        decode(snapshot == null ? null : snapshot.getFormatMetadataMap().get("iceberg"));
    if (metadata != null) {
      return loadMetadataFromIceberg(table, metadata, snapshot, mapper);
    }
    Map<String, Object> view = new LinkedHashMap<>();
    view.put("format-version", 2);
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
    view.put("snapshots", snapshot == null ? List.of() : List.of(snapshot(snapshot)));
    Map<String, String> properties = new LinkedHashMap<>(table.getPropertiesMap());
    properties.remove("metadata-location");
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
                        && entry.getValue().snapshotId() > 0L
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
    view.put("snapshots", snapshotValues);
    Map<String, String> properties = new LinkedHashMap<>(table.getPropertiesMap());
    properties.remove("metadata-location");
    properties.put("metadata-location", metadataLocation);
    view.put("properties", properties);
    return Map.copyOf(view);
  }

  public static String metadataLocation(Table table, Snapshot snapshot) {
    IcebergMetadata metadata =
        decode(snapshot == null ? null : snapshot.getFormatMetadataMap().get("iceberg"));
    if (metadata != null && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    String property = table.getPropertiesOrDefault("metadata-location", "");
    return property.isBlank() ? null : property;
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
      if (metadata.getCurrentSnapshotId() > 0L) {
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
              .filter(entry -> entry.getValue() != null && entry.getValue().getSnapshotId() > 0L)
              .collect(
                  LinkedHashMap::new,
                  (map, entry) -> map.put(entry.getKey(), ref(entry.getValue())),
                  LinkedHashMap::putAll));
      view.put("snapshots", snapshot == null ? List.of() : List.of(snapshot(snapshot)));
    }
    Map<String, String> properties = new LinkedHashMap<>(table.getPropertiesMap());
    properties.remove("metadata-location");
    view.put("properties", properties);
    return Map.copyOf(view);
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
    if (snapshot.getParentSnapshotId() != 0L) {
      values.put("parent-snapshot-id", snapshot.getParentSnapshotId());
    }
    if (snapshot.getSequenceNumber() != 0L) {
      values.put("sequence-number", snapshot.getSequenceNumber());
    }
    if (!snapshot.getManifestList().isBlank()) {
      values.put("manifest-list", snapshot.getManifestList());
    }
    if (snapshot.getSchemaId() != 0) {
      values.put("schema-id", snapshot.getSchemaId());
    }
    values.put("summary", snapshot.getSummaryCount() == 0 ? Map.of() : snapshot.getSummaryMap());
    if (snapshot.hasUpstreamCreatedAt()) {
      values.put("timestamp-ms", snapshot.getUpstreamCreatedAt().getSeconds() * 1000L);
    }
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
