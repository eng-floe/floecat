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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asInteger;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asLong;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asObjectMap;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asString;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonNull;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.maybeInt;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.normalizeFormatVersion;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.normalizeFormatVersionForSnapshots;

import ai.floedb.floecat.catalog.rpc.PartitionField;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.SnapshotMetadataUtil;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergBlobMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadataLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergPartitionStatisticsFile;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSnapshotLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortField;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergStatisticsFile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class TableMetadataBuilder {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String DATA_SOURCE_FORMAT = "data_source_format";
  private static final String DELTA_SOURCE = "DELTA";
  private static final String NAME_MAPPING_PROPERTY = "schema.name-mapping.default";

  private TableMetadataBuilder() {}

  public static TableMetadataView fromCatalog(
      String tableName,
      Table table,
      Map<String, String> props,
      IcebergMetadata metadata,
      List<Snapshot> snapshots) {
    String metadataLocation =
        metadata == null || metadata.getMetadataLocation().isBlank()
            ? null
            : metadata.getMetadataLocation();
    return buildMetadata(tableName, table, props, metadata, snapshots, metadataLocation);
  }

  public static TableMetadataView fromCanonicalMetadata(
      String tableName, IcebergMetadata metadata, List<Snapshot> snapshots) {
    String metadataLocation =
        metadata == null || metadata.getMetadataLocation().isBlank()
            ? null
            : metadata.getMetadataLocation();
    return buildMetadata(tableName, null, Map.of(), metadata, snapshots, metadataLocation);
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
    Map<String, String> effectiveProps = new LinkedHashMap<>();
    if (metadata != null && metadata.getPropertiesCount() > 0) {
      effectiveProps.putAll(metadata.getPropertiesMap());
    }
    if (props != null && !props.isEmpty()) {
      effectiveProps.putAll(props);
    }
    String propertyMetadataLocation = MetadataLocationUtil.metadataLocation(effectiveProps);
    // The catalog pointer (table property) is the source of truth for current metadata location.
    // Snapshot payload metadata is used as fallback only when pointer data is unavailable.
    if (propertyMetadataLocation != null && !propertyMetadataLocation.isBlank()) {
      metadataLocation = propertyMetadataLocation;
    } else if (metadataLocation == null || metadataLocation.isBlank()) {
      metadataLocation = propertyMetadataLocation;
    }
    String metadataLocationValue =
        metadata != null && metadata.getLocation() != null && !metadata.getLocation().isBlank()
            ? metadata.getLocation()
            : null;
    String location =
        table != null && table.hasUpstream()
            ? table.getUpstream().getUri()
            : metadataLocationValue != null
                ? metadataLocationValue
                : effectiveProps.get("location");
    location = hasText(location) ? location : null;
    Long lastUpdatedMs =
        (metadata != null && metadata.getLastUpdatedMs() > 0)
            ? Long.valueOf(metadata.getLastUpdatedMs())
            : null;
    Long currentSnapshotId =
        metadata != null && metadata.getCurrentSnapshotId() >= 0
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
    formatVersion =
        normalizeFormatVersion(formatVersion, maybeInt(formatVersionProperty(effectiveProps)));
    if (tableUuid == null) {
      String candidate = effectiveProps.get("table-uuid");
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
      currentSchemaId = maybeInt(effectiveProps.get("current-schema-id"));
    }
    if (lastColumnId == null) {
      lastColumnId = maybeInt(effectiveProps.get("last-column-id"));
    }
    if (defaultSpecId == null) {
      defaultSpecId = maybeInt(effectiveProps.get("default-spec-id"));
    }
    if (lastPartitionId == null) {
      lastPartitionId = maybeInt(effectiveProps.get("last-partition-id"));
    }
    if (defaultSortOrderId == null) {
      defaultSortOrderId = maybeInt(effectiveProps.get("default-sort-order-id"));
    }
    if (lastUpdatedMs == null) {
      lastUpdatedMs = asLong(effectiveProps.get("last-updated-ms"));
    }
    if (lastUpdatedMs == null || lastUpdatedMs <= 0) {
      lastUpdatedMs = System.currentTimeMillis();
    }
    if (currentSnapshotId == null) {
      currentSnapshotId = asLong(effectiveProps.get("current-snapshot-id"));
    }
    if (lastSequenceNumber == null) {
      lastSequenceNumber = asLong(effectiveProps.get("last-sequence-number"));
    }
    if (lastSequenceNumber == null) {
      lastSequenceNumber = 0L;
    }
    List<Map<String, Object>> schemaList = schemasFromMetadata(metadata);
    if (schemaList.isEmpty() && table != null) {
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
    ensureDeltaNameMappingProperty(effectiveProps, schemaList, currentSchemaId);
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
    List<Map<String, Object>> snapshotList = snapshots(orderedSnapshots);
    Set<Long> snapshotIds = snapshotIds(snapshotList);
    if (currentSnapshotId == null
        || currentSnapshotId < 0
        || !snapshotIds.contains(currentSnapshotId)) {
      currentSnapshotId = null;
    }
    Long maxSnapshotSequence = maxSnapshotSequence(orderedSnapshots);
    if (maxSnapshotSequence != null
        && (lastSequenceNumber == null || lastSequenceNumber < maxSnapshotSequence)) {
      lastSequenceNumber = maxSnapshotSequence;
    }
    if (lastSequenceNumber == null || lastSequenceNumber < 0) {
      lastSequenceNumber = 0L;
    }
    formatVersion = normalizeFormatVersionForSnapshots(formatVersion, maxSnapshotSequence);
    Map<String, Object> refs = refs(metadata);
    refs = mergePropertyRefs(effectiveProps, refs);
    if (currentSnapshotId == null || currentSnapshotId < 0) {
      Long mainRefSnapshotId = mainRefSnapshotId(refs);
      if (mainRefSnapshotId != null && snapshotIds.contains(mainRefSnapshotId)) {
        currentSnapshotId = mainRefSnapshotId;
      }
    }
    refs = sanitizeRefs(refs, snapshotIds, currentSnapshotId);
    syncProperty(effectiveProps, "table-uuid", tableUuid);
    syncOrRemove(effectiveProps, "current-snapshot-id", currentSnapshotId);
    syncProperty(effectiveProps, "last-sequence-number", lastSequenceNumber);
    syncProperty(effectiveProps, "format-version", formatVersion);
    syncProperty(effectiveProps, "current-schema-id", currentSchemaId);
    syncProperty(effectiveProps, "last-column-id", lastColumnId);
    syncProperty(effectiveProps, "default-spec-id", defaultSpecId);
    syncProperty(effectiveProps, "last-partition-id", lastPartitionId);
    syncProperty(effectiveProps, "default-sort-order-id", defaultSortOrderId);
    removeMetadataLocation(effectiveProps);
    return new TableMetadataView(
        formatVersion,
        tableUuid,
        location,
        metadataLocation,
        lastUpdatedMs,
        effectiveProps,
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
        snapshotList);
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
    String metadataLoc =
        MetadataLocationUtil.metadataLocation(request == null ? null : request.properties());
    String metadataLocation = null;
    if (metadataLoc != null && !metadataLoc.isBlank()) {
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
    Integer formatVersion = normalizeFormatVersion(maybeInt(formatVersionProperty(props)), null);
    props.putIfAbsent("format-version", formatVersion.toString());
    props.putIfAbsent("current-schema-id", schemaId.toString());
    props.putIfAbsent("last-column-id", lastColumnId.toString());
    props.putIfAbsent("default-spec-id", defaultSpecId.toString());
    props.putIfAbsent("last-partition-id", lastPartitionId.toString());
    props.putIfAbsent("default-sort-order-id", defaultSortOrderId.toString());
    long lastSequenceNumber = 0L;
    props.putIfAbsent("last-sequence-number", Long.toString(lastSequenceNumber));
    removeMetadataLocation(props);
    String tableUuid = props.get("table-uuid");
    if (tableUuid == null || tableUuid.isBlank()) {
      tableUuid = table.hasResourceId() ? table.getResourceId().getId() : tableName;
    }
    return new TableMetadataView(
        formatVersion,
        tableUuid,
        hasText(location) ? location : null,
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

  private static void ensureDeltaNameMappingProperty(
      Map<String, String> props, List<Map<String, Object>> schemaList, Integer currentSchemaId) {
    if (props == null || props.isEmpty()) {
      return;
    }
    String source = props.get(DATA_SOURCE_FORMAT);
    if (source == null || !DELTA_SOURCE.equalsIgnoreCase(source)) {
      return;
    }
    String existing = props.get(NAME_MAPPING_PROPERTY);
    if (existing != null && !existing.isBlank()) {
      return;
    }
    String mappingJson = buildNameMappingJson(schemaList, currentSchemaId);
    if (mappingJson != null && !mappingJson.isBlank()) {
      props.put(NAME_MAPPING_PROPERTY, mappingJson);
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
      // Iceberg expects schema.name-mapping.default as a top-level JSON array of mapped fields.
      return JSON.writeValueAsString(mapping);
    } catch (JsonProcessingException ignored) {
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
        Integer schemaId = asInteger(schema == null ? null : schema.get("schema-id"));
        if (schemaId != null && schemaId.equals(currentSchemaId)) {
          return schema;
        }
      }
    }
    return schemaList.get(0);
  }

  private static List<Map<String, Object>> buildNameMappingFields(
      List<Map<String, Object>> fields) {
    if (fields == null || fields.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>(fields.size());
    for (Map<String, Object> field : fields) {
      Map<String, Object> mapped = buildNameMappingField(field);
      if (mapped != null) {
        out.add(mapped);
      }
    }
    return out;
  }

  private static Map<String, Object> buildNameMappingField(Map<String, Object> field) {
    if (field == null || field.isEmpty()) {
      return null;
    }
    Integer fieldId = asInteger(field.get("id"));
    String name = asString(field.get("name"));
    if (fieldId == null || fieldId <= 0 || name == null || name.isBlank()) {
      return null;
    }

    Map<String, Object> mapped = new LinkedHashMap<>();
    mapped.put("field-id", fieldId);
    mapped.put("names", List.of(name));

    Map<String, Object> type = asObjectMap(field.get("type"));
    if (type != null) {
      String typeName = asString(type.get("type"));
      if ("struct".equalsIgnoreCase(typeName)) {
        List<Map<String, Object>> nested =
            buildNameMappingFields(asFieldMapList(type.get("fields")));
        if (!nested.isEmpty()) {
          mapped.put("fields", nested);
        }
      }
    }
    return mapped;
  }

  private static List<Map<String, Object>> asFieldMapList(Object value) {
    if (!(value instanceof List<?> list) || list.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>(list.size());
    for (Object entry : list) {
      Map<String, Object> mapped = asObjectMap(entry);
      if (mapped != null && !mapped.isEmpty()) {
        out.add(mapped);
      }
    }
    return out;
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

  private static void syncOrRemove(Map<String, String> props, String key, Object value) {
    if (props == null || key == null || key.isBlank()) {
      return;
    }
    if (value == null) {
      props.remove(key);
      return;
    }
    props.put(key, value.toString());
  }

  private static void syncWriteMetadataPath(Map<String, String> props, String metadataLocation) {
    String directory = MetadataLocationUtil.metadataDirectory(metadataLocation);
    if (directory == null || directory.isBlank()) {
      return;
    }
    props.put("write.metadata.path", directory);
  }

  private static void removeMetadataLocation(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return;
    }
    props.remove(MetadataLocationUtil.PRIMARY_KEY);
  }

  private static boolean hasText(String value) {
    return value != null && !value.isBlank();
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

  private static Set<Long> snapshotIds(List<Map<String, Object>> snapshots) {
    Set<Long> ids = new HashSet<>();
    if (snapshots == null) {
      return ids;
    }
    for (Map<String, Object> snapshot : snapshots) {
      Long id = asLong(snapshot == null ? null : snapshot.get("snapshot-id"));
      if (id != null && id >= 0) {
        ids.add(id);
      }
    }
    return ids;
  }

  private static Map<String, Object> sanitizeRefs(
      Map<String, Object> refs, Set<Long> snapshotIds, Long currentSnapshotId) {
    Map<String, Object> out = new LinkedHashMap<>();
    if (refs != null && !refs.isEmpty()) {
      refs.forEach(
          (name, rawRef) -> {
            if (name == null || name.isBlank() || !(rawRef instanceof Map<?, ?> rawMap)) {
              return;
            }
            Map<String, Object> ref = new LinkedHashMap<>();
            rawMap.forEach(
                (k, v) -> {
                  if (k instanceof String key && v != null) {
                    ref.put(key, v);
                  }
                });
            Long refSnapshotId = asLong(ref.get("snapshot-id"));
            if (refSnapshotId == null
                || refSnapshotId < 0
                || !snapshotIds.contains(refSnapshotId)) {
              return;
            }
            ref.put("snapshot-id", refSnapshotId);
            String type = asString(ref.get("type"));
            ref.put("type", (type == null || type.isBlank()) ? "branch" : type.toLowerCase());
            if (ref.containsKey("max-reference-age-ms")) {
              Object legacyValue = ref.remove("max-reference-age-ms");
              ref.putIfAbsent("max-ref-age-ms", legacyValue);
            }
            out.put(name, Map.copyOf(ref));
          });
    }
    if (currentSnapshotId != null && currentSnapshotId >= 0) {
      out.put("main", Map.of("snapshot-id", currentSnapshotId, "type", "branch"));
    }
    return out;
  }

  private static Long mainRefSnapshotId(Map<String, Object> refs) {
    if (refs == null || refs.isEmpty()) {
      return null;
    }
    Object main = refs.get("main");
    if (!(main instanceof Map<?, ?> map)) {
      return null;
    }
    return asLong(map.get("snapshot-id"));
  }

  private static String formatVersionProperty(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return null;
    }
    String value = props.get("format-version");
    return (value == null || value.isBlank()) ? null : value;
  }

  private static Map<String, Object> schemaFromTable(Table table) {
    String schemaJson = table.getSchemaJson();
    if (schemaJson == null || schemaJson.isBlank()) {
      throw new IllegalArgumentException("schemaJson is required");
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
    normalizeSchema(schema);
    return schema;
  }

  private static Map<String, Object> schemaFromRequest(TableRequests.Create request) {
    JsonNode node = request.schema();
    if (node == null || node.isNull()) {
      throw new IllegalArgumentException("schema is required");
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("schema must be an object");
    }
    Map<String, Object> schema =
        new LinkedHashMap<>(JSON.convertValue(node, new TypeReference<Map<String, Object>>() {}));
    normalizeSchema(schema);
    return schema;
  }

  private static Map<String, Object> partitionSpecFromRequest(TableRequests.Create request) {
    JsonNode node = request.partitionSpec();
    if (node == null || node.isNull()) {
      throw new IllegalArgumentException("partition-spec is required");
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("partition-spec must be an object");
    }
    Map<String, Object> spec =
        new LinkedHashMap<>(JSON.convertValue(node, new TypeReference<Map<String, Object>>() {}));
    Integer specId = asInteger(spec.get("spec-id"));
    if (specId == null) {
      throw new IllegalArgumentException("partition-spec requires spec-id");
    }
    return spec;
  }

  private static Map<String, Object> sortOrderFromRequest(TableRequests.Create request) {
    JsonNode node = request.writeOrder();
    if (node == null || node.isNull()) {
      throw new IllegalArgumentException("write-order is required");
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("write-order must be an object");
    }
    Map<String, Object> order =
        new LinkedHashMap<>(JSON.convertValue(node, new TypeReference<Map<String, Object>>() {}));
    Integer orderId = asInteger(order.get("order-id"));
    if (orderId == null) {
      throw new IllegalArgumentException("write-order requires order-id");
    }
    normalizeSortOrder(order);
    return order;
  }

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

  private static List<Map<String, Object>> schemasFromMetadata(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (IcebergSchema schema : metadata.getSchemasList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("schema-id", schema.getSchemaId());
      Object schemaObj = parseSchema(schema.getSchemaJson());
      if (schemaObj instanceof Map<?, ?> mapObj) {
        @SuppressWarnings("unchecked")
        Map<String, Object> typed = (Map<String, Object>) mapObj;
        entry.putAll(typed);
      }
      if (schema.getIdentifierFieldIdsCount() > 0) {
        entry.put("identifier-field-ids", schema.getIdentifierFieldIdsList());
      }
      out.add(entry);
    }
    return out;
  }

  private static List<Map<String, Object>> partitionSpecsFromMetadata(IcebergMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (PartitionSpecInfo spec : metadata.getPartitionSpecsList()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("spec-id", spec.getSpecId());
      List<Map<String, Object>> fields = new ArrayList<>();
      for (PartitionField field : spec.getFieldsList()) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("field-id", field.getFieldId());
        f.put("source-id", field.getFieldId());
        f.put("name", field.getName());
        f.put("transform", field.getTransform());
        fields.add(f);
      }
      entry.put("fields", fields);
      out.add(entry);
    }
    return out;
  }

  private static List<Map<String, Object>> sortOrdersFromMetadata(IcebergMetadata metadata) {
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
        f.put("source-id", field.getSourceFieldId());
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

  private static void normalizeSchema(Map<String, Object> schema) {
    List<Map<String, Object>> fields = normalizeSchemaFields(schema);
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("schema.fields is required");
    }
    for (Map<String, Object> field : fields) {
      Object fieldIdSource =
          firstNonNull(
              field.get("id"), firstNonNull(field.get("field-id"), field.get("source-id")));
      Integer fieldId = asInteger(fieldIdSource);
      if (fieldId == null || fieldId <= 0) {
        throw new IllegalArgumentException("schema.fields entries require positive ids");
      }
      field.put("id", fieldId);
    }
    Integer schemaId = asInteger(schema.get("schema-id"));
    if (schemaId == null || schemaId < 0) {
      throw new IllegalArgumentException("schema requires schema-id");
    }
    Integer lastColumnId = asInteger(schema.get("last-column-id"));
    if (lastColumnId == null) {
      lastColumnId = maxSchemaFieldId(fields);
      if (lastColumnId == null || lastColumnId <= 0) {
        throw new IllegalArgumentException("schema requires last-column-id");
      }
      schema.put("last-column-id", lastColumnId);
    }
  }

  private static Integer maxSchemaFieldId(List<Map<String, Object>> fields) {
    int max = 0;
    for (Map<String, Object> field : fields) {
      Integer fieldId = asInteger(field.get("id"));
      if (fieldId == null) {
        fieldId = asInteger(firstNonNull(field.get("field-id"), field.get("source-id")));
      }
      if (fieldId != null && fieldId > max) {
        max = fieldId;
      }
    }
    return max == 0 ? null : max;
  }

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

  private static Object parseSchema(String json) {
    if (json == null || json.isBlank()) {
      throw new IllegalArgumentException("schemaJson is required");
    }
    try {
      return JSON.readValue(json, Object.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("schemaJson is invalid", e);
    }
  }

  static void normalizeSortOrder(Map<String, Object> order) {
    Object fieldsObj = order.get("fields");
    if (!(fieldsObj instanceof List<?> list)) {
      throw new IllegalArgumentException("write-order.fields is required");
    }
    List<Map<String, Object>> normalized = new ArrayList<>();
    for (Object entry : list) {
      if (!(entry instanceof Map<?, ?> mapEntry)) {
        throw new IllegalArgumentException("write-order.fields entries must be objects");
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
      if (!field.containsKey("source-id")) {
        throw new IllegalArgumentException("write-order.fields require source-id");
      }
      if (!field.containsKey("transform")) {
        throw new IllegalArgumentException("write-order.fields require transform");
      }
      if (!field.containsKey("direction")) {
        throw new IllegalArgumentException("write-order.fields require direction");
      }
      if (!field.containsKey("null-order")) {
        throw new IllegalArgumentException("write-order.fields require null-order");
      }
      field.put("direction", canonicalDirection(field.get("direction")));
      field.put("null-order", canonicalNullOrder(field.get("null-order")));
      normalized.add(field);
    }
    order.put("fields", normalized);
    Integer orderId = asInteger(order.get("order-id"));
    if (orderId == null) {
      throw new IllegalArgumentException("write-order requires order-id");
    }
    if (!normalized.isEmpty() && orderId <= 0) {
      throw new IllegalArgumentException("write-order order-id must be > 0 when fields exist");
    }
    if (normalized.isEmpty() && orderId != 0) {
      throw new IllegalArgumentException("write-order order-id must be 0 when fields are empty");
    }
    order.put("order-id", orderId);
  }

  private static String canonicalDirection(Object raw) {
    if (raw == null) {
      throw new IllegalArgumentException("write-order.fields require direction");
    }
    String value = raw.toString();
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("write-order.fields require direction");
    }
    String normalized = value.toLowerCase(Locale.ROOT).replaceAll("\\s+", "");
    if ("asc".equals(normalized)) {
      return "asc";
    }
    if ("desc".equals(normalized)) {
      return "desc";
    }
    throw new IllegalArgumentException("write-order direction must be asc or desc");
  }

  private static String canonicalNullOrder(Object raw) {
    if (raw == null) {
      throw new IllegalArgumentException("write-order.fields require null-order");
    }
    String value = raw.toString();
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("write-order.fields require null-order");
    }
    String normalized = value.replace('_', '-').toLowerCase(Locale.ROOT).replaceAll("\\s+", "");
    if ("nulls-first".equals(normalized) || "nullsfirst".equals(normalized)) {
      return "nulls-first";
    }
    if ("nulls-last".equals(normalized) || "nullslast".equals(normalized)) {
      return "nulls-last";
    }
    throw new IllegalArgumentException("write-order null-order must be nulls-first or nulls-last");
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
      if (snapshot.hasParentSnapshotId()) {
        entry.put("parent-snapshot-id", snapshot.getParentSnapshotId());
      }
      long sequenceNumber = snapshot.getSequenceNumber();
      if (sequenceNumber > 0) {
        entry.put("sequence-number", sequenceNumber);
      }
      long timestampMs =
          snapshot.hasUpstreamCreatedAt()
              ? snapshot.getUpstreamCreatedAt().getSeconds() * 1000L
              : 0L;
      entry.put("timestamp-ms", timestampMs);
      String manifestList = firstManifestList(snapshot);
      entry.put("manifest-list", manifestList == null ? "" : manifestList);
      int schemaId = snapshot.getSchemaId();
      if (schemaId >= 0) {
        entry.put("schema-id", schemaId);
      }
      Map<String, String> summary =
          new LinkedHashMap<>(SnapshotMetadataUtil.snapshotSummary(snapshot));
      String operation = SnapshotMetadataUtil.snapshotOperation(snapshot);
      if (operation != null && !operation.isBlank()) {
        summary.putIfAbsent("operation", operation);
      }
      entry.put("summary", summary);
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
              entry.put("type", ref.getType().toLowerCase(Locale.ROOT));
              if (ref.hasMaxReferenceAgeMs()) {
                entry.put("max-ref-age-ms", ref.getMaxReferenceAgeMs());
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

  private static String firstManifestList(Snapshot snapshot) {
    if (snapshot == null || snapshot.getManifestListCount() == 0) {
      return null;
    }
    for (String manifestList : snapshot.getManifestListList()) {
      if (manifestList != null && !manifestList.isBlank()) {
        return manifestList;
      }
    }
    return null;
  }
}
