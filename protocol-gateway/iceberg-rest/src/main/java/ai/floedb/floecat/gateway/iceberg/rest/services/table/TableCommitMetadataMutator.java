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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.normalizeFormatVersionForSnapshots;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@ApplicationScoped
public class TableCommitMetadataMutator {

  public TableMetadataView apply(TableMetadataView metadata, TableRequests.Commit req) {
    if (metadata == null || req == null || req.updates() == null || req.updates().isEmpty()) {
      return metadata;
    }
    TableMetadataView updated = mergeTableDefinitionUpdates(metadata, req);
    updated = preferRequestedSequence(updated, req);
    updated = preferSnapshotSequence(updated, req);
    updated = mergeSnapshotUpdates(updated, req);
    return normalizeResponseMetadata(updated);
  }

  private TableMetadataView mergeTableDefinitionUpdates(
      TableMetadataView metadata, TableRequests.Commit req) {
    Integer formatVersion = metadata.formatVersion();
    Integer lastColumnId = metadata.lastColumnId();
    Integer currentSchemaId = metadata.currentSchemaId();
    Integer defaultSpecId = metadata.defaultSpecId();
    Integer defaultSortOrderId = metadata.defaultSortOrderId();
    String tableLocation = metadata.location();
    List<Map<String, Object>> schemas =
        metadata.schemas() == null ? new ArrayList<>() : new ArrayList<>(metadata.schemas());
    List<Map<String, Object>> partitionSpecs =
        metadata.partitionSpecs() == null
            ? new ArrayList<>()
            : new ArrayList<>(metadata.partitionSpecs());
    List<Map<String, Object>> sortOrders =
        metadata.sortOrders() == null ? new ArrayList<>() : new ArrayList<>(metadata.sortOrders());
    Integer lastAddedSchemaId = null;
    Integer lastAddedSpecId = null;
    Integer lastAddedSortOrderId = null;

    for (Map<String, Object> update : req.updates()) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if ("upgrade-format-version".equals(action)) {
        Integer requested = asInteger(update.get("format-version"));
        if (requested != null) {
          formatVersion = requested;
        }
      } else if ("set-location".equals(action)) {
        String requestedLocation = asString(update.get("location"));
        if (requestedLocation != null && !requestedLocation.isBlank()) {
          tableLocation = requestedLocation;
        }
      } else if ("add-schema".equals(action)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> schema =
            update.get("schema") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
        if (schema != null && !schema.isEmpty()) {
          upsertById(schemas, new LinkedHashMap<>(schema), "schema-id");
          Integer schemaId = asInteger(schema.get("schema-id"));
          if (schemaId != null && schemaId >= 0) {
            lastAddedSchemaId = schemaId;
          }
        }
        Integer reqLastColumn = asInteger(update.get("last-column-id"));
        if (reqLastColumn == null) {
          reqLastColumn = maxSchemaFieldId(schema);
        }
        if (reqLastColumn != null) {
          lastColumnId = reqLastColumn;
        }
      } else if ("set-current-schema".equals(action)) {
        Integer schemaId =
            resolveLastAddedId(asInteger(update.get("schema-id")), lastAddedSchemaId);
        if (schemaId != null) {
          currentSchemaId = schemaId;
        }
      } else if ("add-spec".equals(action)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> spec =
            update.get("spec") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
        if (spec != null && !spec.isEmpty()) {
          upsertById(partitionSpecs, new LinkedHashMap<>(spec), "spec-id");
          Integer specId = asInteger(spec.get("spec-id"));
          if (specId != null && specId >= 0) {
            lastAddedSpecId = specId;
          }
        }
      } else if ("set-default-spec".equals(action)) {
        Integer specId = resolveLastAddedId(asInteger(update.get("spec-id")), lastAddedSpecId);
        if (specId != null) {
          defaultSpecId = specId;
        }
      } else if ("add-sort-order".equals(action)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> sortOrder =
            update.get("sort-order") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
        if (sortOrder != null && !sortOrder.isEmpty()) {
          upsertById(sortOrders, new LinkedHashMap<>(sortOrder), "order-id");
          Integer sortOrderId = asInteger(sortOrder.get("order-id"));
          if (sortOrderId == null) {
            sortOrderId = asInteger(sortOrder.get("sort-order-id"));
          }
          if (sortOrderId != null && sortOrderId >= 0) {
            lastAddedSortOrderId = sortOrderId;
          }
        }
      } else if ("set-default-sort-order".equals(action)) {
        Integer sortOrderId =
            resolveLastAddedId(asInteger(update.get("sort-order-id")), lastAddedSortOrderId);
        if (sortOrderId != null) {
          defaultSortOrderId = sortOrderId;
        }
      }
    }

    schemas = dedupeById(schemas, "schema-id");
    partitionSpecs = dedupeById(partitionSpecs, "spec-id");
    sortOrders = dedupeById(sortOrders, "order-id");

    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    props.put("format-version", Integer.toString(formatVersion));
    if (lastColumnId != null) {
      props.put("last-column-id", Integer.toString(lastColumnId));
    }
    if (currentSchemaId != null) {
      props.put("current-schema-id", Integer.toString(currentSchemaId));
    }
    if (defaultSpecId != null) {
      props.put("default-spec-id", Integer.toString(defaultSpecId));
    }
    if (defaultSortOrderId != null) {
      props.put("default-sort-order-id", Integer.toString(defaultSortOrderId));
    }
    if (tableLocation != null && !tableLocation.isBlank()) {
      props.put("location", tableLocation);
    }

    return new TableMetadataView(
        formatVersion,
        metadata.tableUuid(),
        tableLocation,
        metadata.metadataLocation(),
        metadata.lastUpdatedMs(),
        Map.copyOf(props),
        lastColumnId,
        currentSchemaId,
        defaultSpecId,
        metadata.lastPartitionId(),
        defaultSortOrderId,
        metadata.currentSnapshotId(),
        metadata.lastSequenceNumber(),
        List.copyOf(schemas),
        List.copyOf(partitionSpecs),
        List.copyOf(sortOrders),
        metadata.refs(),
        metadata.snapshotLog(),
        metadata.metadataLog(),
        metadata.statistics(),
        metadata.partitionStatistics(),
        metadata.snapshots());
  }

  private TableMetadataView preferSnapshotSequence(
      TableMetadataView metadata, TableRequests.Commit req) {
    Long requestedSequence = maxSequenceNumber(req);
    if (requestedSequence == null || requestedSequence <= 0) {
      return metadata;
    }
    Long existingSequence = metadata.lastSequenceNumber();
    Long latestSequence =
        existingSequence == null
            ? requestedSequence
            : Math.max(existingSequence, requestedSequence);
    if (existingSequence != null && existingSequence >= latestSequence) {
      return metadata;
    }
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    props.put("last-sequence-number", Long.toString(latestSequence));
    return new TableMetadataView(
        metadata.formatVersion(),
        metadata.tableUuid(),
        metadata.location(),
        metadata.metadataLocation(),
        metadata.lastUpdatedMs(),
        Map.copyOf(props),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.defaultSpecId(),
        metadata.lastPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.currentSnapshotId(),
        latestSequence,
        metadata.schemas(),
        metadata.partitionSpecs(),
        metadata.sortOrders(),
        metadata.refs(),
        metadata.snapshotLog(),
        metadata.metadataLog(),
        metadata.statistics(),
        metadata.partitionStatistics(),
        metadata.snapshots());
  }

  private TableMetadataView preferRequestedSequence(
      TableMetadataView metadata, TableRequests.Commit req) {
    Long requested = requestedSequenceNumber(req);
    if (requested == null || requested <= 0) {
      return metadata;
    }
    Long existing = metadata.lastSequenceNumber();
    if (existing != null && existing >= requested) {
      return metadata;
    }
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    props.put("last-sequence-number", Long.toString(requested));
    return new TableMetadataView(
        metadata.formatVersion(),
        metadata.tableUuid(),
        metadata.location(),
        metadata.metadataLocation(),
        metadata.lastUpdatedMs(),
        Map.copyOf(props),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.defaultSpecId(),
        metadata.lastPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.currentSnapshotId(),
        requested,
        metadata.schemas(),
        metadata.partitionSpecs(),
        metadata.sortOrders(),
        metadata.refs(),
        metadata.snapshotLog(),
        metadata.metadataLog(),
        metadata.statistics(),
        metadata.partitionStatistics(),
        metadata.snapshots());
  }

  private TableMetadataView mergeSnapshotUpdates(
      TableMetadataView metadata, TableRequests.Commit req) {
    List<Map<String, Object>> addedSnapshots = extractSnapshots(req.updates());
    if (addedSnapshots.isEmpty()) {
      return metadata;
    }
    List<Map<String, Object>> existing =
        metadata.snapshots() == null ? List.of() : metadata.snapshots();
    Map<Long, Map<String, Object>> merged = new LinkedHashMap<>();
    for (Map<String, Object> snapshot : existing) {
      Long id = snapshotId(snapshot);
      if (id != null) {
        merged.put(id, new LinkedHashMap<>(snapshot));
      }
    }
    for (Map<String, Object> snapshot : addedSnapshots) {
      Long id = snapshotId(snapshot);
      if (id == null) {
        continue;
      }
      merged.put(id, new LinkedHashMap<>(snapshot));
    }
    List<Map<String, Object>> updatedSnapshots =
        merged.isEmpty() ? List.of() : List.copyOf(merged.values());
    Long requestSequence = maxSequenceNumber(req);
    Long snapshotSequence = maxSequenceFromSnapshots(updatedSnapshots);
    Long existingSequence = metadata.lastSequenceNumber();
    Long maxSequence = maxNonNull(snapshotSequence, requestSequence, existingSequence);
    Integer formatVersion =
        normalizeFormatVersionForSnapshots(metadata.formatVersion(), requestSequence);
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    Long currentSnapshotId = desiredCurrentSnapshotId(metadata, req, updatedSnapshots, props);
    if (maxSequence != null && maxSequence > 0) {
      props.put("last-sequence-number", Long.toString(maxSequence));
    }
    if (currentSnapshotId != null && currentSnapshotId >= 0) {
      props.put("current-snapshot-id", Long.toString(currentSnapshotId));
    }
    return new TableMetadataView(
        formatVersion,
        metadata.tableUuid(),
        metadata.location(),
        metadata.metadataLocation(),
        metadata.lastUpdatedMs(),
        Map.copyOf(props),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.defaultSpecId(),
        metadata.lastPartitionId(),
        metadata.defaultSortOrderId(),
        currentSnapshotId != null ? currentSnapshotId : metadata.currentSnapshotId(),
        maxSequence != null ? maxSequence : metadata.lastSequenceNumber(),
        metadata.schemas(),
        metadata.partitionSpecs(),
        metadata.sortOrders(),
        metadata.refs(),
        metadata.snapshotLog(),
        metadata.metadataLog(),
        metadata.statistics(),
        metadata.partitionStatistics(),
        updatedSnapshots);
  }

  private TableMetadataView normalizeResponseMetadata(TableMetadataView metadata) {
    List<Map<String, Object>> schemas = normalizeSchemas(metadata.schemas());
    Integer currentSchemaId = normalizeCurrentSchemaId(metadata.currentSchemaId(), schemas);
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    if (currentSchemaId != null) {
      props.put("current-schema-id", Integer.toString(currentSchemaId));
    }
    return new TableMetadataView(
        metadata.formatVersion(),
        metadata.tableUuid(),
        metadata.location(),
        metadata.metadataLocation(),
        metadata.lastUpdatedMs(),
        Map.copyOf(props),
        metadata.lastColumnId(),
        currentSchemaId,
        metadata.defaultSpecId(),
        metadata.lastPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.currentSnapshotId(),
        metadata.lastSequenceNumber(),
        schemas,
        metadata.partitionSpecs(),
        metadata.sortOrders(),
        metadata.refs(),
        metadata.snapshotLog(),
        metadata.metadataLog(),
        metadata.statistics(),
        metadata.partitionStatistics(),
        metadata.snapshots());
  }

  private List<Map<String, Object>> normalizeSchemas(List<Map<String, Object>> schemas) {
    if (schemas == null || schemas.isEmpty()) {
      Map<String, Object> fallback = new LinkedHashMap<>();
      fallback.put("type", "struct");
      fallback.put("schema-id", 0);
      fallback.put("fields", List.of());
      return List.of(Map.copyOf(fallback));
    }
    for (Map<String, Object> schema : schemas) {
      if (schema == null) {
        continue;
      }
      Integer schemaId = asInteger(schema.get("schema-id"));
      if (schemaId != null && schemaId >= 0) {
        return schemas;
      }
    }
    List<Map<String, Object>> normalized = new ArrayList<>(schemas.size());
    boolean patched = false;
    for (Map<String, Object> schema : schemas) {
      if (!patched && schema != null) {
        Map<String, Object> copy = new LinkedHashMap<>(schema);
        copy.put("schema-id", 0);
        normalized.add(Map.copyOf(copy));
        patched = true;
      } else {
        normalized.add(schema);
      }
    }
    if (!patched) {
      Map<String, Object> fallback = new LinkedHashMap<>();
      fallback.put("type", "struct");
      fallback.put("schema-id", 0);
      fallback.put("fields", List.of());
      normalized.set(0, Map.copyOf(fallback));
    }
    return List.copyOf(normalized);
  }

  private Integer normalizeCurrentSchemaId(
      Integer currentSchemaId, List<Map<String, Object>> schemas) {
    Integer candidate = currentSchemaId != null && currentSchemaId >= 0 ? currentSchemaId : null;
    if (candidate != null && containsSchemaId(schemas, candidate)) {
      return candidate;
    }
    for (Map<String, Object> schema : schemas) {
      if (schema == null) {
        continue;
      }
      Integer schemaId = asInteger(schema.get("schema-id"));
      if (schemaId != null && schemaId >= 0) {
        return schemaId;
      }
    }
    return 0;
  }

  private boolean containsSchemaId(List<Map<String, Object>> schemas, int schemaId) {
    if (schemas == null || schemas.isEmpty()) {
      return false;
    }
    for (Map<String, Object> schema : schemas) {
      if (schema == null) {
        continue;
      }
      Integer value = asInteger(schema.get("schema-id"));
      if (value != null && value == schemaId) {
        return true;
      }
    }
    return false;
  }

  private Long maxSequenceNumber(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return null;
    }
    Long max = null;
    for (Map<String, Object> update : req.updates()) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if (!"add-snapshot".equals(action)) {
        continue;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> snapshot =
          update.get("snapshot") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
      if (snapshot == null) {
        continue;
      }
      Long sequence = parseLong(snapshot.get("sequence-number"));
      if (sequence == null || sequence <= 0) {
        continue;
      }
      max = max == null ? sequence : Math.max(max, sequence);
    }
    return max;
  }

  private Long requestedSequenceNumber(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return null;
    }
    Long max = null;
    for (Map<String, Object> update : req.updates()) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if (!"set-properties".equals(action)) {
        continue;
      }
      Map<String, String> updates = asStringMap(update.get("updates"));
      Long candidate = parseLong(updates.get("last-sequence-number"));
      if (candidate != null && candidate > 0) {
        max = max == null ? candidate : Math.max(max, candidate);
      }
    }
    return max;
  }

  private List<Map<String, Object>> extractSnapshots(List<Map<String, Object>> updates) {
    List<Map<String, Object>> out = new ArrayList<>();
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if (!"add-snapshot".equals(action)) {
        continue;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> snapshot =
          update.get("snapshot") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
      if (snapshot == null || snapshot.isEmpty()) {
        continue;
      }
      out.add(snapshot);
    }
    return out;
  }

  private Long desiredCurrentSnapshotId(
      TableMetadataView metadata,
      TableRequests.Commit req,
      List<Map<String, Object>> updatedSnapshots,
      Map<String, String> props) {
    Long propertyCurrentSnapshotId = parseLong(props.get("current-snapshot-id"));
    if (propertyCurrentSnapshotId != null && propertyCurrentSnapshotId >= 0) {
      return propertyCurrentSnapshotId;
    }
    Long requestedMainRefSnapshotId = requestedMainRefSnapshotId(req);
    if (requestedMainRefSnapshotId != null && requestedMainRefSnapshotId >= 0) {
      return requestedMainRefSnapshotId;
    }
    if (metadata.currentSnapshotId() == null) {
      return latestSnapshotId(updatedSnapshots);
    }
    return metadata.currentSnapshotId();
  }

  private Long requestedMainRefSnapshotId(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return null;
    }
    for (Map<String, Object> update : req.updates()) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if (!"set-snapshot-ref".equals(action)) {
        continue;
      }
      String refName = asString(update.get("ref-name"));
      if (!"main".equals(refName)) {
        continue;
      }
      return parseLong(update.get("snapshot-id"));
    }
    return null;
  }

  private Long latestSnapshotId(List<Map<String, Object>> snapshots) {
    Long latest = null;
    for (Map<String, Object> snapshot : snapshots) {
      Long id = snapshotId(snapshot);
      if (id != null && id >= 0) {
        latest = id;
      }
    }
    return latest;
  }

  private Long snapshotId(Map<String, Object> snapshot) {
    if (snapshot == null) {
      return null;
    }
    return parseLong(snapshot.get("snapshot-id"));
  }

  private Long maxSequenceFromSnapshots(List<Map<String, Object>> snapshots) {
    Long max = null;
    for (Map<String, Object> snapshot : snapshots) {
      if (snapshot == null) {
        continue;
      }
      Long seq = parseLong(snapshot.get("sequence-number"));
      if (seq == null || seq <= 0) {
        continue;
      }
      max = max == null ? seq : Math.max(max, seq);
    }
    return max;
  }

  private Long maxNonNull(Long... values) {
    Long max = null;
    if (values == null) {
      return null;
    }
    for (Long value : values) {
      if (value == null || value <= 0) {
        continue;
      }
      max = max == null ? value : Math.max(max, value);
    }
    return max;
  }

  private Integer resolveLastAddedId(Integer requested, Integer lastAdded) {
    if (requested == null) {
      return null;
    }
    if (requested == -1) {
      return lastAdded;
    }
    return requested;
  }

  private Long parseLong(Object value) {
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

  private Integer asInteger(Object value) {
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

  @SuppressWarnings("unchecked")
  private Map<String, String> asStringMap(Object value) {
    if (!(value instanceof Map<?, ?> map) || map.isEmpty()) {
      return Map.of();
    }
    Map<String, String> converted = new LinkedHashMap<>();
    map.forEach(
        (k, v) -> {
          String key = asString(k);
          String strValue = asString(v);
          if (key != null && strValue != null) {
            converted.put(key, strValue);
          }
        });
    return converted;
  }

  @SuppressWarnings("unchecked")
  private Integer maxSchemaFieldId(Map<String, Object> schema) {
    if (schema == null || schema.isEmpty()) {
      return null;
    }
    Object rawFields = schema.get("fields");
    if (!(rawFields instanceof List<?> fields)) {
      return null;
    }
    Integer max = null;
    for (Object fieldObj : fields) {
      if (!(fieldObj instanceof Map<?, ?> fieldMap)) {
        continue;
      }
      Integer fieldId = asInteger(((Map<String, Object>) fieldMap).get("id"));
      if (fieldId == null) {
        continue;
      }
      max = max == null ? fieldId : Math.max(max, fieldId);
    }
    return max;
  }

  private void upsertById(
      List<Map<String, Object>> entries, Map<String, Object> candidate, String idKey) {
    if (entries == null || candidate == null || candidate.isEmpty()) {
      return;
    }
    Object candidateId = candidate.get(idKey);
    if (candidateId == null) {
      entries.add(candidate);
      return;
    }
    for (int i = 0; i < entries.size(); i++) {
      Map<String, Object> existing = entries.get(i);
      if (existing == null) {
        continue;
      }
      if (Objects.equals(existing.get(idKey), candidateId)) {
        entries.set(i, candidate);
        return;
      }
    }
    entries.add(candidate);
  }

  private List<Map<String, Object>> dedupeById(List<Map<String, Object>> entries, String idKey) {
    if (entries == null || entries.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    Set<Object> ids = new LinkedHashSet<>();
    for (int i = entries.size() - 1; i >= 0; i--) {
      Map<String, Object> entry = entries.get(i);
      if (entry == null || entry.isEmpty()) {
        continue;
      }
      Object id = entry.get(idKey);
      if (id != null && !ids.add(id)) {
        continue;
      }
      out.add(0, entry);
    }
    return out;
  }
}
