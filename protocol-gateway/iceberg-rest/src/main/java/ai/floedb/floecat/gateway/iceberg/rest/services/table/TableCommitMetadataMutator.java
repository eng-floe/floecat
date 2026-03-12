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
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.maxFieldId;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.normalizeFormatVersion;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.normalizeFormatVersionForSnapshots;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataListUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataViews;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class TableCommitMetadataMutator {

  public TableMetadataView apply(TableMetadataView metadata, TableRequests.Commit req) {
    if (metadata == null || req == null || req.updates() == null || req.updates().isEmpty()) {
      return metadata;
    }
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(req);
    TableMetadataView updated = mergeTableDefinitionUpdates(metadata, req);
    updated = preferSequenceAtLeast(updated, parsed.requestedSequenceNumber());
    updated = preferSequenceAtLeast(updated, parsed.maxSnapshotSequenceNumber());
    updated = mergeSnapshotUpdates(updated, parsed);
    return normalizeResponseMetadata(updated);
  }

  private TableMetadataView mergeTableDefinitionUpdates(
      TableMetadataView metadata, TableRequests.Commit req) {
    Integer formatVersion = metadata.formatVersion();
    Integer lastColumnId = metadata.lastColumnId();
    Integer currentSchemaId = metadata.currentSchemaId();
    Integer defaultSpecId = metadata.defaultSpecId();
    Integer lastPartitionId = metadata.lastPartitionId();
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
      CommitUpdateInspector.UpdateAction action = CommitUpdateInspector.actionTypeOf(update);
      if (action == null) {
        continue;
      }
      switch (action) {
        case UPGRADE_FORMAT_VERSION -> {
          Integer requested = asInteger(update.get("format-version"));
          if (requested != null) {
            formatVersion = requested;
          }
        }
        case SET_LOCATION -> {
          String requestedLocation = asString(update.get("location"));
          if (requestedLocation != null && !requestedLocation.isBlank()) {
            tableLocation = requestedLocation;
          }
        }
        case ADD_SCHEMA -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> schema =
              update.get("schema") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          if (schema != null && !schema.isEmpty()) {
            MetadataListUtil.upsertById(schemas, new LinkedHashMap<>(schema), "schema-id");
            Integer schemaId = asInteger(schema.get("schema-id"));
            if (schemaId != null && schemaId >= 0) {
              lastAddedSchemaId = schemaId;
            }
          }
          Integer reqLastColumn = asInteger(update.get("last-column-id"));
          if (reqLastColumn == null) {
            reqLastColumn = maxFieldId(schema, "fields", "id");
          }
          if (reqLastColumn != null) {
            lastColumnId = reqLastColumn;
          }
        }
        case SET_CURRENT_SCHEMA -> {
          Integer schemaId =
              resolveLastAddedId(asInteger(update.get("schema-id")), lastAddedSchemaId);
          if (schemaId != null) {
            currentSchemaId = schemaId;
          }
        }
        case ADD_SPEC -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> spec =
              update.get("spec") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          if (spec != null && !spec.isEmpty()) {
            MetadataListUtil.upsertById(partitionSpecs, new LinkedHashMap<>(spec), "spec-id");
            Integer specId = asInteger(spec.get("spec-id"));
            if (specId != null && specId >= 0) {
              lastAddedSpecId = specId;
            }
            Integer partitionFieldMax = maxPartitionFieldId(spec);
            if (partitionFieldMax != null && partitionFieldMax >= 0) {
              lastPartitionId = partitionFieldMax;
            }
          }
        }
        case SET_DEFAULT_SPEC -> {
          Integer specId = resolveLastAddedId(asInteger(update.get("spec-id")), lastAddedSpecId);
          if (specId != null) {
            defaultSpecId = specId;
          }
        }
        case ADD_SORT_ORDER -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> sortOrder =
              update.get("sort-order") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          if (sortOrder != null && !sortOrder.isEmpty()) {
            MetadataListUtil.upsertById(sortOrders, new LinkedHashMap<>(sortOrder), "order-id");
            Integer sortOrderId = asInteger(sortOrder.get("order-id"));
            if (sortOrderId == null) {
              sortOrderId = asInteger(sortOrder.get("sort-order-id"));
            }
            if (sortOrderId != null && sortOrderId >= 0) {
              lastAddedSortOrderId = sortOrderId;
            }
          }
        }
        case SET_DEFAULT_SORT_ORDER -> {
          Integer sortOrderId =
              resolveLastAddedId(asInteger(update.get("sort-order-id")), lastAddedSortOrderId);
          if (sortOrderId != null) {
            defaultSortOrderId = sortOrderId;
          }
        }
        default -> {
          // Ignore non table-definition actions.
        }
      }
    }

    schemas = MetadataListUtil.dedupeById(schemas, "schema-id");
    partitionSpecs = MetadataListUtil.dedupeById(partitionSpecs, "spec-id");
    sortOrders = MetadataListUtil.dedupeById(sortOrders, "order-id");
    formatVersion = normalizeFormatVersion(formatVersion, null);

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
    if (lastPartitionId != null) {
      props.put("last-partition-id", Integer.toString(lastPartitionId));
    }
    if (defaultSortOrderId != null) {
      props.put("default-sort-order-id", Integer.toString(defaultSortOrderId));
    }
    if (tableLocation != null && !tableLocation.isBlank()) {
      props.put("location", tableLocation);
    }

    return copyMetadata(
        metadata,
        formatVersion,
        tableLocation,
        metadata.metadataLocation(),
        Map.copyOf(props),
        lastColumnId,
        currentSchemaId,
        defaultSpecId,
        lastPartitionId,
        defaultSortOrderId,
        metadata.currentSnapshotId(),
        metadata.lastSequenceNumber(),
        List.copyOf(schemas),
        List.copyOf(partitionSpecs),
        List.copyOf(sortOrders),
        metadata.snapshots());
  }

  private TableMetadataView preferSequenceAtLeast(
      TableMetadataView metadata, Long candidateSequence) {
    if (candidateSequence == null || candidateSequence <= 0) {
      return metadata;
    }
    Long existing = metadata.lastSequenceNumber();
    long effective = existing == null ? candidateSequence : Math.max(existing, candidateSequence);
    if (existing != null && existing >= effective) {
      return metadata;
    }
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    props.put("last-sequence-number", Long.toString(effective));
    return copyMetadata(
        metadata,
        metadata.formatVersion(),
        metadata.location(),
        metadata.metadataLocation(),
        Map.copyOf(props),
        metadata.lastColumnId(),
        metadata.currentSchemaId(),
        metadata.defaultSpecId(),
        metadata.lastPartitionId(),
        metadata.defaultSortOrderId(),
        metadata.currentSnapshotId(),
        effective,
        metadata.schemas(),
        metadata.partitionSpecs(),
        metadata.sortOrders(),
        metadata.snapshots());
  }

  private TableMetadataView mergeSnapshotUpdates(
      TableMetadataView metadata, CommitUpdateInspector.Parsed parsed) {
    List<Map<String, Object>> addedSnapshots = parsed.addedSnapshots();
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
    Long requestSequence = parsed.maxSnapshotSequenceNumber();
    Long snapshotSequence = maxSequenceFromSnapshots(updatedSnapshots);
    Long existingSequence = metadata.lastSequenceNumber();
    Long maxSequence = maxNonNull(snapshotSequence, requestSequence, existingSequence);
    Integer formatVersion =
        normalizeFormatVersionForSnapshots(metadata.formatVersion(), requestSequence);
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    Long currentSnapshotId = desiredCurrentSnapshotId(metadata, parsed, updatedSnapshots, props);
    if (maxSequence != null && maxSequence > 0) {
      props.put("last-sequence-number", Long.toString(maxSequence));
    }
    if (currentSnapshotId != null && currentSnapshotId >= 0) {
      props.put("current-snapshot-id", Long.toString(currentSnapshotId));
    }
    return copyMetadata(
        metadata,
        formatVersion,
        metadata.location(),
        metadata.metadataLocation(),
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
    return copyMetadata(
        metadata,
        metadata.formatVersion(),
        metadata.location(),
        metadata.metadataLocation(),
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
        metadata.snapshots());
  }

  private TableMetadataView copyMetadata(
      TableMetadataView base,
      Integer formatVersion,
      String location,
      String metadataLocation,
      Map<String, String> properties,
      Integer lastColumnId,
      Integer currentSchemaId,
      Integer defaultSpecId,
      Integer lastPartitionId,
      Integer defaultSortOrderId,
      Long currentSnapshotId,
      Long lastSequenceNumber,
      List<Map<String, Object>> schemas,
      List<Map<String, Object>> partitionSpecs,
      List<Map<String, Object>> sortOrders,
      List<Map<String, Object>> snapshots) {
    return TableMetadataViews.copy(base)
        .formatVersion(formatVersion)
        .location(location)
        .metadataLocation(metadataLocation)
        .properties(properties)
        .lastColumnId(lastColumnId)
        .currentSchemaId(currentSchemaId)
        .defaultSpecId(defaultSpecId)
        .lastPartitionId(lastPartitionId)
        .defaultSortOrderId(defaultSortOrderId)
        .currentSnapshotId(currentSnapshotId)
        .lastSequenceNumber(lastSequenceNumber)
        .schemas(schemas)
        .partitionSpecs(partitionSpecs)
        .sortOrders(sortOrders)
        .snapshots(snapshots)
        .build();
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

  private Long desiredCurrentSnapshotId(
      TableMetadataView metadata,
      CommitUpdateInspector.Parsed parsed,
      List<Map<String, Object>> updatedSnapshots,
      Map<String, String> props) {
    Long propertyCurrentSnapshotId = parseLong(props.get("current-snapshot-id"));
    if (propertyCurrentSnapshotId != null && propertyCurrentSnapshotId >= 0) {
      return propertyCurrentSnapshotId;
    }
    Long requestedMainRefSnapshotId = parsed == null ? null : parsed.requestedMainRefSnapshotId();
    if (requestedMainRefSnapshotId != null && requestedMainRefSnapshotId >= 0) {
      return requestedMainRefSnapshotId;
    }
    if (metadata.currentSnapshotId() == null) {
      return latestSnapshotId(updatedSnapshots);
    }
    return metadata.currentSnapshotId();
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

  @SuppressWarnings("unchecked")
  private Integer maxPartitionFieldId(Map<String, Object> spec) {
    if (spec == null || spec.isEmpty()) {
      return null;
    }
    Object rawFields = spec.get("fields");
    if (!(rawFields instanceof List<?> fields) || fields.isEmpty()) {
      return 0;
    }
    Integer max = null;
    for (Object fieldObj : fields) {
      if (!(fieldObj instanceof Map<?, ?> fieldMap)) {
        continue;
      }
      Integer fieldId = asInteger(((Map<String, Object>) fieldMap).get("field-id"));
      if (fieldId == null) {
        continue;
      }
      max = max == null ? fieldId : Math.max(max, fieldId);
    }
    return max == null ? 0 : max;
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
}
