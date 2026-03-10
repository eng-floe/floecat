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

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asInteger;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asLong;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asObjectMap;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.firstNonNull;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TablePropertyService {
  private static final Logger LOG = Logger.getLogger(TablePropertyService.class);
  private static final Set<String> RESERVED_REMOVE_PROPERTIES =
      Set.of("format-version", "format_version");

  public void stripMetadataLocation(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return;
    }
    // no-op: metadata-location updates are carried through standard set-properties updates
  }

  public boolean hasPropertyUpdates(TableRequests.Commit req) {
    if (req == null || req.updates() == null || req.updates().isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : req.updates()) {
      String action = asString(update == null ? null : update.get("action"));
      if ("set-properties".equals(action) || "remove-properties".equals(action)) {
        return true;
      }
    }
    return false;
  }

  public Response applyPropertyUpdates(
      Map<String, String> properties, List<Map<String, Object>> updates) {
    if (updates == null) {
      return null;
    }
    for (Map<String, Object> update : updates) {
      if (update == null) {
        return validationError("commit update entry cannot be null");
      }
      String action = asString(update.get("action"));
      if (action == null) {
        return validationError("commit update missing action");
      }
      switch (action) {
        case "set-properties" -> {
          Map<String, String> toSet = new LinkedHashMap<>(asStringMap(update.get("updates")));
          if (toSet.isEmpty()) {
            return validationError("set-properties requires updates");
          }
          stripMetadataLocation(toSet);
          if (!toSet.isEmpty()) {
            properties.putAll(toSet);
          }
        }
        case "remove-properties" -> {
          List<String> removals = asStringList(update.get("removals"));
          if (removals.isEmpty()) {
            return validationError("remove-properties requires removals");
          }
          for (String removal : removals) {
            if (RESERVED_REMOVE_PROPERTIES.contains(removal)) {
              LOG.debugf("Ignored commit removal of reserved property %s", removal);
              continue;
            }
            properties.remove(removal);
          }
        }
        default -> {
          // ignore
        }
      }
    }
    return null;
  }

  public record PropertyUpdateResult(Map<String, String> properties, Response error) {
    public boolean hasError() {
      return error != null;
    }
  }

  public PropertyUpdateResult applyCommitPropertyUpdates(
      Supplier<Table> tableSupplier,
      Map<String, String> mergedProps,
      List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return new PropertyUpdateResult(mergedProps, null);
    }
    Map<String, String> targetProps = mergedProps;
    if (hasPropertyUpdates(updates)) {
      if (targetProps == null) {
        targetProps = ensurePropertyMap(tableSupplier, null);
      }
      Response updateError = applyPropertyUpdates(targetProps, updates);
      if (updateError != null) {
        return new PropertyUpdateResult(null, updateError);
      }
    }
    targetProps = applySnapshotPropertyUpdates(targetProps, tableSupplier, updates);
    targetProps = applyRefPropertyUpdates(targetProps, tableSupplier, updates);
    targetProps = applyTableDefinitionPropertyUpdates(targetProps, tableSupplier, updates);
    return new PropertyUpdateResult(targetProps, null);
  }

  public Map<String, String> ensurePropertyMap(
      Supplier<Table> tableSupplier, Map<String, String> current) {
    if (current != null) {
      return current;
    }
    Table table = tableSupplier.get();
    if (table == null || table.getPropertiesMap().isEmpty()) {
      return new LinkedHashMap<>();
    }
    return new LinkedHashMap<>(table.getPropertiesMap());
  }

  public Response applyLocationUpdate(
      TableSpec.Builder spec,
      FieldMask.Builder mask,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return null;
    }
    String location = null;
    for (Map<String, Object> update : updates) {
      String action = asString(update == null ? null : update.get("action"));
      if (!"set-location".equals(action)) {
        continue;
      }
      if (location != null) {
        return validationError("set-location may only be specified once");
      }
      String value = asString(update.get("location"));
      if (value == null || value.isBlank()) {
        return validationError("set-location requires location");
      }
      location = value;
    }
    if (location == null) {
      return null;
    }
    Table existing = tableSupplier.get();
    if (existing == null || !existing.hasUpstream()) {
      LOG.debug("Skipping set-location update for table without upstream reference");
      return null;
    }
    UpstreamRef upstream = existing.getUpstream();
    UpstreamRef.Builder builder = upstream.toBuilder().setUri(location);
    spec.setUpstream(builder.build());
    mask.addPaths("upstream.uri");
    return null;
  }

  private Response validationError(String message) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(new IcebergErrorResponse(new IcebergError(message, "ValidationException", 400)))
        .build();
  }

  private static Map<String, String> asStringMap(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return Map.of();
    }
    Map<String, String> result = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
      result.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return result;
  }

  private static List<String> asStringList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    return list.stream()
        .filter(v -> v != null && !v.toString().isBlank())
        .map(Object::toString)
        .toList();
  }

  private boolean hasPropertyUpdates(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : updates) {
      String action = asString(update == null ? null : update.get("action"));
      if ("set-properties".equals(action) || "remove-properties".equals(action)) {
        return true;
      }
    }
    return false;
  }

  private Map<String, String> applyRefPropertyUpdates(
      Map<String, String> mergedProps,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates) {
    Map<String, Map<String, Object>> refs = loadStoredRefs(mergedProps, tableSupplier);
    boolean mutated = false;
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if ("set-snapshot-ref".equals(action)) {
        String refName = asString(update.get("ref-name"));
        Long snapshotId = asLong(update.get("snapshot-id"));
        if (refName == null || refName.isBlank() || snapshotId == null || snapshotId <= 0) {
          continue;
        }
        Map<String, Object> refMap = new LinkedHashMap<>();
        refMap.put("snapshot-id", snapshotId);
        String type = asString(update.get("type"));
        if (type != null && !type.isBlank()) {
          refMap.put("type", type.toLowerCase(Locale.ROOT));
        }
        Long maxRefAge =
            asLong(firstNonNull(update.get("max-ref-age-ms"), update.get("max_ref_age_ms")));
        if (maxRefAge != null) {
          refMap.put("max-ref-age-ms", maxRefAge);
        }
        Long maxSnapshotAge =
            asLong(
                firstNonNull(update.get("max-snapshot-age-ms"), update.get("max_snapshot_age_ms")));
        if (maxSnapshotAge != null) {
          refMap.put("max-snapshot-age-ms", maxSnapshotAge);
        }
        Integer minSnapshots =
            asInteger(
                firstNonNull(
                    update.get("min-snapshots-to-keep"), update.get("min_snapshots_to_keep")));
        if (minSnapshots != null) {
          refMap.put("min-snapshots-to-keep", minSnapshots);
        }
        refs.put(refName, refMap);
        mutated = true;
      } else if ("remove-snapshot-ref".equals(action)) {
        String refName = asString(update.get("ref-name"));
        if (refName != null && refs.remove(refName) != null) {
          mutated = true;
        }
      }
    }
    if (!mutated) {
      return mergedProps;
    }
    Map<String, String> targetProps =
        mergedProps == null
            ? new LinkedHashMap<>(tableSupplier.get().getPropertiesMap())
            : mergedProps;
    if (refs.isEmpty()) {
      targetProps.remove(RefPropertyUtil.PROPERTY_KEY);
    } else {
      targetProps.put(RefPropertyUtil.PROPERTY_KEY, RefPropertyUtil.encode(refs));
    }
    Long mainSnapshotId = mainRefSnapshotId(refs);
    if (mainSnapshotId != null && mainSnapshotId > 0) {
      targetProps.put("current-snapshot-id", Long.toString(mainSnapshotId));
    } else {
      targetProps.remove("current-snapshot-id");
    }
    return targetProps;
  }

  private Map<String, String> applySnapshotPropertyUpdates(
      Map<String, String> mergedProps,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates) {
    Long latestSnapshotId = null;
    Long latestSequence = null;
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
      Long snapshotId = asLong(snapshot.get("snapshot-id"));
      if (snapshotId != null && snapshotId > 0) {
        latestSnapshotId = snapshotId;
      }
      Long sequenceNumber = asLong(snapshot.get("sequence-number"));
      if (sequenceNumber != null && sequenceNumber > 0) {
        latestSequence =
            latestSequence == null ? sequenceNumber : Math.max(latestSequence, sequenceNumber);
      }
    }
    if (latestSnapshotId == null || latestSnapshotId <= 0) {
      return mergedProps;
    }
    Map<String, String> targetProps =
        mergedProps == null
            ? new LinkedHashMap<>(tableSupplier.get().getPropertiesMap())
            : mergedProps;
    targetProps.put("current-snapshot-id", Long.toString(latestSnapshotId));
    if (latestSequence != null && latestSequence > 0) {
      Long existing = asLong(targetProps.get("last-sequence-number"));
      if (existing == null || existing < latestSequence) {
        targetProps.put("last-sequence-number", Long.toString(latestSequence));
      }
    }
    return targetProps;
  }

  private Map<String, String> applyTableDefinitionPropertyUpdates(
      Map<String, String> mergedProps,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates) {
    Map<String, String> targetProps =
        mergedProps == null
            ? new LinkedHashMap<>(tableSupplier.get().getPropertiesMap())
            : mergedProps;
    boolean mutated = false;
    Integer lastAddedSchemaId = null;
    Integer lastAddedSpecId = null;
    Integer lastAddedSortOrderId = null;
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if ("upgrade-format-version".equals(action)) {
        Integer version = asInteger(update.get("format-version"));
        if (version != null && version > 0) {
          targetProps.put("format-version", Integer.toString(version));
          mutated = true;
        }
      } else if ("add-schema".equals(action)) {
        Map<String, Object> schema = asObjectMap(update.get("schema"));
        Integer schemaId = asInteger(schema == null ? null : schema.get("schema-id"));
        if (schemaId != null && schemaId >= 0) {
          lastAddedSchemaId = schemaId;
        }
        Integer lastColumnId = asInteger(update.get("last-column-id"));
        if (lastColumnId == null) {
          lastColumnId = maxSchemaFieldId(schema);
        }
        if (lastColumnId != null && lastColumnId >= 0) {
          targetProps.put("last-column-id", Integer.toString(lastColumnId));
          mutated = true;
        }
      } else if ("set-current-schema".equals(action)) {
        Integer schemaId =
            resolveLastAddedId(asInteger(update.get("schema-id")), lastAddedSchemaId);
        if (schemaId != null && schemaId >= 0) {
          targetProps.put("current-schema-id", Integer.toString(schemaId));
          mutated = true;
        }
      } else if ("add-spec".equals(action)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> spec =
            update.get("spec") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
        if (spec != null) {
          Integer specId = asInteger(spec.get("spec-id"));
          if (specId != null && specId >= 0) {
            lastAddedSpecId = specId;
          }
          Integer partitionFieldMax = maxPartitionFieldId(spec);
          if (partitionFieldMax != null && partitionFieldMax >= 0) {
            targetProps.put("last-partition-id", Integer.toString(partitionFieldMax));
            mutated = true;
          }
        }
      } else if ("set-default-spec".equals(action)) {
        Integer specId = resolveLastAddedId(asInteger(update.get("spec-id")), lastAddedSpecId);
        if (specId != null && specId >= 0) {
          targetProps.put("default-spec-id", Integer.toString(specId));
          mutated = true;
        }
      } else if ("add-sort-order".equals(action)) {
        Map<String, Object> sortOrder = asObjectMap(update.get("sort-order"));
        Integer sortOrderId =
            asInteger(
                firstNonNull(
                    sortOrder == null ? null : sortOrder.get("sort-order-id"),
                    sortOrder == null ? null : sortOrder.get("order-id")));
        if (sortOrderId != null && sortOrderId >= 0) {
          lastAddedSortOrderId = sortOrderId;
        }
      } else if ("set-default-sort-order".equals(action)) {
        Integer orderId =
            resolveLastAddedId(asInteger(update.get("sort-order-id")), lastAddedSortOrderId);
        if (orderId != null && orderId >= 0) {
          targetProps.put("default-sort-order-id", Integer.toString(orderId));
          mutated = true;
        }
      } else if ("set-location".equals(action)) {
        String location = asString(update.get("location"));
        if (location != null && !location.isBlank()) {
          targetProps.put("location", location);
          mutated = true;
        }
      }
    }
    if (!mutated) {
      return mergedProps;
    }
    return targetProps;
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
  private Integer maxSchemaFieldId(Map<String, Object> schema) {
    if (schema == null || schema.isEmpty()) {
      return null;
    }
    Object rawFields = schema.get("fields");
    if (!(rawFields instanceof List<?> fields) || fields.isEmpty()) {
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

  private Map<String, Map<String, Object>> loadStoredRefs(
      Map<String, String> mergedProps, Supplier<Table> tableSupplier) {
    String encoded =
        mergedProps != null
            ? mergedProps.get(RefPropertyUtil.PROPERTY_KEY)
            : tableSupplier.get().getPropertiesMap().get(RefPropertyUtil.PROPERTY_KEY);
    return RefPropertyUtil.decode(encoded);
  }

  private Long mainRefSnapshotId(Map<String, Map<String, Object>> refs) {
    if (refs == null || refs.isEmpty()) {
      return null;
    }
    Map<String, Object> main = refs.get("main");
    if (main == null || main.isEmpty()) {
      return null;
    }
    return asLong(main.get("snapshot-id"));
  }

  public Table applyCanonicalMetadataProperties(Table plannedTable, TableMetadataView metadata) {
    if (plannedTable == null || metadata == null) {
      return plannedTable;
    }
    Map<String, String> props = new LinkedHashMap<>(plannedTable.getPropertiesMap());
    putIntProperty(props, "format-version", metadata.formatVersion());
    putIntProperty(props, "last-column-id", metadata.lastColumnId());
    putIntProperty(props, "current-schema-id", metadata.currentSchemaId());
    putIntProperty(props, "default-spec-id", metadata.defaultSpecId());
    putIntProperty(props, "last-partition-id", metadata.lastPartitionId());
    putIntProperty(props, "default-sort-order-id", metadata.defaultSortOrderId());
    putLongProperty(props, "last-sequence-number", metadata.lastSequenceNumber());
    syncLongProperty(props, "current-snapshot-id", metadata.currentSnapshotId());
    putStringProperty(props, "table-uuid", metadata.tableUuid());
    putStringProperty(props, "location", metadata.location());
    return plannedTable.toBuilder().clearProperties().putAllProperties(props).build();
  }

  private void putIntProperty(Map<String, String> props, String key, Integer value) {
    if (props == null || key == null || value == null || value < 0) {
      return;
    }
    props.put(key, Integer.toString(value));
  }

  private void putLongProperty(Map<String, String> props, String key, Long value) {
    if (props == null || key == null || value == null || value < 0) {
      return;
    }
    props.put(key, Long.toString(value));
  }

  private void putStringProperty(Map<String, String> props, String key, String value) {
    if (props == null || key == null || value == null || value.isBlank()) {
      return;
    }
    props.put(key, value);
  }

  private void syncLongProperty(Map<String, String> props, String key, Long value) {
    if (props == null || key == null) {
      return;
    }
    if (value == null || value < 0) {
      props.remove(key);
      return;
    }
    props.put(key, Long.toString(value));
  }

  // TableMappingUtil provides asString.
}
