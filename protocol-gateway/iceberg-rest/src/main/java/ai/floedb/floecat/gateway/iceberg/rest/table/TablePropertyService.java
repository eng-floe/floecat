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
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asString;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asStringList;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asStringMap;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.maxFieldId;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.normalizeFormatVersion;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.RefPropertyUtil;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TablePropertyService {
  private static final Logger LOG = Logger.getLogger(TablePropertyService.class);
  private static final Set<String> RESERVED_REMOVE_PROPERTIES = Set.of("format-version");
  private static final Set<String> TABLE_DEFINITION_PROPERTY_KEYS =
      Set.of(
          "format-version",
          "last-column-id",
          "current-schema-id",
          "default-spec-id",
          "last-partition-id",
          "default-sort-order-id");

  public void stripMetadataLocation(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return;
    }
    // no-op: metadata-location updates are carried through standard set-properties updates
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
      CommitUpdateInspector.UpdateAction action = CommitUpdateInspector.actionTypeOf(update);
      if (action == null) {
        return validationError("commit update missing action");
      }
      switch (action) {
        case SET_PROPERTIES -> {
          Map<String, String> toSet = new LinkedHashMap<>(asStringMap(update.get("updates")));
          if (toSet.isEmpty()) {
            return validationError(
                CommitUpdateInspector.ACTION_SET_PROPERTIES + " requires updates");
          }
          stripMetadataLocation(toSet);
          if (!toSet.isEmpty()) {
            properties.putAll(toSet);
          }
        }
        case REMOVE_PROPERTIES -> {
          List<String> removals = asStringList(update.get("removals"));
          if (removals.isEmpty()) {
            return validationError(
                CommitUpdateInspector.ACTION_REMOVE_PROPERTIES + " requires removals");
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
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspectUpdates(updates);
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
    targetProps = applySnapshotPropertyUpdates(targetProps, tableSupplier, parsed);
    targetProps = applyRefPropertyUpdates(targetProps, tableSupplier, parsed);
    targetProps = applyLocationPropertyUpdates(targetProps, tableSupplier, updates);
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
      String action = CommitUpdateInspector.actionOf(update);
      if (!CommitUpdateInspector.ACTION_SET_LOCATION.equals(action)) {
        continue;
      }
      if (location != null) {
        return validationError(
            CommitUpdateInspector.ACTION_SET_LOCATION + " may only be specified once");
      }
      String value = asString(update.get("location"));
      if (value == null || value.isBlank()) {
        return validationError(CommitUpdateInspector.ACTION_SET_LOCATION + " requires location");
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

  private Map<String, String> applyLocationPropertyUpdates(
      Map<String, String> mergedProps,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return mergedProps;
    }
    String location = null;
    for (Map<String, Object> update : updates) {
      String action = CommitUpdateInspector.actionOf(update);
      if (!CommitUpdateInspector.ACTION_SET_LOCATION.equals(action)) {
        continue;
      }
      String value = asString(update.get("location"));
      if (value == null || value.isBlank()) {
        continue;
      }
      location = value;
      break;
    }
    if (location == null) {
      return mergedProps;
    }
    Map<String, String> targetProps = ensurePropertyMap(tableSupplier, mergedProps);
    targetProps.put("location", location);
    return targetProps;
  }

  private Response validationError(String message) {
    return IcebergErrorResponses.validation(message);
  }

  private boolean hasPropertyUpdates(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : updates) {
      CommitUpdateInspector.UpdateAction action = CommitUpdateInspector.actionTypeOf(update);
      if (action != null && action.isPropertyAction()) {
        return true;
      }
    }
    return false;
  }

  private Map<String, String> applyRefPropertyUpdates(
      Map<String, String> mergedProps,
      Supplier<Table> tableSupplier,
      CommitUpdateInspector.Parsed parsed) {
    Map<String, Map<String, Object>> refs = loadStoredRefs(mergedProps, tableSupplier);
    boolean mutated = false;
    for (CommitUpdateInspector.SnapshotRefMutation mutation : parsed.snapshotRefMutations()) {
      if (mutation == null) {
        continue;
      }
      if (mutation.remove()) {
        String refName = mutation.refName();
        if (refName != null && refs.remove(refName) != null) {
          mutated = true;
        }
        continue;
      }
      String refName = mutation.refName();
      Long snapshotId = mutation.snapshotId();
      if (refName == null || refName.isBlank() || snapshotId == null || snapshotId <= 0) {
        continue;
      }
      Map<String, Object> refMap = new LinkedHashMap<>();
      refMap.put("snapshot-id", snapshotId);
      String type = mutation.type();
      if (type != null && !type.isBlank()) {
        refMap.put("type", type.toLowerCase(Locale.ROOT));
      }
      Long maxRefAge = mutation.maxRefAgeMs();
      if (maxRefAge != null) {
        refMap.put("max-ref-age-ms", maxRefAge);
      }
      Long maxSnapshotAge = mutation.maxSnapshotAgeMs();
      if (maxSnapshotAge != null) {
        refMap.put("max-snapshot-age-ms", maxSnapshotAge);
      }
      Integer minSnapshots = mutation.minSnapshotsToKeep();
      if (minSnapshots != null) {
        refMap.put("min-snapshots-to-keep", minSnapshots);
      }
      refs.put(refName, refMap);
      mutated = true;
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
      CommitUpdateInspector.Parsed parsed) {
    Long latestSnapshotId = parsed.latestAddedSnapshotId();
    Long latestSequence = parsed.maxSnapshotSequenceNumber();
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
    if (updates == null || updates.isEmpty()) {
      return mergedProps;
    }
    List<Map<String, Object>> definitionUpdates = new java.util.ArrayList<>();
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      CommitUpdateInspector.UpdateAction action = CommitUpdateInspector.actionTypeOf(update);
      if (action != null && action.isTableDefinitionAction()) {
        definitionUpdates.add(update);
      }
    }
    if (definitionUpdates.isEmpty()) {
      return mergedProps;
    }
    Map<String, String> sourceProps =
        mergedProps == null
            ? new LinkedHashMap<>(tableSupplier.get().getPropertiesMap())
            : new LinkedHashMap<>(mergedProps);
    Map<String, String> mutatedProps =
        applyTableDefinitionProperties(new LinkedHashMap<>(sourceProps), definitionUpdates);
    Map<String, String> targetProps =
        mergedProps == null ? new LinkedHashMap<>(sourceProps) : mergedProps;
    boolean changed = false;
    for (String key : TABLE_DEFINITION_PROPERTY_KEYS) {
      String value = mutatedProps.get(key);
      if (value == null || value.equals(sourceProps.get(key))) {
        continue;
      }
      targetProps.put(key, value);
      changed = true;
    }
    return changed ? targetProps : mergedProps;
  }

  private Map<String, String> applyTableDefinitionProperties(
      Map<String, String> props, List<Map<String, Object>> updates) {
    Integer formatVersion = asInteger(props.get("format-version"));
    Integer lastColumnId = asInteger(props.get("last-column-id"));
    Integer currentSchemaId = asInteger(props.get("current-schema-id"));
    Integer defaultSpecId = asInteger(props.get("default-spec-id"));
    Integer lastPartitionId = asInteger(props.get("last-partition-id"));
    Integer defaultSortOrderId = asInteger(props.get("default-sort-order-id"));
    Integer lastAddedSchemaId = null;
    Integer lastAddedSpecId = null;
    Integer lastAddedSortOrderId = null;

    for (Map<String, Object> update : updates) {
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
        case ADD_SCHEMA -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> schema =
              update.get("schema") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          Integer schemaId = schema == null ? null : asInteger(schema.get("schema-id"));
          if (schemaId != null && schemaId >= 0) {
            lastAddedSchemaId = schemaId;
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
          Integer specId = spec == null ? null : asInteger(spec.get("spec-id"));
          if (specId != null && specId >= 0) {
            lastAddedSpecId = specId;
          }
          Integer partitionFieldMax = maxPartitionFieldId(spec);
          if (partitionFieldMax != null && partitionFieldMax >= 0) {
            lastPartitionId = partitionFieldMax;
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
          Integer sortOrderId = sortOrder == null ? null : asInteger(sortOrder.get("order-id"));
          if (sortOrderId == null && sortOrder != null) {
            sortOrderId = asInteger(sortOrder.get("sort-order-id"));
          }
          if (sortOrderId != null && sortOrderId >= 0) {
            lastAddedSortOrderId = sortOrderId;
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

    formatVersion = normalizeFormatVersion(formatVersion, null);
    putIntProperty(props, "format-version", formatVersion);
    putIntProperty(props, "last-column-id", lastColumnId);
    putIntProperty(props, "current-schema-id", currentSchemaId);
    putIntProperty(props, "default-spec-id", defaultSpecId);
    putIntProperty(props, "last-partition-id", lastPartitionId);
    putIntProperty(props, "default-sort-order-id", defaultSortOrderId);
    return props;
  }

  private Integer resolveLastAddedId(Integer requestedId, Integer lastAddedId) {
    if (requestedId == null) {
      return null;
    }
    if (requestedId >= 0) {
      return requestedId;
    }
    return requestedId == -1 ? lastAddedId : null;
  }

  private Integer maxPartitionFieldId(Map<String, Object> spec) {
    if (spec == null || spec.isEmpty()) {
      return null;
    }
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fields =
        spec.get("fields") instanceof List<?> list ? (List<Map<String, Object>>) list : List.of();
    if (fields.isEmpty()) {
      return 0;
    }
    Integer max = null;
    for (Map<String, Object> field : fields) {
      if (field == null) {
        continue;
      }
      Integer fieldId = asInteger(field.get("field-id"));
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

  public Table applyCanonicalMetadataProperties(Table plannedTable, TableMetadata metadata) {
    if (plannedTable == null || metadata == null) {
      return plannedTable;
    }
    Map<String, String> props = new LinkedHashMap<>(plannedTable.getPropertiesMap());
    putIntProperty(props, "format-version", metadata.formatVersion());
    putIntProperty(props, "last-column-id", metadata.lastColumnId());
    putIntProperty(props, "current-schema-id", metadata.currentSchemaId());
    putIntProperty(props, "default-spec-id", metadata.defaultSpecId());
    putIntProperty(props, "last-partition-id", metadata.lastAssignedPartitionId());
    putIntProperty(props, "default-sort-order-id", metadata.defaultSortOrderId());
    putLongProperty(props, "last-sequence-number", metadata.lastSequenceNumber());
    syncLongProperty(
        props,
        "current-snapshot-id",
        metadata.currentSnapshot() == null ? null : metadata.currentSnapshot().snapshotId());
    putStringProperty(props, "table-uuid", metadata.uuid());
    props.putIfAbsent("location", metadata.location());
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
