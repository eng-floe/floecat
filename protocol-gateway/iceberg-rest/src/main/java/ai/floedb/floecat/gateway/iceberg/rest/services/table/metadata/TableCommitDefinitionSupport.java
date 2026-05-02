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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.metadata;

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asString;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.maxFieldId;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.normalizeFormatVersion;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataListUtil;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class TableCommitDefinitionSupport {

  TableMetadataView mergeTableDefinitionUpdates(
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

    return TableMetadataViewSupport.copyMetadata(metadata)
        .formatVersion(formatVersion)
        .location(tableLocation)
        .properties(Map.copyOf(props))
        .lastColumnId(lastColumnId)
        .currentSchemaId(currentSchemaId)
        .defaultSpecId(defaultSpecId)
        .lastPartitionId(lastPartitionId)
        .defaultSortOrderId(defaultSortOrderId)
        .schemas(List.copyOf(schemas))
        .partitionSpecs(List.copyOf(partitionSpecs))
        .sortOrders(List.copyOf(sortOrders))
        .build();
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
