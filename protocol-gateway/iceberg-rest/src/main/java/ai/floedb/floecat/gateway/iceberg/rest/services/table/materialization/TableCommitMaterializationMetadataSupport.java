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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.materialization;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.metadata.TableMetadataViewSupport;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class TableCommitMaterializationMetadataSupport {

  TableMetadataView normalizeRequiredMetadata(TableMetadataView metadata) {
    if (metadata == null) {
      return null;
    }
    List<Map<String, Object>> schemas = ensureSchemas(metadata.schemas());
    Integer currentSchemaId =
        firstNonNull(
            nonNegative(metadata.currentSchemaId()), firstEntryId(schemas, "schema-id"), 0);
    Integer lastColumnId =
        firstNonNull(nonNegative(metadata.lastColumnId()), maxColumnIdFromSchemas(schemas), 0);
    List<Map<String, Object>> partitionSpecs = ensureEntries(metadata.partitionSpecs(), "spec-id");
    Integer defaultSpecId =
        firstNonNull(
            nonNegative(metadata.defaultSpecId()), firstEntryId(partitionSpecs, "spec-id"), 0);
    Integer lastPartitionId =
        firstNonNull(
            nonNegative(metadata.lastPartitionId()), maxPartitionFieldId(partitionSpecs), 0);
    List<Map<String, Object>> sortOrders = ensureEntries(metadata.sortOrders(), "order-id");
    Integer defaultSortOrderId =
        firstNonNull(
            nonNegative(metadata.defaultSortOrderId()), firstEntryId(sortOrders, "order-id"), 0);
    Map<String, String> normalizedProperties =
        normalizeRequiredIdProperties(
            metadata.properties(),
            currentSchemaId,
            lastColumnId,
            defaultSpecId,
            lastPartitionId,
            defaultSortOrderId);
    return TableMetadataViewSupport.copyMetadata(metadata)
        .properties(normalizedProperties)
        .lastColumnId(lastColumnId)
        .currentSchemaId(currentSchemaId)
        .defaultSpecId(defaultSpecId)
        .lastPartitionId(lastPartitionId)
        .defaultSortOrderId(defaultSortOrderId)
        .schemas(schemas)
        .partitionSpecs(partitionSpecs)
        .sortOrders(sortOrders)
        .build();
  }

  private List<Map<String, Object>> ensureSchemas(List<Map<String, Object>> schemas) {
    if (schemas != null && !schemas.isEmpty()) {
      return ensureNonNegativeEntryId(schemas, "schema-id");
    }
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("type", "struct");
    schema.put("schema-id", 0);
    schema.put("fields", List.of());
    return List.of(Map.copyOf(schema));
  }

  private List<Map<String, Object>> ensureEntries(List<Map<String, Object>> entries, String idKey) {
    if (entries != null && !entries.isEmpty()) {
      return ensureNonNegativeEntryId(entries, idKey);
    }
    Map<String, Object> entry = new LinkedHashMap<>();
    entry.put(idKey, 0);
    entry.put("fields", List.of());
    return List.of(Map.copyOf(entry));
  }

  private Map<String, String> normalizeRequiredIdProperties(
      Map<String, String> properties,
      Integer currentSchemaId,
      Integer lastColumnId,
      Integer defaultSpecId,
      Integer lastPartitionId,
      Integer defaultSortOrderId) {
    Map<String, String> normalized =
        properties == null ? new LinkedHashMap<>() : new LinkedHashMap<>(properties);
    if (currentSchemaId != null) {
      normalized.put("current-schema-id", currentSchemaId.toString());
    }
    if (lastColumnId != null) {
      normalized.put("last-column-id", lastColumnId.toString());
    }
    if (defaultSpecId != null) {
      normalized.put("default-spec-id", defaultSpecId.toString());
    }
    if (lastPartitionId != null) {
      normalized.put("last-partition-id", lastPartitionId.toString());
    }
    if (defaultSortOrderId != null) {
      normalized.put("default-sort-order-id", defaultSortOrderId.toString());
    }
    return normalized;
  }

  private Integer firstEntryId(List<Map<String, Object>> entries, String idKey) {
    for (Map<String, Object> entry : entries) {
      if (entry == null) {
        continue;
      }
      Integer id = nonNegative(asInteger(entry.get(idKey)));
      if (id != null) {
        return id;
      }
    }
    return null;
  }

  private List<Map<String, Object>> ensureNonNegativeEntryId(
      List<Map<String, Object>> entries, String idKey) {
    for (Map<String, Object> entry : entries) {
      if (entry == null) {
        continue;
      }
      Integer id = nonNegative(asInteger(entry.get(idKey)));
      if (id != null) {
        return entries;
      }
    }
    List<Map<String, Object>> normalized = new ArrayList<>(entries.size());
    boolean patched = false;
    for (Map<String, Object> entry : entries) {
      if (!patched && entry != null) {
        Map<String, Object> copy = new LinkedHashMap<>(entry);
        copy.put(idKey, 0);
        normalized.add(Map.copyOf(copy));
        patched = true;
      } else {
        normalized.add(entry);
      }
    }
    if (!patched) {
      Map<String, Object> fallback = new LinkedHashMap<>();
      fallback.put(idKey, 0);
      normalized.set(0, Map.copyOf(fallback));
    }
    return List.copyOf(normalized);
  }

  private Integer maxColumnIdFromSchemas(List<Map<String, Object>> schemas) {
    int max = -1;
    for (Map<String, Object> schema : schemas) {
      if (schema == null) {
        continue;
      }
      int schemaMax = maxColumnIdFromFields(asListOfMaps(schema.get("fields")));
      if (schemaMax > max) {
        max = schemaMax;
      }
    }
    return max < 0 ? null : max;
  }

  private int maxColumnIdFromFields(List<Map<String, Object>> fields) {
    int max = -1;
    for (Map<String, Object> field : fields) {
      if (field == null) {
        continue;
      }
      Integer id = asInteger(field.get("id"));
      if (id != null && id > max) {
        max = id;
      }
      Object nestedType = field.get("type");
      if (nestedType instanceof Map<?, ?> nestedMap) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nestedFields =
            asListOfMaps(((Map<String, Object>) nestedMap).get("fields"));
        int nestedMax = maxColumnIdFromFields(nestedFields);
        if (nestedMax > max) {
          max = nestedMax;
        }
      }
    }
    return max;
  }

  private Integer maxPartitionFieldId(List<Map<String, Object>> specs) {
    int max = -1;
    for (Map<String, Object> spec : specs) {
      if (spec == null) {
        continue;
      }
      for (Map<String, Object> field : asListOfMaps(spec.get("fields"))) {
        Integer fieldId = asInteger(field == null ? null : field.get("field-id"));
        if (fieldId != null && fieldId > max) {
          max = fieldId;
        }
      }
    }
    return max < 0 ? null : max;
  }

  private List<Map<String, Object>> asListOfMaps(Object value) {
    if (!(value instanceof List<?> list) || list.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>(list.size());
    for (Object entry : list) {
      if (entry instanceof Map<?, ?> raw) {
        @SuppressWarnings("unchecked")
        Map<String, Object> typed = (Map<String, Object>) raw;
        out.add(typed);
      }
    }
    return out;
  }

  private Integer firstNonNull(Integer... values) {
    if (values == null) {
      return null;
    }
    for (Integer value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private Integer nonNegative(Integer value) {
    return value == null || value < 0 ? null : value;
  }

  private Integer asInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number n) {
      return n.intValue();
    }
    try {
      return Integer.parseInt(String.valueOf(value));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
