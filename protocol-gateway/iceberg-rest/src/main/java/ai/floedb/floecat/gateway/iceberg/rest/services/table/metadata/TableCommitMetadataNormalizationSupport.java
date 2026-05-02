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

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class TableCommitMetadataNormalizationSupport {

  TableMetadataView normalizeResponseMetadata(TableMetadataView metadata) {
    List<Map<String, Object>> schemas = normalizeSchemas(metadata.schemas());
    Integer currentSchemaId = normalizeCurrentSchemaId(metadata.currentSchemaId(), schemas);
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    if (currentSchemaId != null) {
      props.put("current-schema-id", Integer.toString(currentSchemaId));
    }
    return TableMetadataViewSupport.copyMetadata(metadata)
        .properties(Map.copyOf(props))
        .currentSchemaId(currentSchemaId)
        .schemas(schemas)
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
