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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class DeltaSchemaNormalizer {
  private static final ObjectMapper JSON = new ObjectMapper();

  private DeltaSchemaNormalizer() {}

  public static boolean isDeltaTable(Table table, Map<String, String> properties) {
    if (table != null
        && table.hasUpstream()
        && table.getUpstream().getFormat() == TableFormat.TF_DELTA) {
      return true;
    }
    if (properties == null || properties.isEmpty()) {
      return false;
    }
    String source =
        firstNonBlank(properties.get("data_source_format"), properties.get("upstream.format"));
    if (source == null) {
      source = properties.get("format");
    }
    return source != null && source.trim().equalsIgnoreCase("DELTA");
  }

  public static Map<String, Object> normalizeSchemaMap(String rawSchemaJson, int desiredSchemaId) {
    if (rawSchemaJson == null || rawSchemaJson.isBlank()) {
      return null;
    }
    try {
      Map<String, Object> root =
          JSON.readValue(rawSchemaJson, new TypeReference<Map<String, Object>>() {});
      return normalizeSchemaMap(root, desiredSchemaId);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse Delta schema JSON", e);
    }
  }

  public static String normalizeSchemaJson(String rawSchemaJson, int desiredSchemaId) {
    Map<String, Object> normalized = normalizeSchemaMap(rawSchemaJson, desiredSchemaId);
    if (normalized == null) {
      return null;
    }
    try {
      return JSON.writeValueAsString(normalized);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize normalized Delta schema JSON", e);
    }
  }

  private static Map<String, Object> normalizeSchemaMap(
      Map<String, Object> root, int desiredSchemaId) {
    if (root == null) {
      return null;
    }
    int seed = Math.max(1, maxFieldIdInSchema(root.get("fields")) + 1);
    IdAllocator ids = new IdAllocator(seed);
    List<Map<String, Object>> fields = normalizeFields(root.get("fields"), ids);
    int schemaId = nonNegativeInt(root.get("schema-id"), Math.max(0, desiredSchemaId));
    int lastColumnId = Math.max(nonNegativeInt(root.get("last-column-id"), 0), maxFieldId(fields));

    Map<String, Object> normalized = new LinkedHashMap<>();
    normalized.put("schema-id", schemaId);
    normalized.put("type", "struct");
    normalized.put("fields", fields);
    normalized.put("last-column-id", lastColumnId);
    return normalized;
  }

  private static List<Map<String, Object>> normalizeFields(Object rawFields, IdAllocator ids) {
    if (!(rawFields instanceof List<?> list)) {
      return List.of();
    }
    List<Map<String, Object>> normalized = new ArrayList<>(list.size());
    for (Object entry : list) {
      if (!(entry instanceof Map<?, ?> rawField)) {
        continue;
      }
      Map<String, Object> field = asStringObjectMap(rawField);
      int fieldId = ids.assign(fieldId(field));
      String fieldName = stringValue(field.get("name"));
      if (fieldName == null || fieldName.isBlank()) {
        fieldName = "field_" + fieldId;
      }

      Map<String, Object> out = new LinkedHashMap<>();
      out.put("id", fieldId);
      out.put("name", fieldName);
      out.put("required", required(field));
      out.put("type", normalizeType(field.get("type"), ids));
      normalized.add(out);
    }
    return normalized;
  }

  private static Object normalizeType(Object rawType, IdAllocator ids) {
    if (rawType instanceof String typeName) {
      return normalizePrimitiveType(typeName);
    }
    if (!(rawType instanceof Map<?, ?> rawTypeMap)) {
      return "string";
    }
    Map<String, Object> typeMap = asStringObjectMap(rawTypeMap);
    String typeName = stringValue(typeMap.get("type"));
    if (typeName == null || typeName.isBlank()) {
      return "string";
    }
    String normalizedType = typeName.trim().toLowerCase(Locale.ROOT);
    return switch (normalizedType) {
      case "struct" -> normalizeStructType(typeMap, ids);
      case "array", "list" -> normalizeListType(typeMap, ids);
      case "map" -> normalizeMapType(typeMap, ids);
      default -> normalizePrimitiveType(typeName);
    };
  }

  private static Map<String, Object> normalizeStructType(
      Map<String, Object> typeMap, IdAllocator ids) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("type", "struct");
    out.put("fields", normalizeFields(typeMap.get("fields"), ids));
    return out;
  }

  private static Map<String, Object> normalizeListType(
      Map<String, Object> typeMap, IdAllocator ids) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("type", "list");
    out.put("element-id", ids.assign(nonNegativeInt(typeMap.get("element-id"), 0)));
    out.put(
        "element",
        normalizeType(firstNonNull(typeMap.get("element"), typeMap.get("elementType")), ids));
    out.put(
        "element-required",
        !booleanValue(
            firstNonNull(typeMap.get("containsNull"), typeMap.get("element-optional")), true));
    return out;
  }

  private static Map<String, Object> normalizeMapType(
      Map<String, Object> typeMap, IdAllocator ids) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("type", "map");
    out.put("key-id", ids.assign(nonNegativeInt(typeMap.get("key-id"), 0)));
    out.put("key", normalizeType(firstNonNull(typeMap.get("key"), typeMap.get("keyType")), ids));
    out.put("value-id", ids.assign(nonNegativeInt(typeMap.get("value-id"), 0)));
    out.put(
        "value", normalizeType(firstNonNull(typeMap.get("value"), typeMap.get("valueType")), ids));
    out.put(
        "value-required",
        !booleanValue(
            firstNonNull(typeMap.get("valueContainsNull"), typeMap.get("value-optional")), true));
    return out;
  }

  private static String normalizePrimitiveType(String rawType) {
    if (rawType == null || rawType.isBlank()) {
      return "string";
    }
    String normalized = rawType.trim().toLowerCase(Locale.ROOT);
    return switch (normalized) {
      case "byte", "short", "integer" -> "int";
      case "real" -> "float";
      case "str" -> "string";
      case "timestamp_ntz" -> "timestamp";
      default -> normalized;
    };
  }

  private static boolean required(Map<String, Object> field) {
    Object explicit = field.get("required");
    if (explicit instanceof Boolean bool) {
      return bool;
    }
    Object nullable = field.get("nullable");
    return nullable instanceof Boolean bool && !bool;
  }

  private static int fieldId(Map<String, Object> field) {
    int directId = positiveInt(field.get("id"), 0);
    if (directId > 0) {
      return directId;
    }
    Object metadataObj = field.get("metadata");
    if (metadataObj instanceof Map<?, ?> metadataMap) {
      return positiveInt(metadataMap.get("delta.columnMapping.id"), 0);
    }
    return 0;
  }

  private static int maxFieldIdInSchema(Object rawFields) {
    if (!(rawFields instanceof List<?> list)) {
      return 0;
    }
    int max = 0;
    for (Object entry : list) {
      if (!(entry instanceof Map<?, ?> rawField)) {
        continue;
      }
      Map<String, Object> field = asStringObjectMap(rawField);
      max = Math.max(max, fieldId(field));
      max = Math.max(max, maxFieldIdInType(field.get("type")));
    }
    return max;
  }

  private static int maxFieldIdInType(Object rawType) {
    if (!(rawType instanceof Map<?, ?> rawTypeMap)) {
      return 0;
    }
    Map<String, Object> typeMap = asStringObjectMap(rawTypeMap);
    String typeName = stringValue(typeMap.get("type"));
    if (typeName == null) {
      return 0;
    }
    String normalized = typeName.trim().toLowerCase(Locale.ROOT);
    return switch (normalized) {
      case "struct" -> maxFieldIdInSchema(typeMap.get("fields"));
      case "array", "list" ->
          Math.max(
              nonNegativeInt(typeMap.get("element-id"), 0),
              maxFieldIdInType(firstNonNull(typeMap.get("element"), typeMap.get("elementType"))));
      case "map" -> {
        int keyId = nonNegativeInt(typeMap.get("key-id"), 0);
        int valueId = nonNegativeInt(typeMap.get("value-id"), 0);
        int nested =
            Math.max(
                maxFieldIdInType(firstNonNull(typeMap.get("key"), typeMap.get("keyType"))),
                maxFieldIdInType(firstNonNull(typeMap.get("value"), typeMap.get("valueType"))));
        yield Math.max(Math.max(keyId, valueId), nested);
      }
      default -> 0;
    };
  }

  private static int maxFieldId(List<Map<String, Object>> fields) {
    int max = 0;
    for (Map<String, Object> field : fields) {
      max = Math.max(max, positiveInt(field.get("id"), 0));
      max = Math.max(max, maxFieldIdInType(field.get("type")));
    }
    return max;
  }

  private static Map<String, Object> asStringObjectMap(Map<?, ?> input) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : input.entrySet()) {
      if (entry.getKey() != null) {
        out.put(entry.getKey().toString(), entry.getValue());
      }
    }
    return out;
  }

  private static String firstNonBlank(String left, String right) {
    if (left != null && !left.isBlank()) {
      return left;
    }
    if (right != null && !right.isBlank()) {
      return right;
    }
    return null;
  }

  private static Object firstNonNull(Object left, Object right) {
    return left != null ? left : right;
  }

  private static String stringValue(Object value) {
    return value == null ? null : value.toString();
  }

  private static int positiveInt(Object value, int fallback) {
    if (value instanceof Number number) {
      int asInt = number.intValue();
      return asInt > 0 ? asInt : fallback;
    }
    if (value instanceof String raw) {
      try {
        int asInt = Integer.parseInt(raw.trim());
        return asInt > 0 ? asInt : fallback;
      } catch (RuntimeException ignored) {
        return fallback;
      }
    }
    return fallback;
  }

  private static int nonNegativeInt(Object value, int fallback) {
    if (value instanceof Number number) {
      return Math.max(0, number.intValue());
    }
    if (value instanceof String raw) {
      try {
        return Math.max(0, Integer.parseInt(raw.trim()));
      } catch (RuntimeException ignored) {
        return fallback;
      }
    }
    return fallback;
  }

  private static boolean booleanValue(Object value, boolean fallback) {
    if (value instanceof Boolean bool) {
      return bool;
    }
    if (value instanceof String raw) {
      if ("true".equalsIgnoreCase(raw)) {
        return true;
      }
      if ("false".equalsIgnoreCase(raw)) {
        return false;
      }
    }
    return fallback;
  }

  private static final class IdAllocator {
    private final Set<Integer> seen = new HashSet<>();
    private int next;

    private IdAllocator(int initial) {
      next = Math.max(1, initial);
    }

    private int assign(int existing) {
      if (existing > 0 && !seen.contains(existing)) {
        seen.add(existing);
        next = Math.max(next, existing + 1);
        return existing;
      }
      while (seen.contains(next)) {
        next++;
      }
      int assigned = next++;
      seen.add(assigned);
      return assigned;
    }
  }
}
