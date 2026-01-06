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

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asInteger;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.firstNonNull;

import ai.floedb.floecat.catalog.rpc.PartitionField;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortField;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class SchemaMapper {
  private static final ObjectMapper JSON = new ObjectMapper();

  private SchemaMapper() {}

  static Map<String, Object> schemaFromTable(Table table) {
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

  static Map<String, Object> schemaFromRequest(TableRequests.Create request) {
    JsonNode node = request.schema();
    if (node == null || node.isNull()) {
      String schemaJson = request.schemaJson();
      if (schemaJson != null && !schemaJson.isBlank()) {
        try {
          node = JSON.readTree(schemaJson);
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException("schemaJson is invalid", e);
        }
      }
    }
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

  static Map<String, Object> partitionSpecFromRequest(TableRequests.Create request) {
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

  static Map<String, Object> sortOrderFromRequest(TableRequests.Create request) {
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

  static Integer maxPartitionFieldId(Map<String, Object> spec) {
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

  static List<Map<String, Object>> schemasFromMetadata(IcebergMetadata metadata) {
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

  static List<Map<String, Object>> partitionSpecsFromMetadata(IcebergMetadata metadata) {
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

  static List<Map<String, Object>> sortOrdersFromMetadata(IcebergMetadata metadata) {
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
      lastColumnId = maxFieldId(fields);
      if (lastColumnId == null || lastColumnId <= 0) {
        throw new IllegalArgumentException("schema requires last-column-id");
      }
      schema.put("last-column-id", lastColumnId);
    }
  }

  private static Integer maxFieldId(List<Map<String, Object>> fields) {
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

  @SuppressWarnings("unchecked")
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
}
