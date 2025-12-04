package ai.floedb.metacat.gateway.iceberg.rest.support.mapper.table;

import static ai.floedb.metacat.gateway.iceberg.rest.support.mapper.table.TableMappingUtil.asInteger;
import static ai.floedb.metacat.gateway.iceberg.rest.support.mapper.table.TableMappingUtil.firstNonNull;

import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergSchema;
import ai.floedb.metacat.catalog.rpc.IcebergSortField;
import ai.floedb.metacat.catalog.rpc.IcebergSortOrder;
import ai.floedb.metacat.catalog.rpc.PartitionField;
import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.TableRequests;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class SchemaMapper {
  private static final ObjectMapper JSON = new ObjectMapper();

  private SchemaMapper() {}

  static Map<String, Object> schemaFromTable(Table table) {
    String schemaJson = table.getSchemaJson();
    if (schemaJson == null || schemaJson.isBlank()) {
      return defaultSchema();
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
    try {
      normalizeSchema(schema);
      return schema;
    } catch (IllegalArgumentException e) {
      return defaultSchema();
    }
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
    try {
      normalizeSchema(schema);
      return schema;
    } catch (IllegalArgumentException e) {
      return defaultSchema();
    }
  }

  static Map<String, Object> partitionSpecFromRequest(TableRequests.Create request) {
    JsonNode node = request.partitionSpec();
    if (node == null || node.isNull()) {
      return defaultPartitionSpec();
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("partition-spec must be an object");
    }
    return new LinkedHashMap<>(
        JSON.convertValue(node, new TypeReference<Map<String, Object>>() {}));
  }

  static Map<String, Object> sortOrderFromRequest(TableRequests.Create request) {
    JsonNode node = request.writeOrder();
    if (node == null || node.isNull()) {
      Map<String, Object> defaults = defaultSortOrder();
      defaults.put("fields", List.of());
      return defaults;
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("write-order must be an object");
    }
    Map<String, Object> order =
        new LinkedHashMap<>(JSON.convertValue(node, new TypeReference<Map<String, Object>>() {}));
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

  static Map<String, Object> defaultPartitionSpec() {
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("spec-id", 0);
    spec.put("fields", List.of());
    return spec;
  }

  static Map<String, Object> defaultSortOrder() {
    Map<String, Object> order = new LinkedHashMap<>();
    order.put("sort-order-id", 0);
    order.put("order-id", 0);
    order.put("fields", List.of());
    normalizeSortOrder(order);
    return order;
  }

  static Map<String, Object> defaultSchema() {
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("schema-id", 0);
    schema.put("last-column-id", 1);
    schema.put("type", "struct");
    Map<String, Object> field = new LinkedHashMap<>();
    field.put("id", 1);
    field.put("name", "placeholder");
    field.put("type", "string");
    field.put("required", false);
    schema.put("fields", new ArrayList<>(List.of(field)));
    normalizeSchema(schema);
    return schema;
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
      if (schema.getLastColumnId() > 0) {
        entry.put("last-column-id", schema.getLastColumnId());
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
      if (!spec.getSpecName().isBlank()) {
        entry.put("spec-name", spec.getSpecName());
      }
      List<Map<String, Object>> fields = new ArrayList<>();
      for (PartitionField field : spec.getFieldsList()) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("field-id", field.getFieldId());
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
        f.put("source-field-id", field.getSourceFieldId());
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
    int nextFieldId = 1;
    int maxFieldId = 0;
    for (Map<String, Object> field : fields) {
      Object fieldIdSource =
          firstNonNull(
              field.get("id"), firstNonNull(field.get("field-id"), field.get("source-id")));
      Integer fieldId = asInteger(fieldIdSource);
      if (fieldId == null || fieldId <= 0) {
        fieldId = nextFieldId++;
      } else if (fieldId >= nextFieldId) {
        nextFieldId = fieldId + 1;
      }
      maxFieldId = Math.max(maxFieldId, fieldId);
      field.put("id", fieldId);
    }
    Integer schemaId = asInteger(schema.get("schema-id"));
    if (schemaId == null || schemaId < 0) {
      schemaId = 0;
      schema.put("schema-id", schemaId);
    }
    Integer lastColumnId = asInteger(schema.get("last-column-id"));
    if (lastColumnId == null || lastColumnId < maxFieldId) {
      lastColumnId = maxFieldId;
      schema.put("last-column-id", lastColumnId);
    }
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
      return Map.of();
    }
    try {
      return JSON.readValue(json, Object.class);
    } catch (JsonProcessingException e) {
      return Map.of();
    }
  }

  static void normalizeSortOrder(Map<String, Object> order) {
    Integer orderId = asInteger(firstNonNull(order.get("sort-order-id"), order.get("order-id")));
    if (orderId == null || orderId < 0) {
      orderId = 0;
    }
    order.put("sort-order-id", orderId);
    order.put("order-id", orderId);
    Object fieldsObj = order.get("fields");
    if (!(fieldsObj instanceof List<?> list)) {
      order.put("fields", List.of());
      return;
    }
    List<Map<String, Object>> normalized = new ArrayList<>();
    for (Object entry : list) {
      if (!(entry instanceof Map<?, ?> mapEntry)) {
        continue;
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
      if (!field.containsKey("transform")) {
        field.put("transform", "identity");
      }
      if (!field.containsKey("direction")) {
        field.put("direction", "ASC");
      }
      if (!field.containsKey("null-order")) {
        field.put("null-order", "NULLS_FIRST");
      }
      normalized.add(field);
    }
    order.put("fields", normalized);
  }
}
