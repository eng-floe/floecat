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

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.maxFieldId;

import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class TableCreateTransactionMapper {
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
  @Inject ObjectMapper mapper;

  public TransactionCommitRequest buildCreateRequest(
      List<String> namespacePath,
      String tableName,
      ResourceId catalogId,
      ResourceId namespaceId,
      TableRequests.Create request,
      TableGatewaySupport tableSupport) {
    TableSpec spec;
    try {
      spec = tableSupport.buildCreateSpec(catalogId, namespaceId, tableName, request).build();
    } catch (Exception e) {
      throw e instanceof IllegalArgumentException
          ? (IllegalArgumentException) e
          : new IllegalArgumentException(e.getMessage(), e);
    }

    List<Map<String, Object>> updates = buildCreateUpdates(request, spec);
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(namespacePath, tableName),
                CommitUpdateInspector.assertCreateRequirements(),
                updates)));
  }

  private List<Map<String, Object>> buildCreateUpdates(
      TableRequests.Create request, TableSpec spec) {
    Map<String, Object> schema = requireObject(request.schema(), "schema");
    Integer schemaId = requireNonNegativeInt(schema, "schema-id", "schema");
    Integer lastColumnId =
        firstNonNegativeInt(
            asInt(schema.get("last-column-id")),
            maxFieldId(schema, "fields", "id", "field-id", "source-id"));
    if (lastColumnId == null) {
      throw new IllegalArgumentException("schema requires last-column-id");
    }

    Map<String, Object> partitionSpec =
        request.partitionSpec() == null || request.partitionSpec().isNull()
            ? defaultPartitionSpec()
            : requireObject(request.partitionSpec(), "partition-spec");
    Integer specId = requireNonNegativeInt(partitionSpec, "spec-id", "partition-spec");

    Map<String, Object> sortOrder =
        request.writeOrder() == null || request.writeOrder().isNull()
            ? defaultSortOrder()
            : requireObject(request.writeOrder(), "write-order");
    Integer sortOrderId = requireNonNegativeInt(sortOrder, "order-id", "write-order");

    Map<String, String> props = new LinkedHashMap<>(spec.getPropertiesMap());
    String tableLocation = blankToNull(props.remove("location"));
    Integer formatVersion = firstNonNegativeInt(asInt(props.remove("format-version")), 2);
    props.putIfAbsent("last-sequence-number", "0");

    List<Map<String, Object>> updates = new ArrayList<>();
    if (tableLocation != null) {
      updates.add(Map.of("action", "set-location", "location", tableLocation));
    }
    updates.add(Map.of("action", "upgrade-format-version", "format-version", formatVersion));
    updates.add(Map.of("action", "add-schema", "schema", schema, "last-column-id", lastColumnId));
    updates.add(Map.of("action", "set-current-schema", "schema-id", schemaId));
    updates.add(Map.of("action", "add-spec", "spec", partitionSpec));
    updates.add(Map.of("action", "set-default-spec", "spec-id", specId));
    updates.add(Map.of("action", "add-sort-order", "sort-order", sortOrder));
    updates.add(Map.of("action", "set-default-sort-order", "sort-order-id", sortOrderId));
    if (!props.isEmpty()) {
      updates.add(Map.of("action", "set-properties", "updates", props));
    }
    return List.copyOf(updates);
  }

  private Map<String, Object> requireObject(JsonNode node, String fieldName) {
    if (node == null || node.isNull()) {
      throw new IllegalArgumentException(fieldName + " is required");
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException(fieldName + " must be an object");
    }
    return new LinkedHashMap<>(mapper.convertValue(node, MAP_TYPE));
  }

  private Integer requireNonNegativeInt(Map<String, Object> map, String key, String fieldName) {
    Integer value = asInt(map.get(key));
    if (value == null || value < 0) {
      throw new IllegalArgumentException(fieldName + " requires " + key);
    }
    return value;
  }

  @SafeVarargs
  private final Integer firstNonNegativeInt(Integer... values) {
    if (values == null) {
      return null;
    }
    for (Integer value : values) {
      if (value != null && value >= 0) {
        return value;
      }
    }
    return null;
  }

  private Integer asInt(Object value) {
    if (value instanceof Integer i) {
      return i;
    }
    if (value instanceof Number n) {
      return n.intValue();
    }
    if (value instanceof String s) {
      try {
        return Integer.parseInt(s);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }

  private String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private Map<String, Object> defaultPartitionSpec() {
    return new LinkedHashMap<>(Map.of("spec-id", 0, "fields", List.of()));
  }

  private Map<String, Object> defaultSortOrder() {
    return new LinkedHashMap<>(Map.of("order-id", 0, "fields", List.of()));
  }
}
