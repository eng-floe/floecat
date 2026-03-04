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

package ai.floedb.floecat.connector.common.resolver;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * DeltaSchemaMapper: Converts Delta Lake-formatted schema JSON to logical SchemaDescriptor.
 *
 * <p>Handles Delta's JSON metadata format with support for nested structures (struct, array, map).
 */
final class DeltaSchemaMapper {

  private static final Logger LOG = Logger.getLogger(DeltaSchemaMapper.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private DeltaSchemaMapper() {}

  /**
   * Map a Delta schema JSON to logical SchemaDescriptor.
   *
   * @param cid_algo Column ID algorithm to use
   * @param schemaJson Delta schema in JSON form (Delta Lake table schema)
   * @param partitionKeys Set of partition column names (logical names)
   * @return SchemaDescriptor with all nested columns flattened
   */
  static SchemaDescriptor map(
      ColumnIdAlgorithm cid_algo, String schemaJson, Set<String> partitionKeys) {
    SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();

    try {
      JsonNode root = MAPPER.readTree(schemaJson);
      walkDeltaStruct(cid_algo, sb, root, "", partitionKeys);

    } catch (Exception e) {
      LOG.warn("Failed to parse Delta schema JSON; returning empty schema", e);
    }

    return sb.build();
  }

  /**
   * Recursively walk a Delta struct JSON node and add columns to the schema builder.
   *
   * <p>Delta structs have format: { "fields": [ { "name": "...", "type": {...}, "nullable": true,
   * "fieldId": N, ... }, ... ] }
   *
   * @param cid_algo Column ID algorithm
   * @param sb SchemaDescriptor builder accumulating columns
   * @param node The Delta struct node being walked
   * @param prefix Current path prefix (e.g., "" for top-level, "address" for nested)
   * @param partitionKeys Set of partition column names
   */
  private static void walkDeltaStruct(
      ColumnIdAlgorithm cid_algo,
      SchemaDescriptor.Builder sb,
      JsonNode node,
      String prefix,
      Set<String> partitionKeys) {

    if (node == null || !node.has("fields")) {
      return;
    }

    ArrayNode fields = (ArrayNode) node.get("fields");

    for (int i = 0; i < fields.size(); i++) {
      JsonNode f = fields.get(i);
      int ordinal = i + 1; // 1-based ordinal within the parent struct

      String name = f.path("name").asText();
      String logicalType =
          f.path("type").isTextual() ? f.get("type").asText() : f.path("type").toString();

      boolean nullable = f.path("nullable").asBoolean(true);

      String physical = prefix.isEmpty() ? name : prefix + "." + name;

      boolean isPartition = partitionKeys.contains(name) || partitionKeys.contains(physical);

      int fieldId = f.path("fieldId").asInt(0);

      JsonNode typeNode = f.get("type");

      // Emit both container and leaf nodes. Stats will later filter to leaf=true.
      // For Delta JSON, treat struct/array/map as containers (leaf=false), regardless of
      // element/value type.
      boolean isLeaf = true;
      if (typeNode != null && typeNode.isObject()) {
        String typeTag = typeNode.path("type").asText("");
        if ("struct".equals(typeTag) || "array".equals(typeTag) || "map".equals(typeTag)) {
          isLeaf = false;
        }
      }

      sb.addColumns(
          ColumnIdComputer.withComputedId(
              cid_algo,
              SchemaColumn.newBuilder()
                  .setName(name)
                  .setLogicalType(logicalType)
                  .setFieldId(fieldId)
                  .setNullable(nullable)
                  .setPhysicalPath(physical)
                  .setPartitionKey(isPartition)
                  .setOrdinal(ordinal) // 1-based ordinal within the parent schema object
                  .setLeaf(isLeaf)
                  .build()));

      if (typeNode == null || !typeNode.isObject()) {
        continue;
      }

      // struct
      if ("struct".equals(typeNode.path("type").asText(""))) {
        walkDeltaStruct(cid_algo, sb, typeNode, physical, partitionKeys);
      }

      // list<struct>
      if ("array".equals(typeNode.path("type").asText(""))) {
        JsonNode elem = typeNode.get("elementType");
        if (elem != null && elem.isObject() && "struct".equals(elem.path("type").asText(""))) {

          walkDeltaStruct(cid_algo, sb, elem, physical + "[]", partitionKeys);
        }
      }

      // map<*, struct>
      if ("map".equals(typeNode.path("type").asText(""))) {
        JsonNode val = typeNode.get("valueType");
        if (val != null && val.isObject() && "struct".equals(val.path("type").asText(""))) {

          walkDeltaStruct(cid_algo, sb, val, physical + "{}", partitionKeys);
        }
      }
    }
  }
}
