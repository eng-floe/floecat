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
import ai.floedb.floecat.common.rpc.SourceType;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.Locale;
import java.util.Set;

/**
 * DeltaSchemaMapper: Converts Delta Lake-formatted schema JSON to logical SchemaDescriptor.
 *
 * <p>Handles Delta's JSON metadata format with support for nested structures (struct, array, map).
 */
final class DeltaSchemaMapper {
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
      JsonNode fields = root.get("fields");
      if (fields == null || !fields.isArray()) {
        throw new IllegalArgumentException("Delta schema JSON must contain a 'fields' array");
      }
      walkDeltaStruct(cid_algo, sb, root, "", partitionKeys);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse Delta schema JSON", e);
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
      String logicalType = deltaTypeToCanonical(f.get("type"));

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
                  .setSourceType(deltaSourceType(typeNode))
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

  /**
   * Convert a Delta Lake type JSON node to its canonical logical-type string.
   *
   * <p>Delta types are either textual scalars (e.g. {@code "string"}, {@code "timestamp"}) or JSON
   * objects for complex types ({@code {"type":"struct",...}}, {@code {"type":"array",...}}, {@code
   * {"type":"map",...}}).
   *
   * <p>Timestamp semantics: Delta's {@code "timestamp"} is always UTC-stored → canonical {@code
   * "TIMESTAMPTZ"}. Delta's {@code "timestamp_ntz"} is timezone-naive → canonical {@code
   * "TIMESTAMP"}. Note: the spec decode matrix v1 has these inverted; the semantically correct
   * mapping is applied here.
   *
   * <p>Integer aliases: {@code "byte"}, {@code "short"}, {@code "integer"}, {@code "long"} and
   * their SQL synonyms all collapse to canonical {@code "INT"} (64-bit), consistent with {@link
   * ai.floedb.floecat.types.LogicalKind}.
   *
   * <p>For {@code decimal(p,s)}, the raw Delta string (e.g. {@code "decimal(10,2)"}) is upper-cased
   * and returned as-is; it is parseable by the canonical type parser.
   *
   * @param typeNode Delta "type" JSON node (may be textual or object)
   * @return canonical logical-type string (never null)
   */
  private static String deltaTypeToCanonical(JsonNode typeNode) {
    if (typeNode == null) {
      throw new IllegalArgumentException("Delta field type is missing");
    }

    // Complex types are represented as JSON objects with a "type" discriminator.
    if (typeNode.isObject()) {
      return switch (typeNode.path("type").asText("")) {
        case "struct" -> "STRUCT";
        case "array" -> "ARRAY";
        case "map" -> "MAP";
        default ->
            throw new IllegalArgumentException(
                "Unrecognized Delta complex type: '" + typeNode.path("type").asText("") + "'");
      };
    }

    // Scalar types are textual identifiers.
    String raw = typeNode.asText("");
    String lowerRaw = raw.toLowerCase(Locale.ROOT);
    return switch (lowerRaw) {
      case "boolean" -> "BOOLEAN";
      // All integer sizes collapse to canonical INT (64-bit).
      case "byte", "tinyint", "short", "smallint", "integer", "int", "long", "bigint" -> "INT";
      case "float" -> "FLOAT";
      case "double" -> "DOUBLE";
      case "string" -> "STRING";
      case "binary" -> "BINARY";
      case "date" -> "DATE";
      // Delta "timestamp" is UTC-stored → TIMESTAMPTZ.
      // Delta "timestamp_ntz" is timezone-naive → TIMESTAMP.
      // Note: the spec decode matrix v1 has these inverted; correct semantic mapping applied.
      case "timestamp" -> "TIMESTAMPTZ";
      case "timestamp_ntz" -> "TIMESTAMP";
      case "interval" -> "INTERVAL";
      default -> {
        // decimal(p,s) arrives as e.g. "decimal(10,2)" — upper-case and pass through.
        if (lowerRaw.startsWith("decimal")) {
          yield canonicalDeltaDecimal(raw);
        }
        throw new IllegalArgumentException("Unrecognized Delta scalar type: '" + raw + "'");
      }
    };
  }

  private static String canonicalDeltaDecimal(String raw) {
    final LogicalType logicalType;
    try {
      logicalType = LogicalTypeFormat.parse(raw);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid Delta decimal type: '" + raw + "'", e);
    }
    ConnectorTypeConstraints.validateDecimalPrecision(logicalType, "Delta", raw);
    return LogicalTypeFormat.format(logicalType);
  }

  private static SourceType deltaSourceType(JsonNode typeNode) {
    return SourceType.newBuilder()
        .setEngineKind("delta")
        .setDeclaredType(deltaDeclaredType(typeNode))
        .build();
  }

  private static String deltaDeclaredType(JsonNode typeNode) {
    if (typeNode == null) {
      return "";
    }
    if (typeNode.isTextual()) {
      return typeNode.asText("");
    }
    return typeNode.toString();
  }
}
