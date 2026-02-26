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
import ai.floedb.floecat.types.LogicalTypeFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * GenericSchemaMapper: Converts generic (non-Iceberg, non-Delta) schema JSON to logical
 * SchemaDescriptor.
 *
 * <p>Expected generic schema form: { "cols": [ { "name": "id", "type": "int" }, ... ] }
 *
 * <p>Used for:
 *
 * <ul>
 *   <li>Internal test tables
 *   <li>Connectors without Iceberg/Delta metadata
 *   <li>Legacy catalog formats
 * </ul>
 */
final class GenericSchemaMapper {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private GenericSchemaMapper() {}

  /**
   * Map a generic schema JSON to logical SchemaDescriptor.
   *
   * @param cid_algo Column ID algorithm to use
   * @param schemaJson Generic schema in JSON form (must have "cols" array)
   * @return SchemaDescriptor with flat columns
   */
  static SchemaDescriptor map(ColumnIdAlgorithm cid_algo, String schemaJson) {
    SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();

    try {
      JsonNode root = MAPPER.readTree(schemaJson);
      JsonNode colsNode = root.path("cols");

      if (!colsNode.isArray()) {
        throw new IllegalArgumentException("Generic schema JSON must contain a 'cols' array");
      }

      int ordinal = 1;

      for (JsonNode col : colsNode) {
        String name = col.path("name").asText("").trim();
        if (name.isEmpty()) {
          throw new IllegalArgumentException(
              "Generic schema column at ordinal " + ordinal + " is missing a non-blank 'name'");
        }
        String declaredType = col.path("type").asText("").trim();
        if (declaredType.isEmpty()) {
          throw new IllegalArgumentException(
              "Generic schema column '" + name + "' is missing a non-blank 'type'");
        }

        String canonicalType = LogicalTypeFormat.format(LogicalTypeFormat.parse(declaredType));
        sb.addColumns(
            ColumnIdComputer.withComputedId(
                cid_algo,
                SchemaColumn.newBuilder()
                    .setName(name)
                    .setLogicalType(canonicalType)
                    .setSourceType(
                        SourceType.newBuilder()
                            .setEngineKind("generic")
                            .setDeclaredType(declaredType)
                            .build())
                    .setFieldId(ordinal) // deterministic order
                    .setNullable(true) // assume nullable
                    .setPhysicalPath(name)
                    .setPartitionKey(false)
                    .setOrdinal(ordinal++) // 1-based ordinal within the parent schema object
                    .setLeaf(true)
                    .build()));
      }

    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse generic schema JSON", e);
    }

    return sb.build();
  }
}
