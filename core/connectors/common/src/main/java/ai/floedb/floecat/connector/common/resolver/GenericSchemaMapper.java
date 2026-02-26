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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;

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

  private static final Logger LOG = Logger.getLogger(GenericSchemaMapper.class);
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
        LOG.warn("Generic schema JSON does not contain 'cols' array: " + schemaJson);
        return sb.build();
      }

      int ordinal = 1;

      for (JsonNode col : colsNode) {
        String name = col.path("name").asText();
        String type = col.path("type").asText();
        sb.addColumns(
            ColumnIdComputer.withComputedId(
                cid_algo,
                SchemaColumn.newBuilder()
                    .setName(name)
                    .setLogicalType(type)
                    .setSourceType(
                        SourceType.newBuilder()
                            .setEngineKind("generic")
                            .setDeclaredType(type)
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
      LOG.warn("Failed to parse generic schema JSON; returning empty schema", e);
    }

    return sb.build();
  }
}
