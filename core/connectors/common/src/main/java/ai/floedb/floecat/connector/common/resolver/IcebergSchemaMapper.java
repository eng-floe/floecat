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
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * IcebergSchemaMapper: Converts Iceberg-formatted schema JSON to logical SchemaDescriptor.
 *
 * <p>Traversal and physical-path notation come from {@link IcebergNestedPaths}, the single walk
 * shared with the stats-side fieldId→path maps — every visited node (including list elements, map
 * keys, and map values) becomes a SchemaColumn row, so the schema path set and the stats path set
 * are identical by construction.
 */
public final class IcebergSchemaMapper {

  private IcebergSchemaMapper() {}

  /**
   * Map an Iceberg schema JSON to logical SchemaDescriptor.
   *
   * @param cid_algo Column ID algorithm to use
   * @param schemaJson Iceberg schema in JSON form (e.g., from SchemaParser.toJson())
   * @param partitionKeys Set of partition column names (logical names)
   * @return SchemaDescriptor with all nested columns flattened
   */
  public static SchemaDescriptor map(
      ColumnIdAlgorithm cid_algo, String schemaJson, Set<String> partitionKeys) {
    try {
      Schema iceberg = SchemaParser.fromJson(schemaJson);
      SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();

      IcebergNestedPaths.walk(
          iceberg,
          (field, path, ordinal) -> {
            Type t = field.type();
            boolean isLeaf =
                !(t instanceof Types.StructType)
                    && !(t instanceof Types.ListType)
                    && !(t instanceof Types.MapType);
            // Match by canonical path only: top-level paths equal the name, and a bare-name
            // match would wrongly flag synthetic nested rows (a partition column literally
            // named "key" must not mark every map-key row).
            boolean isPartition = partitionKeys.contains(path);
            LogicalType logicalType = IcebergTypeMappings.toLogical(t);

            sb.addColumns(
                ColumnIdComputer.withComputedId(
                    cid_algo,
                    SchemaColumn.newBuilder()
                        .setName(field.name())
                        .setType(LogicalTypeProtoAdapter.toProto(logicalType))
                        .setFieldId(field.fieldId())
                        .setNullable(!field.isRequired())
                        .setPhysicalPath(path)
                        .setPartitionKey(isPartition)
                        .setOrdinal(ordinal)
                        .setLeaf(isLeaf)
                        .build()));
          });

      return sb.build();

    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse Iceberg schema JSON", e);
    }
  }
}
