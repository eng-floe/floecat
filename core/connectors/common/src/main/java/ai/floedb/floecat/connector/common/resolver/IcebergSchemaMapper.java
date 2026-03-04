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
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jboss.logging.Logger;

/**
 * IcebergSchemaMapper: Converts Iceberg-formatted schema JSON to logical SchemaDescriptor.
 *
 * <p>Handles Iceberg's native field IDs and nested structures (struct, list<struct>,
 * map<*,struct>).
 */
public final class IcebergSchemaMapper {

  private static final Logger LOG = Logger.getLogger(IcebergSchemaMapper.class);

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

      int ordinal = 1;
      for (Types.NestedField field : iceberg.columns()) {
        addIcebergField(cid_algo, sb, field, "", partitionKeys, ordinal++);
      }

      return sb.build();

    } catch (Exception e) {
      LOG.warn("Invalid Iceberg schema, falling back to generic: " + schemaJson, e);
      return GenericSchemaMapper.map(cid_algo, schemaJson);
    }
  }

  /**
   * Recursively add an Iceberg field (and its nested children) to the schema builder.
   *
   * @param cid_algo Column ID algorithm
   * @param sb SchemaDescriptor builder accumulating columns
   * @param field The Iceberg NestedField to process
   * @param prefix Current path prefix (e.g., "" for top-level, "address" for nested in address
   *     struct)
   * @param partitionKeys Set of partition column names
   * @param ordinal 1-based ordinal within the parent container
   */
  private static void addIcebergField(
      ColumnIdAlgorithm cid_algo,
      SchemaDescriptor.Builder sb,
      Types.NestedField field,
      String prefix,
      Set<String> partitionKeys,
      int ordinal) {

    String physical = prefix.isEmpty() ? field.name() : prefix + "." + field.name();

    boolean isPartition = partitionKeys.contains(field.name()) || partitionKeys.contains(physical);

    Type t = field.type();

    // Emit both container and leaf nodes. Stats will later filter to leaf=true.
    // We treat struct/list/map as non-leaf (containers).
    boolean isLeaf =
        !(t instanceof Types.StructType)
            && !(t instanceof Types.ListType)
            && !(t instanceof Types.MapType);

    sb.addColumns(
        ColumnIdComputer.withComputedId(
            cid_algo,
            SchemaColumn.newBuilder()
                .setName(field.name())
                .setLogicalType(field.type().toString())
                .setFieldId(field.fieldId())
                .setNullable(!field.isRequired())
                .setPhysicalPath(physical)
                .setPartitionKey(isPartition)
                .setOrdinal(ordinal) // 1-based ordinal within the parent schema object
                .setLeaf(isLeaf)
                .build()));

    // struct
    if (t instanceof Types.StructType st) {
      int childOrdinal = 1;
      for (Types.NestedField child : st.fields()) {
        addIcebergField(cid_algo, sb, child, physical, partitionKeys, childOrdinal++);
      }
      return;
    }

    // list<struct>
    if (t instanceof Types.ListType lt && lt.elementType() instanceof Types.StructType st2) {
      String childPrefix = physical + "[]";
      int childOrdinal = 1;
      for (Types.NestedField child : st2.fields()) {
        addIcebergField(cid_algo, sb, child, childPrefix, partitionKeys, childOrdinal++);
      }
      return;
    }

    // map<*, struct>
    if (t instanceof Types.MapType mt && mt.valueType() instanceof Types.StructType st3) {
      String childPrefix = physical + "{}";
      int childOrdinal = 1;
      for (Types.NestedField child : st3.fields()) {
        addIcebergField(cid_algo, sb, child, childPrefix, partitionKeys, childOrdinal++);
      }
    }
  }
}
