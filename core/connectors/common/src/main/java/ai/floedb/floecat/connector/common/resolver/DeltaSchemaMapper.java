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
import ai.floedb.floecat.delta.identity.KernelAdapter;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.Set;

/**
 * DeltaSchemaMapper: Converts Delta Lake schema JSON to logical SchemaDescriptor.
 *
 * <p>Delta Kernel is the only parser, matching the canonical-identity mapper: a schema Kernel
 * rejects fails here too, so toggling the canonical rollout gate never changes which schemas parse.
 */
final class DeltaSchemaMapper {
  private static final String COLUMN_MAPPING_ID_KEY = "delta.columnMapping.id";

  private DeltaSchemaMapper() {}

  static SchemaDescriptor map(
      ColumnIdAlgorithm cid_algo, String schemaJson, Set<String> partitionKeys) {
    try {
      StructType root = KernelAdapter.deserializeSchema(schemaJson);
      SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();
      walkDeltaStruct(cid_algo, sb, root, "", partitionKeys == null ? Set.of() : partitionKeys);
      return sb.build();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse Delta schema JSON", e);
    }
  }

  private static void walkDeltaStruct(
      ColumnIdAlgorithm cid_algo,
      SchemaDescriptor.Builder sb,
      StructType structType,
      String prefix,
      Set<String> partitionKeys) {
    if (structType == null) {
      return;
    }

    int ordinal = 0;
    for (StructField field : structType.fields()) {
      String name = field.getName();
      String physical = prefix.isEmpty() ? name : prefix + "." + name;
      walkDeltaField(cid_algo, sb, field, physical, partitionKeys, ++ordinal);
    }
  }

  /**
   * Emits one SchemaColumn per visited field and recurses into every container child — struct
   * children as {@code parent.child}, list elements as {@code parent[]}, map keys as {@code
   * parent.key}, map values as {@code parent{}} — matching the canonical path notation of the
   * Iceberg traversal ({@link IcebergNestedPaths}), so the schema path set covers every nested node
   * stats can refer to.
   */
  private static void walkDeltaField(
      ColumnIdAlgorithm cid_algo,
      SchemaDescriptor.Builder sb,
      StructField field,
      String physical,
      Set<String> partitionKeys,
      int ordinal) {
    DataType dataType = field.getDataType();
    // Match by canonical path only: for top-level rows the path equals the name, and a bare-name
    // match would wrongly flag synthetic nested rows (a partition column literally named "key"
    // must not mark every map-key row).
    boolean isPartition = partitionKeys.contains(physical);
    LogicalType logicalType =
        ai.floedb.floecat.delta.identity.DeltaSchemaMapper.toLogical(dataType);

    sb.addColumns(
        ColumnIdComputer.withComputedId(
            cid_algo,
            SchemaColumn.newBuilder()
                .setName(field.getName())
                .setType(LogicalTypeProtoAdapter.toProto(logicalType))
                .setFieldId(extractFieldId(field.getMetadata()))
                .setNullable(field.isNullable())
                .setPhysicalPath(physical)
                .setPartitionKey(isPartition)
                .setOrdinal(ordinal)
                .setLeaf(!isContainerType(dataType))
                .build()));

    if (dataType instanceof StructType nestedStruct) {
      walkDeltaStruct(cid_algo, sb, nestedStruct, physical, partitionKeys);
    } else if (dataType instanceof ArrayType arrayType) {
      walkDeltaField(cid_algo, sb, arrayType.getElementField(), physical + "[]", partitionKeys, 1);
    } else if (dataType instanceof MapType mapType) {
      walkDeltaField(cid_algo, sb, mapType.getKeyField(), physical + ".key", partitionKeys, 1);
      walkDeltaField(cid_algo, sb, mapType.getValueField(), physical + "{}", partitionKeys, 2);
    }
  }

  private static boolean isContainerType(DataType dataType) {
    return dataType instanceof StructType
        || dataType instanceof ArrayType
        || dataType instanceof MapType;
  }

  private static int extractFieldId(FieldMetadata metadata) {
    if (metadata == null) {
      return 0;
    }
    Long fieldId = metadata.getLong(COLUMN_MAPPING_ID_KEY);
    if (fieldId == null) {
      return 0;
    }
    if (fieldId <= 0L || fieldId > Integer.MAX_VALUE) {
      return 0;
    }
    return fieldId.intValue();
  }
}
