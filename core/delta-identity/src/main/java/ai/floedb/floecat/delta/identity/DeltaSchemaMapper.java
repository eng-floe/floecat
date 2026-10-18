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

package ai.floedb.floecat.delta.identity;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.schema.identity.ColumnIdentityMaps;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.LegacyDottedKeyIndex;
import ai.floedb.floecat.schema.identity.SchemaIdentityEntry;
import ai.floedb.floecat.schema.identity.SchemaIdentityState;
import ai.floedb.floecat.types.LogicalField;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.VariantType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Maps Delta schemas to Floecat logical schemas using canonical IDs only. */
public final class DeltaSchemaMapper {
  private static final int MAX_DECIMAL_PRECISION = 38;

  private DeltaSchemaMapper() {}

  public static SchemaDescriptor map(
      String schemaJson, Set<String> partitionKeys, ColumnIdentityMap identityMap) {
    if (identityMap == null || identityMap.equals(ColumnIdentityMap.getDefaultInstance())) {
      throw new IllegalArgumentException("Canonical column identity map is required");
    }
    SchemaIdentityState identity = ColumnIdentityMaps.restore(identityMap);
    StructType root;
    try {
      root = KernelAdapter.deserializeSchema(schemaJson);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse Delta schema JSON", e);
    }
    SchemaDescriptor.Builder descriptor = SchemaDescriptor.newBuilder();
    walkStruct(
        descriptor,
        root,
        ColumnPath.ROOT,
        "",
        partitionKeys == null ? Set.of() : partitionKeys,
        identity);
    if (descriptor.getColumnsCount() != identity.entries().size()) {
      throw new IllegalArgumentException(
          "Column identity map does not exactly match the Delta schema");
    }
    return descriptor.build();
  }

  private static void walkStruct(
      SchemaDescriptor.Builder descriptor,
      StructType struct,
      ColumnPath prefix,
      String physicalPrefix,
      Set<String> partitionKeys,
      SchemaIdentityState identity) {
    int ordinal = 0;
    for (StructField field : struct.fields()) {
      String physical =
          physicalPrefix.isEmpty() ? field.getName() : physicalPrefix + "." + field.getName();
      walkField(
          descriptor,
          field,
          prefix.field(field.getName()),
          physical,
          partitionKeys,
          ++ordinal,
          identity);
    }
  }

  private static void walkField(
      SchemaDescriptor.Builder descriptor,
      StructField field,
      ColumnPath path,
      String physical,
      Set<String> partitionKeys,
      int ordinal,
      SchemaIdentityState identity) {
    DataType type = field.getDataType();
    SchemaIdentityEntry entry =
        identity
            .byPath(path)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Column identity map has no ID for " + path.display()));
    descriptor.addColumns(
        SchemaColumn.newBuilder()
            .setId(entry.canonicalId())
            // Parquet files of a column-mapped table carry the Delta field ID, which readers prefer
            // over the physical path when matching file columns.
            .setFieldId(entry.nativeFieldId().orElse(0))
            .setName(field.getName())
            .setType(LogicalTypeProtoAdapter.toProto(toLogical(type)))
            .setNullable(field.isNullable())
            .setPhysicalPath(physical)
            .setPartitionKey(partitionKeys.contains(physical))
            .setOrdinal(ordinal)
            .setLeaf(!isContainer(type))
            .build());

    if (type instanceof StructType nested) {
      walkStruct(descriptor, nested, path, physical, partitionKeys, identity);
    } else if (type instanceof ArrayType array) {
      walkField(
          descriptor,
          array.getElementField(),
          path.arrayElement(),
          physical + "[]",
          partitionKeys,
          1,
          identity);
    } else if (type instanceof MapType map) {
      walkField(
          descriptor,
          map.getKeyField(),
          path.mapKey(),
          physical + ".key",
          partitionKeys,
          1,
          identity);
      walkField(
          descriptor,
          map.getValueField(),
          path.mapValue(),
          physical + "{}",
          partitionKeys,
          2,
          identity);
    }
  }

  private static boolean isContainer(DataType type) {
    return type instanceof StructType || type instanceof ArrayType || type instanceof MapType;
  }

  public static Map<String, LogicalType> deltaTypeMap(StructType schema) {
    Map<String, LogicalType> result = new LinkedHashMap<>();
    for (StructField field : schema.fields()) {
      result.put(field.getName(), toLogical(field.getDataType()));
    }
    return result;
  }

  /** Collection contents are excluded because Delta log statistics do not describe them. */
  public static Map<String, LogicalType> deltaStatsTypeMap(StructType schema) {
    LegacyDottedKeyIndex<LogicalType> index = LegacyDottedKeyIndex.create();
    collectStatsTypes(schema, ColumnPath.ROOT, index);
    return index.values();
  }

  private static void collectStatsTypes(
      StructType schema, ColumnPath prefix, LegacyDottedKeyIndex<LogicalType> index) {
    for (StructField field : schema.fields()) {
      ColumnPath path = prefix.field(field.getName());
      index.add(path, toLogical(field.getDataType()));
      if (field.getDataType() instanceof StructType nested) {
        collectStatsTypes(nested, path, index);
      }
    }
  }

  public static LogicalType toLogical(DataType type) {
    if (type instanceof BooleanType) return LogicalType.of(LogicalKind.BOOLEAN);
    if (type instanceof ByteType
        || type instanceof ShortType
        || type instanceof IntegerType
        || type instanceof LongType) return LogicalType.of(LogicalKind.INT);
    if (type instanceof FloatType) return LogicalType.of(LogicalKind.FLOAT);
    if (type instanceof DoubleType) return LogicalType.of(LogicalKind.DOUBLE);
    if (type instanceof StringType) return LogicalType.of(LogicalKind.STRING);
    if (type instanceof BinaryType) return LogicalType.of(LogicalKind.BINARY);
    if (type instanceof DateType) return LogicalType.of(LogicalKind.DATE);
    if (type instanceof TimestampType) return LogicalType.of(LogicalKind.TIMESTAMPTZ);
    if (type instanceof TimestampNTZType) return LogicalType.of(LogicalKind.TIMESTAMP);
    if (type instanceof ArrayType array) {
      return LogicalType.array(toLogical(array.getElementType()), array.containsNull());
    }
    if (type instanceof MapType map) {
      return LogicalType.map(
          toLogical(map.getKeyType()), toLogical(map.getValueType()), map.isValueContainsNull());
    }
    if (type instanceof StructType struct) {
      List<LogicalField> fields =
          struct.fields().stream()
              .map(
                  field ->
                      new LogicalField(
                          field.getName(), field.isNullable(), toLogical(field.getDataType())))
              .toList();
      return LogicalType.struct(fields);
    }
    if (type instanceof VariantType) return LogicalType.of(LogicalKind.VARIANT);
    if (type instanceof DecimalType decimal) {
      if (decimal.getPrecision() > MAX_DECIMAL_PRECISION) {
        throw new IllegalArgumentException(
            "Unsupported DECIMAL precision "
                + decimal.getPrecision()
                + " for Delta declared type '"
                + decimal
                + "'; max supported precision is "
                + MAX_DECIMAL_PRECISION);
      }
      return LogicalType.decimal(decimal.getPrecision(), decimal.getScale());
    }
    throw new IllegalArgumentException(
        "Unrecognised Delta type: " + type.getClass().getSimpleName());
  }
}
