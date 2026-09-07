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
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityPathElement;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.SchemaIdentityEntry;
import ai.floedb.floecat.schema.identity.SchemaIdentityState;
import ai.floedb.floecat.types.LogicalField;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
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
import java.util.OptionalInt;
import java.util.Set;

/**
 * DeltaSchemaMapper: Converts Delta Lake schema JSON to logical SchemaDescriptor.
 *
 * <p>This parser intentionally delegates JSON decoding to Delta Kernel so we stay compatible with
 * real snapshot metadata emitted by Databricks/Delta Lake, including shapes our previous manual
 * parser did not understand.
 */
final class DeltaSchemaMapper {
  private static final String COLUMN_MAPPING_ID_KEY = "delta.columnMapping.id";
  private static final int MAX_DECIMAL_PRECISION = 38;

  private DeltaSchemaMapper() {}

  static SchemaDescriptor map(
      ColumnIdAlgorithm cid_algo, String schemaJson, Set<String> partitionKeys) {
    return map(cid_algo, schemaJson, partitionKeys, ColumnIdentityMap.getDefaultInstance());
  }

  static SchemaDescriptor map(
      ColumnIdAlgorithm cid_algo,
      String schemaJson,
      Set<String> partitionKeys,
      ColumnIdentityMap columnIdentityMap) {
    Set<String> effectivePartitionKeys = partitionKeys == null ? Set.of() : partitionKeys;
    Map<ColumnPath, Long> canonicalIds = canonicalIds(columnIdentityMap);
    final SchemaDescriptor descriptor;
    try {
      StructType root = DataTypeJsonSerDe.deserializeStructType(schemaJson);
      SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();
      walkDeltaStruct(
          cid_algo, canonicalIds, sb, root, ColumnPath.ROOT, "", effectivePartitionKeys);
      descriptor = sb.build();
    } catch (CanonicalIdentityException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse Delta schema JSON", e);
    }
    return validateCanonicalCoverage(cid_algo, canonicalIds, descriptor);
  }

  private static void walkDeltaStruct(
      ColumnIdAlgorithm cid_algo,
      Map<ColumnPath, Long> canonicalIds,
      SchemaDescriptor.Builder sb,
      StructType structType,
      ColumnPath logicalPrefix,
      String prefix,
      Set<String> partitionKeys) {
    if (structType == null) {
      return;
    }

    int ordinal = 0;
    for (StructField field : structType.fields()) {
      String name = field.getName();
      String physical = prefix.isEmpty() ? name : prefix + "." + name;
      walkDeltaField(
          cid_algo,
          canonicalIds,
          sb,
          field,
          logicalPrefix.field(name),
          physical,
          partitionKeys,
          ++ordinal);
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
      Map<ColumnPath, Long> canonicalIds,
      SchemaDescriptor.Builder sb,
      StructField field,
      ColumnPath logicalPath,
      String physical,
      Set<String> partitionKeys,
      int ordinal) {
    DataType dataType = field.getDataType();
    // Match by canonical path only: for top-level rows the path equals the name, and a bare-name
    // match would wrongly flag synthetic nested rows (a partition column literally named "key"
    // must not mark every map-key row).
    boolean isPartition = partitionKeys.contains(physical);
    LogicalType logicalType = toLogicalType(dataType);

    SchemaColumn source =
        SchemaColumn.newBuilder()
            .setName(field.getName())
            .setType(LogicalTypeProtoAdapter.toProto(logicalType))
            .setFieldId(extractFieldId(field.getMetadata()))
            .setNullable(field.isNullable())
            .setPhysicalPath(physical)
            .setPartitionKey(isPartition)
            .setOrdinal(ordinal)
            .setLeaf(!isContainerType(dataType))
            .build();
    sb.addColumns(withCanonicalId(cid_algo, canonicalIds, logicalPath, source));

    if (dataType instanceof StructType nestedStruct) {
      walkDeltaStruct(
          cid_algo, canonicalIds, sb, nestedStruct, logicalPath, physical, partitionKeys);
    } else if (dataType instanceof ArrayType arrayType) {
      walkDeltaField(
          cid_algo,
          canonicalIds,
          sb,
          arrayType.getElementField(),
          logicalPath.arrayElement(),
          physical + "[]",
          partitionKeys,
          1);
    } else if (dataType instanceof MapType mapType) {
      walkDeltaField(
          cid_algo,
          canonicalIds,
          sb,
          mapType.getKeyField(),
          logicalPath.mapKey(),
          physical + ".key",
          partitionKeys,
          1);
      walkDeltaField(
          cid_algo,
          canonicalIds,
          sb,
          mapType.getValueField(),
          logicalPath.mapValue(),
          physical + "{}",
          partitionKeys,
          2);
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

  private static LogicalType toLogicalType(DataType dataType) {
    if (dataType instanceof BooleanType) return LogicalType.of(LogicalKind.BOOLEAN);
    if (dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType
        || dataType instanceof LongType) {
      return LogicalType.of(LogicalKind.INT);
    }
    if (dataType instanceof FloatType) return LogicalType.of(LogicalKind.FLOAT);
    if (dataType instanceof DoubleType) return LogicalType.of(LogicalKind.DOUBLE);
    if (dataType instanceof StringType) return LogicalType.of(LogicalKind.STRING);
    if (dataType instanceof BinaryType) return LogicalType.of(LogicalKind.BINARY);
    if (dataType instanceof DateType) return LogicalType.of(LogicalKind.DATE);
    if (dataType instanceof TimestampType) return LogicalType.of(LogicalKind.TIMESTAMPTZ);
    if (dataType instanceof TimestampNTZType) return LogicalType.of(LogicalKind.TIMESTAMP);
    if (dataType instanceof ArrayType arrayType) {
      return LogicalType.array(toLogicalType(arrayType.getElementType()), arrayType.containsNull());
    }
    if (dataType instanceof MapType mapType) {
      return LogicalType.map(
          toLogicalType(mapType.getKeyType()),
          toLogicalType(mapType.getValueType()),
          mapType.isValueContainsNull());
    }
    if (dataType instanceof StructType structType) {
      List<LogicalField> fields =
          structType.fields().stream()
              .map(
                  f ->
                      new LogicalField(f.getName(), f.isNullable(), toLogicalType(f.getDataType())))
              .toList();
      // An explicitly empty source struct is a known-empty shape, not the legacy tag.
      return LogicalType.struct(fields);
    }
    if (dataType instanceof VariantType) return LogicalType.of(LogicalKind.VARIANT);
    if (dataType instanceof DecimalType decimalType) {
      LogicalType logicalType =
          LogicalType.decimal(decimalType.getPrecision(), decimalType.getScale());
      DecimalPrecisionConstraints.validateDecimalPrecision(
          logicalType, "Delta", decimalType.toString(), MAX_DECIMAL_PRECISION);
      return logicalType;
    }

    throw new IllegalArgumentException(
        "Unrecognized Delta type: '" + dataType.getClass().getSimpleName() + "'");
  }

  private static SchemaColumn withCanonicalId(
      ColumnIdAlgorithm algorithm,
      Map<ColumnPath, Long> canonicalIds,
      ColumnPath path,
      SchemaColumn source) {
    if (canonicalIds.isEmpty()) {
      return ColumnIdComputer.withComputedId(algorithm, source);
    }
    Long canonicalId = canonicalIds.get(path);
    if (canonicalId == null || canonicalId <= 0) {
      throw new CanonicalIdentityException("Column identity map has no ID for " + path.display());
    }
    return source.toBuilder().setId(canonicalId).build();
  }

  private static Map<ColumnPath, Long> canonicalIds(ColumnIdentityMap identityMap) {
    if (identityMap == null || identityMap.equals(ColumnIdentityMap.getDefaultInstance())) {
      return Map.of();
    }
    if (identityMap.getFormatVersion() != 1) {
      throw new IllegalArgumentException(
          "Unsupported column identity map format " + identityMap.getFormatVersion());
    }
    IdentityMode mode =
        switch (identityMap.getMode()) {
          case COLUMN_IDENTITY_MODE_NATIVE_FIELD_ID -> IdentityMode.NATIVE_FIELD_ID;
          case COLUMN_IDENTITY_MODE_STRUCTURED_PATH -> IdentityMode.STRUCTURED_PATH;
          default -> throw new IllegalArgumentException("Column identity map has no mode");
        };
    List<SchemaIdentityEntry> entries =
        identityMap.getEntriesList().stream()
            .map(
                entry ->
                    new SchemaIdentityEntry(
                        path(entry.getPathList()),
                        entry.hasNativeFieldId()
                            ? OptionalInt.of(entry.getNativeFieldId())
                            : OptionalInt.empty(),
                        entry.getColumnId()))
            .toList();
    SchemaIdentityState state =
        SchemaIdentityState.restore(
            identityMap.getSourceVersion(),
            identityMap.getHighWaterMark(),
            mode,
            entries,
            identityMap.getFingerprint());
    Map<ColumnPath, Long> result = new LinkedHashMap<>();
    state
        .entries()
        .forEach(
            entry -> {
              Long duplicate = result.putIfAbsent(entry.path(), entry.canonicalId());
              if (duplicate != null) {
                throw new IllegalArgumentException(
                    "Duplicate column identity path " + entry.path().display());
              }
            });
    return Map.copyOf(result);
  }

  private static SchemaDescriptor validateCanonicalCoverage(
      ColumnIdAlgorithm algorithm,
      Map<ColumnPath, Long> canonicalIds,
      SchemaDescriptor descriptor) {
    if (algorithm != ColumnIdAlgorithm.CID_CANONICAL_MAP) {
      return descriptor;
    }
    if (canonicalIds.isEmpty()) {
      throw new CanonicalIdentityException("Canonical column identity map is required");
    }
    if (descriptor.getColumnsCount() != canonicalIds.size()) {
      throw new CanonicalIdentityException(
          "Column identity map does not exactly match the Delta schema");
    }
    return descriptor;
  }

  private static ColumnPath path(List<ColumnIdentityPathElement> elements) {
    ColumnPath result = ColumnPath.ROOT;
    for (ColumnIdentityPathElement element : elements) {
      result =
          switch (element.getKind()) {
            case COLUMN_IDENTITY_PATH_ELEMENT_KIND_FIELD -> result.field(element.getName());
            case COLUMN_IDENTITY_PATH_ELEMENT_KIND_ARRAY_ELEMENT -> result.arrayElement();
            case COLUMN_IDENTITY_PATH_ELEMENT_KIND_MAP_KEY -> result.mapKey();
            case COLUMN_IDENTITY_PATH_ELEMENT_KIND_MAP_VALUE -> result.mapValue();
            default -> throw new IllegalArgumentException("Column identity path has no kind");
          };
    }
    return result;
  }

  private static final class CanonicalIdentityException extends IllegalArgumentException {
    private CanonicalIdentityException(String message) {
      super(message);
    }
  }
}
