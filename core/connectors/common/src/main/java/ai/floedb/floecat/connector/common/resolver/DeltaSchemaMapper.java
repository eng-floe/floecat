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
import ai.floedb.floecat.types.LogicalField;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DeltaSchemaMapper: Converts Delta Lake schema JSON to logical SchemaDescriptor.
 *
 * <p>This parser intentionally delegates JSON decoding to Delta Kernel so we stay compatible with
 * real snapshot metadata emitted by Databricks/Delta Lake, including shapes our previous manual
 * parser did not understand.
 */
final class DeltaSchemaMapper {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String COLUMN_MAPPING_ID_KEY = "delta.columnMapping.id";
  private static final int MAX_DECIMAL_PRECISION = 38;

  private DeltaSchemaMapper() {}

  static SchemaDescriptor map(
      ColumnIdAlgorithm cid_algo, String schemaJson, Set<String> partitionKeys) {
    SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();

    try {
      Set<String> effectivePartitionKeys = partitionKeys == null ? Set.of() : partitionKeys;
      AtomicInteger ordinals = new AtomicInteger(0);
      try {
        StructType root = DataTypeJsonSerDe.deserializeStructType(schemaJson);
        walkDeltaStruct(cid_algo, sb, root, "", effectivePartitionKeys, ordinals);
      } catch (Exception kernelFailure) {
        JsonNode root = MAPPER.readTree(schemaJson);
        JsonNode fields = root.get("fields");
        if (fields == null || !fields.isArray()) {
          throw new IllegalArgumentException("Delta schema JSON must contain a 'fields' array");
        }
        walkFallbackStruct(cid_algo, sb, root, "", effectivePartitionKeys, ordinals);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse Delta schema JSON", e);
    }

    return sb.build();
  }

  private static void walkDeltaStruct(
      ColumnIdAlgorithm cid_algo,
      SchemaDescriptor.Builder sb,
      StructType structType,
      String prefix,
      Set<String> partitionKeys,
      AtomicInteger ordinals) {
    if (structType == null) {
      return;
    }

    for (StructField field : structType.fields()) {
      String name = field.getName();
      String physical = prefix.isEmpty() ? name : prefix + "." + name;
      walkDeltaField(cid_algo, sb, field, physical, partitionKeys, ordinals);
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
      AtomicInteger ordinals) {
    DataType dataType = field.getDataType();
    boolean isPartition =
        partitionKeys.contains(field.getName()) || partitionKeys.contains(physical);
    LogicalType logicalType = toLogicalType(dataType);

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
                .setOrdinal(ordinals.incrementAndGet())
                .setLeaf(!isContainerType(dataType))
                .build()));

    if (dataType instanceof StructType nestedStruct) {
      walkDeltaStruct(cid_algo, sb, nestedStruct, physical, partitionKeys, ordinals);
    } else if (dataType instanceof ArrayType arrayType) {
      walkDeltaField(
          cid_algo, sb, arrayType.getElementField(), physical + "[]", partitionKeys, ordinals);
    } else if (dataType instanceof MapType mapType) {
      walkDeltaField(
          cid_algo, sb, mapType.getKeyField(), physical + ".key", partitionKeys, ordinals);
      walkDeltaField(
          cid_algo, sb, mapType.getValueField(), physical + "{}", partitionKeys, ordinals);
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

  private static void walkFallbackStruct(
      ColumnIdAlgorithm cid_algo,
      SchemaDescriptor.Builder sb,
      JsonNode node,
      String prefix,
      Set<String> partitionKeys,
      AtomicInteger ordinals) {
    if (node == null || !node.has("fields")) {
      return;
    }

    ArrayNode fields = (ArrayNode) node.get("fields");
    for (int i = 0; i < fields.size(); i++) {
      JsonNode field = fields.get(i);
      String name = field.path("name").asText();
      String physical = prefix.isEmpty() ? name : prefix + "." + name;
      walkFallbackField(
          cid_algo,
          sb,
          name,
          field.get("type"),
          field.path("nullable").asBoolean(true),
          fallbackFieldId(field),
          physical,
          partitionKeys,
          ordinals);
    }
  }

  /** Fallback-branch counterpart of walkDeltaField: same node set and path notation. */
  private static void walkFallbackField(
      ColumnIdAlgorithm cid_algo,
      SchemaDescriptor.Builder sb,
      String name,
      JsonNode typeNode,
      boolean nullable,
      int fieldId,
      String physical,
      Set<String> partitionKeys,
      AtomicInteger ordinals) {
    boolean isPartition = partitionKeys.contains(name) || partitionKeys.contains(physical);
    LogicalType logicalType = fallbackLogicalType(typeNode);

    sb.addColumns(
        ColumnIdComputer.withComputedId(
            cid_algo,
            SchemaColumn.newBuilder()
                .setName(name)
                .setType(LogicalTypeProtoAdapter.toProto(logicalType))
                .setFieldId(fieldId)
                .setNullable(nullable)
                .setPhysicalPath(physical)
                .setPartitionKey(isPartition)
                .setOrdinal(ordinals.incrementAndGet())
                .setLeaf(!fallbackContainerType(typeNode))
                .build()));

    if (typeNode == null || !typeNode.isObject()) {
      return;
    }
    String tag = typeNode.path("type").asText("");
    switch (tag) {
      case "struct" ->
          walkFallbackStruct(cid_algo, sb, typeNode, physical, partitionKeys, ordinals);
      case "array" ->
          walkFallbackField(
              cid_algo,
              sb,
              "element",
              typeNode.get("elementType"),
              typeNode.path("containsNull").asBoolean(true),
              0,
              physical + "[]",
              partitionKeys,
              ordinals);
      case "map" -> {
        walkFallbackField(
            cid_algo,
            sb,
            "key",
            typeNode.get("keyType"),
            false,
            0,
            physical + ".key",
            partitionKeys,
            ordinals);
        walkFallbackField(
            cid_algo,
            sb,
            "value",
            typeNode.get("valueType"),
            typeNode.path("valueContainsNull").asBoolean(true),
            0,
            physical + "{}",
            partitionKeys,
            ordinals);
      }
      default -> {}
    }
  }

  private static boolean fallbackContainerType(JsonNode typeNode) {
    if (typeNode == null || !typeNode.isObject()) {
      return false;
    }
    String typeTag = typeNode.path("type").asText("");
    return "struct".equals(typeTag) || "array".equals(typeTag) || "map".equals(typeTag);
  }

  private static int fallbackFieldId(JsonNode field) {
    if (field == null) {
      return 0;
    }
    JsonNode metadata = field.get("metadata");
    if (metadata != null && metadata.isObject()) {
      JsonNode columnMappingId = metadata.get(COLUMN_MAPPING_ID_KEY);
      if (columnMappingId != null && columnMappingId.canConvertToInt()) {
        int id = columnMappingId.asInt(0);
        if (id > 0) {
          return id;
        }
      }
    }
    int fieldId = field.path("fieldId").asInt(0);
    return Math.max(fieldId, 0);
  }

  private static LogicalType fallbackLogicalType(JsonNode typeNode) {
    if (typeNode == null) {
      throw new IllegalArgumentException("Delta field type is missing");
    }
    if (typeNode.isObject()) {
      return switch (typeNode.path("type").asText("")) {
        case "struct" -> {
          JsonNode fields = typeNode.get("fields");
          if (fields == null || !fields.isArray()) {
            // No field list at all: unknown shape, keep the legacy tag.
            yield LogicalType.of(LogicalKind.STRUCT);
          }
          if (fields.isEmpty()) {
            // An explicit empty field list is a known-empty struct.
            yield LogicalType.struct(List.of());
          }
          List<LogicalField> structFields = new ArrayList<>();
          for (JsonNode f : fields) {
            structFields.add(
                new LogicalField(
                    f.path("name").asText(),
                    f.path("nullable").asBoolean(true),
                    fallbackLogicalType(f.get("type"))));
          }
          yield LogicalType.struct(structFields);
        }
        case "array" ->
            LogicalType.array(
                fallbackLogicalType(typeNode.get("elementType")),
                typeNode.path("containsNull").asBoolean(true));
        case "map" ->
            LogicalType.map(
                fallbackLogicalType(typeNode.get("keyType")),
                fallbackLogicalType(typeNode.get("valueType")),
                typeNode.path("valueContainsNull").asBoolean(true));
        case "variant" -> LogicalType.of(LogicalKind.VARIANT);
        default ->
            throw new IllegalArgumentException(
                "Unrecognized Delta complex type: '" + typeNode.path("type").asText("") + "'");
      };
    }

    String raw = typeNode.asText("");
    String lowerRaw = raw.toLowerCase(java.util.Locale.ROOT);
    return switch (lowerRaw) {
      case "boolean" -> LogicalType.of(LogicalKind.BOOLEAN);
      case "byte", "tinyint", "short", "smallint", "integer", "int", "long", "bigint" ->
          LogicalType.of(LogicalKind.INT);
      case "float" -> LogicalType.of(LogicalKind.FLOAT);
      case "double" -> LogicalType.of(LogicalKind.DOUBLE);
      case "string" -> LogicalType.of(LogicalKind.STRING);
      case "binary" -> LogicalType.of(LogicalKind.BINARY);
      case "date" -> LogicalType.of(LogicalKind.DATE);
      case "timestamp" -> LogicalType.of(LogicalKind.TIMESTAMPTZ);
      case "timestamp_ntz" -> LogicalType.of(LogicalKind.TIMESTAMP);
      case "interval" -> LogicalType.of(LogicalKind.INTERVAL);
      case "variant" -> LogicalType.of(LogicalKind.VARIANT);
      default -> {
        if (lowerRaw.startsWith("decimal")) {
          yield canonicalDeltaDecimal(raw);
        }
        throw new IllegalArgumentException("Unrecognized Delta scalar type: '" + raw + "'");
      }
    };
  }

  private static LogicalType canonicalDeltaDecimal(String raw) {
    final LogicalType logicalType;
    try {
      logicalType = LogicalTypeFormat.parse(raw);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid Delta decimal type: '" + raw + "'", e);
    }
    DecimalPrecisionConstraints.validateDecimalPrecision(
        logicalType, "Delta", raw, MAX_DECIMAL_PRECISION);
    return logicalType;
  }
}
