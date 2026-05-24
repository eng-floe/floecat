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
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
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
import java.util.Set;

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
      try {
        StructType root = DataTypeJsonSerDe.deserializeStructType(schemaJson);
        walkDeltaStruct(cid_algo, sb, root, "", effectivePartitionKeys);
      } catch (Exception kernelFailure) {
        JsonNode root = MAPPER.readTree(schemaJson);
        JsonNode fields = root.get("fields");
        if (fields == null || !fields.isArray()) {
          throw new IllegalArgumentException("Delta schema JSON must contain a 'fields' array");
        }
        walkFallbackStruct(cid_algo, sb, root, "", effectivePartitionKeys);
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
      Set<String> partitionKeys) {
    if (structType == null) {
      return;
    }

    for (int i = 0; i < structType.fields().size(); i++) {
      StructField field = structType.fields().get(i);
      DataType dataType = field.getDataType();
      String name = field.getName();
      String physical = prefix.isEmpty() ? name : prefix + "." + name;
      boolean isPartition = partitionKeys.contains(name) || partitionKeys.contains(physical);

      sb.addColumns(
          ColumnIdComputer.withComputedId(
              cid_algo,
              SchemaColumn.newBuilder()
                  .setName(name)
                  .setLogicalType(LogicalTypeFormat.format(toLogicalType(dataType)))
                  .setFieldId(extractFieldId(field.getMetadata()))
                  .setNullable(field.isNullable())
                  .setPhysicalPath(physical)
                  .setPartitionKey(isPartition)
                  .setOrdinal(i + 1)
                  .setLeaf(!isContainerType(dataType))
                  .build()));

      if (dataType instanceof StructType nestedStruct) {
        walkDeltaStruct(cid_algo, sb, nestedStruct, physical, partitionKeys);
      } else if (dataType instanceof ArrayType arrayType
          && arrayType.getElementType() instanceof StructType elementStruct) {
        walkDeltaStruct(cid_algo, sb, elementStruct, physical + "[]", partitionKeys);
      } else if (dataType instanceof MapType mapType
          && mapType.getValueType() instanceof StructType valueStruct) {
        walkDeltaStruct(cid_algo, sb, valueStruct, physical + "{}", partitionKeys);
      }
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
    if (dataType instanceof ArrayType) return LogicalType.of(LogicalKind.ARRAY);
    if (dataType instanceof MapType) return LogicalType.of(LogicalKind.MAP);
    if (dataType instanceof StructType) return LogicalType.of(LogicalKind.STRUCT);
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
      Set<String> partitionKeys) {
    if (node == null || !node.has("fields")) {
      return;
    }

    ArrayNode fields = (ArrayNode) node.get("fields");
    for (int i = 0; i < fields.size(); i++) {
      JsonNode field = fields.get(i);
      JsonNode typeNode = field.get("type");
      String name = field.path("name").asText();
      String physical = prefix.isEmpty() ? name : prefix + "." + name;
      boolean isPartition = partitionKeys.contains(name) || partitionKeys.contains(physical);

      sb.addColumns(
          ColumnIdComputer.withComputedId(
              cid_algo,
              SchemaColumn.newBuilder()
                  .setName(name)
                  .setLogicalType(deltaTypeToCanonical(typeNode))
                  .setFieldId(fallbackFieldId(field))
                  .setNullable(field.path("nullable").asBoolean(true))
                  .setPhysicalPath(physical)
                  .setPartitionKey(isPartition)
                  .setOrdinal(i + 1)
                  .setLeaf(!fallbackContainerType(typeNode))
                  .build()));

      if (typeNode == null || !typeNode.isObject()) {
        continue;
      }
      if ("struct".equals(typeNode.path("type").asText(""))) {
        walkFallbackStruct(cid_algo, sb, typeNode, physical, partitionKeys);
      } else if ("array".equals(typeNode.path("type").asText(""))) {
        JsonNode elem = typeNode.get("elementType");
        if (elem != null && elem.isObject() && "struct".equals(elem.path("type").asText(""))) {
          walkFallbackStruct(cid_algo, sb, elem, physical + "[]", partitionKeys);
        }
      } else if ("map".equals(typeNode.path("type").asText(""))) {
        JsonNode value = typeNode.get("valueType");
        if (value != null && value.isObject() && "struct".equals(value.path("type").asText(""))) {
          walkFallbackStruct(cid_algo, sb, value, physical + "{}", partitionKeys);
        }
      }
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

  private static String deltaTypeToCanonical(JsonNode typeNode) {
    if (typeNode == null) {
      throw new IllegalArgumentException("Delta field type is missing");
    }
    if (typeNode.isObject()) {
      return switch (typeNode.path("type").asText("")) {
        case "struct" -> "STRUCT";
        case "array" -> "ARRAY";
        case "map" -> "MAP";
        case "variant" -> "VARIANT";
        default ->
            throw new IllegalArgumentException(
                "Unrecognized Delta complex type: '" + typeNode.path("type").asText("") + "'");
      };
    }

    String raw = typeNode.asText("");
    String lowerRaw = raw.toLowerCase(java.util.Locale.ROOT);
    return switch (lowerRaw) {
      case "boolean" -> "BOOLEAN";
      case "byte", "tinyint", "short", "smallint", "integer", "int", "long", "bigint" -> "INT";
      case "float" -> "FLOAT";
      case "double" -> "DOUBLE";
      case "string" -> "STRING";
      case "binary" -> "BINARY";
      case "date" -> "DATE";
      case "timestamp" -> "TIMESTAMPTZ";
      case "timestamp_ntz" -> "TIMESTAMP";
      case "interval" -> "INTERVAL";
      default -> {
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
    DecimalPrecisionConstraints.validateDecimalPrecision(
        logicalType, "Delta", raw, MAX_DECIMAL_PRECISION);
    return LogicalTypeFormat.format(logicalType);
  }
}
