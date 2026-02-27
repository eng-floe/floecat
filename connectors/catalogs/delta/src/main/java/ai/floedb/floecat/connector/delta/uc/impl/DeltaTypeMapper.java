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

package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
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
import java.util.Map;

/**
 * Maps Delta Kernel {@link DataType} instances to FloeCat canonical {@link LogicalType}.
 *
 * <p>This class operates on Delta Kernel's typed object model (used when reading Delta tables via
 * the Kernel API). For Delta's JSON-schema representation (used when reading table metadata from
 * Unity Catalog or checkpoint files), see {@code DeltaSchemaMapper.deltaTypeToCanonical()}.
 *
 * <p><b>Timestamp semantics:</b> Delta's {@link TimestampType} is always UTC-stored → {@link
 * LogicalKind#TIMESTAMPTZ}. Delta's {@link TimestampNTZType} is timezone-naive → {@link
 * LogicalKind#TIMESTAMP}. The Floe spec decode matrix v1 has these two entries inverted; this
 * implementation applies the semantically correct mapping and records the discrepancy in code
 * comments.
 *
 * <p><b>Integer collapsing:</b> All Delta integer sizes ({@link ByteType}, {@link ShortType},
 * {@link IntegerType}, {@link LongType}) collapse to canonical {@link LogicalKind#INT} (64-bit).
 *
 * <p><b>Unrecognised types</b> fail fast with {@link IllegalArgumentException}.
 */
final class DeltaTypeMapper {

  /**
   * Maps all fields in a Delta {@link StructType} to canonical logical types.
   *
   * @param st top-level Delta struct schema
   * @return ordered map of field name → {@link LogicalType}
   */
  static Map<String, LogicalType> deltaTypeMap(StructType st) {
    Map<String, LogicalType> out = new LinkedHashMap<>();
    for (StructField f : st.fields()) {
      LogicalType lt = toLogical(f.getDataType());
      out.put(f.getName(), lt);
    }
    return out;
  }

  /**
   * Converts a single Delta Kernel {@link DataType} to a FloeCat canonical {@link LogicalType}.
   *
   * @param dt a Delta Kernel data type (never null)
   * @return the corresponding canonical {@link LogicalType}
   */
  static LogicalType toLogical(DataType dt) {
    if (dt instanceof BooleanType) return LogicalType.of(LogicalKind.BOOLEAN);
    if (dt instanceof ByteType
        || dt instanceof ShortType
        || dt instanceof IntegerType
        || dt instanceof LongType) return LogicalType.of(LogicalKind.INT);
    if (dt instanceof FloatType) return LogicalType.of(LogicalKind.FLOAT);
    if (dt instanceof DoubleType) return LogicalType.of(LogicalKind.DOUBLE);
    if (dt instanceof StringType) return LogicalType.of(LogicalKind.STRING);
    if (dt instanceof BinaryType) return LogicalType.of(LogicalKind.BINARY);
    if (dt instanceof DateType) return LogicalType.of(LogicalKind.DATE);
    // Delta TimestampType is UTC-stored → TIMESTAMPTZ (adjusted to UTC).
    // Delta TimestampNTZType is timezone-naive (local) → TIMESTAMP (no UTC normalisation).
    if (dt instanceof TimestampType) return LogicalType.of(LogicalKind.TIMESTAMPTZ);
    if (dt instanceof TimestampNTZType) return LogicalType.of(LogicalKind.TIMESTAMP);
    if (dt instanceof ArrayType) return LogicalType.of(LogicalKind.ARRAY);
    if (dt instanceof MapType) return LogicalType.of(LogicalKind.MAP);
    if (dt instanceof StructType) return LogicalType.of(LogicalKind.STRUCT);
    if (dt instanceof VariantType) return LogicalType.of(LogicalKind.VARIANT);
    if (dt instanceof DecimalType dec)
      return LogicalType.decimal(dec.getPrecision(), dec.getScale());

    throw new IllegalArgumentException("Unrecognised Delta type: " + dt.getClass().getSimpleName());
  }
}
