package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
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
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import java.util.LinkedHashMap;
import java.util.Map;

final class DeltaTypeMapper {

  static Map<String, LogicalType> deltaTypeMap(StructType st) {
    Map<String, LogicalType> out = new LinkedHashMap<>();
    for (StructField f : st.fields()) {
      LogicalType lt = toLogical(f.getDataType());
      if (lt != null) {
        out.put(f.getName(), lt);
      }
    }
    return out;
  }

  static LogicalType toLogical(DataType dt) {
    if (dt instanceof BooleanType) return LogicalType.of(LogicalKind.BOOLEAN);
    if (dt instanceof ByteType || dt instanceof ShortType) return LogicalType.of(LogicalKind.INT16);
    if (dt instanceof IntegerType) return LogicalType.of(LogicalKind.INT32);
    if (dt instanceof LongType) return LogicalType.of(LogicalKind.INT64);
    if (dt instanceof FloatType) return LogicalType.of(LogicalKind.FLOAT32);
    if (dt instanceof DoubleType) return LogicalType.of(LogicalKind.FLOAT64);
    if (dt instanceof StringType) return LogicalType.of(LogicalKind.STRING);
    if (dt instanceof BinaryType) return LogicalType.of(LogicalKind.BINARY);
    if (dt instanceof DateType) return LogicalType.of(LogicalKind.DATE);
    if (dt instanceof TimestampType) return LogicalType.of(LogicalKind.TIMESTAMP);
    if (dt instanceof DecimalType dec)
      return LogicalType.decimal(dec.getPrecision(), dec.getScale());
    return null;
  }
}
