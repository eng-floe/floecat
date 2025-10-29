package ai.floedb.metacat.connector.iceberg.impl;

import ai.floedb.metacat.types.LogicalKind;
import ai.floedb.metacat.types.LogicalType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

final class IcebergTypeMapper {
  static LogicalType toLogical(Type t) {
    return switch (t.typeId()) {
      case BOOLEAN -> LogicalType.of(LogicalKind.BOOLEAN);
      case INTEGER -> LogicalType.of(LogicalKind.INT32);
      case LONG -> LogicalType.of(LogicalKind.INT64);
      case FLOAT -> LogicalType.of(LogicalKind.FLOAT32);
      case DOUBLE -> LogicalType.of(LogicalKind.FLOAT64);
      case DATE -> LogicalType.of(LogicalKind.DATE);
      case TIME -> LogicalType.of(LogicalKind.TIME);
      case TIMESTAMP -> LogicalType.of(LogicalKind.TIMESTAMP);
      case STRING -> LogicalType.of(LogicalKind.STRING);
      case BINARY -> LogicalType.of(LogicalKind.BINARY);
      case UUID -> LogicalType.of(LogicalKind.UUID);
      case DECIMAL -> {
        var d = (Types.DecimalType) t;
        yield LogicalType.decimal(d.precision(), d.scale());
      }
      default -> LogicalType.of(LogicalKind.BINARY);
    };
  }
}
