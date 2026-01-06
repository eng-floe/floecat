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

package ai.floedb.floecat.connector.iceberg.impl;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
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
