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

import ai.floedb.floecat.connector.common.resolver.IcebergTypeMappings;
import ai.floedb.floecat.types.LogicalType;
import org.apache.iceberg.types.Type;

/**
 * Maps Iceberg native types to FloeCat canonical {@link LogicalType}.
 *
 * <p><b>Module boundary note:</b> This mapper delegates to {@link IcebergTypeMappings} in {@code
 * core/connectors/common} so that schema and stats paths share the exact same mapping and
 * validation.
 *
 * <p><b>Timestamp semantics:</b> Iceberg {@code TimestampType.withZone()} is UTC-normalised →
 * {@link ai.floedb.floecat.types.LogicalKind#TIMESTAMPTZ}. Iceberg {@code
 * TimestampType.withoutZone()} is timezone-naive → {@link
 * ai.floedb.floecat.types.LogicalKind#TIMESTAMP}.
 *
 * <p><b>Integer collapsing:</b> Both Iceberg {@code INTEGER} (32-bit) and {@code LONG} (64-bit)
 * collapse to canonical {@link ai.floedb.floecat.types.LogicalKind#INT} (64-bit).
 *
 * <p><b>Complex types:</b> {@code LIST} → {@link ai.floedb.floecat.types.LogicalKind#ARRAY}, {@code
 * MAP} → {@link ai.floedb.floecat.types.LogicalKind#MAP}, {@code STRUCT} → {@link
 * ai.floedb.floecat.types.LogicalKind#STRUCT}. Element/value/field types are captured by child
 * {@code SchemaColumn} rows with their own paths.
 *
 * <p><b>Unrecognised/unsupported types</b> fail fast with {@link IllegalArgumentException}.
 */
final class IcebergTypeMapper {
  /**
   * Converts an Iceberg {@link Type} to a FloeCat canonical {@link LogicalType}.
   *
   * @param t an Iceberg type (never null)
   * @return the corresponding canonical {@link LogicalType}
   */
  static LogicalType toLogical(Type t) {
    return IcebergTypeMappings.toLogical(t);
  }
}
