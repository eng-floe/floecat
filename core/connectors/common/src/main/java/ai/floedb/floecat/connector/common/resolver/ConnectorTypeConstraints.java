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

import ai.floedb.floecat.types.LogicalType;

/**
 * Shared validation rules for type constraints that apply across all connectors.
 *
 * <p>Currently enforces the FloeCat DECIMAL precision ceiling of {@value #MAX_DECIMAL_PRECISION}.
 * This ceiling matches Iceberg, Delta Lake, and the FloeDB execution engine ({@code
 * FloeTypeMapper}). Sources that may declare higher precisions (e.g. BigQuery {@code BIGNUMERIC} up
 * to 76) are rejected at schema-parse time with a clear error message.
 *
 * <p>Note: {@link ai.floedb.floecat.types.LogicalType} itself imposes no upper bound on precision —
 * that is intentionally a connector-layer concern so that the canonical model can represent future
 * higher-precision sources without a core-library change.
 */
final class ConnectorTypeConstraints {
  /** Maximum DECIMAL precision supported by Iceberg, Delta, and the FloeDB execution engine. */
  static final int MAX_DECIMAL_PRECISION = 38;

  private ConnectorTypeConstraints() {}

  /**
   * Validates that a DECIMAL type's precision does not exceed {@value #MAX_DECIMAL_PRECISION}.
   *
   * @param logicalType the type to validate (no-op if null or non-DECIMAL)
   * @param sourceSystem human-readable source system name for error messages (e.g. "Iceberg")
   * @param declaredType the raw declared type string for error messages (e.g. "decimal(40,2)")
   * @throws IllegalArgumentException if the precision exceeds {@value #MAX_DECIMAL_PRECISION}
   */
  static void validateDecimalPrecision(
      LogicalType logicalType, String sourceSystem, String declaredType) {
    if (logicalType == null || !logicalType.isDecimal()) {
      return;
    }
    int precision = logicalType.precision();
    if (precision > MAX_DECIMAL_PRECISION) {
      throw new IllegalArgumentException(
          "Unsupported DECIMAL precision "
              + precision
              + " for "
              + sourceSystem
              + " declared type '"
              + declaredType
              + "'; max supported precision is "
              + MAX_DECIMAL_PRECISION);
    }
  }
}
