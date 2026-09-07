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

package ai.floedb.floecat.schema.identity.delta;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/** A Delta column mapping mode. */
public enum ColumnMappingMode {
  NONE,
  ID,
  NAME;

  public static final String PROPERTY = "delta.columnMapping.mode";

  /** Reads the configured mode from table properties; the caller must also check the protocol. */
  public static ColumnMappingMode fromTableProperties(Map<String, String> properties) {
    String value = properties == null ? null : properties.get(PROPERTY);
    if (value == null || value.isBlank() || "none".equalsIgnoreCase(value.trim())) {
      return NONE;
    }
    return switch (value.trim().toLowerCase(Locale.ROOT)) {
      case "id" -> ID;
      case "name" -> NAME;
      default -> throw new IllegalArgumentException("Unsupported " + PROPERTY + "='" + value + "'");
    };
  }

  public boolean isEnabled() {
    return this != NONE;
  }

  /**
   * Narrows a configured mode to the one readers may actually trust.
   *
   * <p>Delta honours column mapping metadata only when the table protocol supports the column
   * mapping feature. A table that sets the property without that protocol support still stores
   * logical names in its data files and statistics, so it must be read as {@link #NONE}.
   */
  public ColumnMappingMode effective(boolean protocolSupportsColumnMapping) {
    return isEnabled() && protocolSupportsColumnMapping ? this : NONE;
  }

  /**
   * Reads the effective mode from table properties, consulting the protocol only when the
   * properties actually configure column mapping.
   *
   * <p>The supplier is deferred so callers that cannot cheaply reach a protocol — or cannot reach
   * one at all — are only asked for it when the answer can change the result.
   */
  public static ColumnMappingMode effectiveFromTableProperties(
      Map<String, String> properties, BooleanSupplier protocolSupportsColumnMapping) {
    Objects.requireNonNull(protocolSupportsColumnMapping, "protocolSupportsColumnMapping");
    ColumnMappingMode configured = fromTableProperties(properties);
    return configured.isEnabled()
        ? configured.effective(protocolSupportsColumnMapping.getAsBoolean())
        : NONE;
  }
}
