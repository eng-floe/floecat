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

/** Delta's {@code delta.columnMapping.mode}. */
public enum ColumnMappingMode {
  NONE,
  ID,
  NAME;

  public static final String PROPERTY_KEY = "delta.columnMapping.mode";

  /**
   * Parses a mode. A null or blank property means the table predates column mapping and is {@link
   * #NONE}; an unrecognised value is rejected rather than being taken for {@code none}, because
   * silently treating an unknown mode as unmapped would pick the wrong identity source and read
   * physical names as though they were logical ones.
   */
  public static ColumnMappingMode parse(String raw) {
    if (raw == null || raw.isBlank()) {
      return NONE;
    }
    return switch (raw.trim().toLowerCase(Locale.ROOT)) {
      case "none" -> NONE;
      case "id" -> ID;
      case "name" -> NAME;
      default ->
          throw new IllegalArgumentException(
              "Unsupported " + PROPERTY_KEY + "='" + raw + "'; expected one of none, id, name");
    };
  }

  /** Reads the mode out of a table's properties. */
  public static ColumnMappingMode fromProperties(Map<String, String> properties) {
    return properties == null || properties.isEmpty() ? NONE : parse(properties.get(PROPERTY_KEY));
  }

  /** True when the data files carry physical names rather than the schema's logical names. */
  public boolean isEnabled() {
    return this != NONE;
  }
}
