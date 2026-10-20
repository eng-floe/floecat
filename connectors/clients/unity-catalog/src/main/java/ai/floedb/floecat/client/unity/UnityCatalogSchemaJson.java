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

package ai.floedb.floecat.client.unity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Objects;

/** Builds Delta schema JSON without dropping field metadata carried by Unity {@code type_json}. */
public final class UnityCatalogSchemaJson {
  private static final ObjectMapper JSON = new ObjectMapper();

  private UnityCatalogSchemaJson() {}

  public static String fromColumns(List<UnityCatalogTable.Column> columns) {
    Objects.requireNonNull(columns, "columns");
    var fields = JSON.createArrayNode();
    for (UnityCatalogTable.Column column : columns) {
      fields.add(field(column));
    }
    ObjectNode schema = JSON.createObjectNode();
    schema.put("type", "struct");
    schema.set("fields", fields);
    return schema.toString();
  }

  /** Resolves one complete column while preserving authoritative field metadata. */
  public static ObjectNode field(UnityCatalogTable.Column column) {
    ObjectNode parsed = fieldFromJson(column.typeJson(), column.name());
    ObjectNode field = JSON.createObjectNode();
    field.put("name", column.name());
    if (parsed == null) {
      String declared = column.typeText() != null ? column.typeText() : column.typeName();
      if (declared == null || declared.isBlank()) {
        throw new IllegalArgumentException(
            "Unity Catalog column states no type: column=" + column.name());
      }
      field.set("type", UnityCatalogTypeText.toDeltaType(declared));
    } else {
      field.set("type", parsed.get("type"));
    }
    field.put("nullable", column.nullable());
    if (parsed != null) {
      parsed
          .properties()
          .forEach(
              entry -> {
                if (!field.has(entry.getKey())) {
                  field.set(entry.getKey(), entry.getValue());
                }
              });
    }
    return field;
  }

  private static ObjectNode fieldFromJson(String value, String columnName) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      JsonNode parsed = JSON.readTree(value);
      if (!parsed.isObject() || parsed.get("type") == null || parsed.get("type").isNull()) {
        throw new IllegalArgumentException(
            "Unity type_json must be an object with a type: column=" + columnName);
      }
      return ((ObjectNode) parsed).deepCopy();
    } catch (com.fasterxml.jackson.core.JsonProcessingException invalid) {
      throw new IllegalArgumentException("Invalid Unity type_json: column=" + columnName, invalid);
    }
  }
}
