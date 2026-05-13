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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.junit.jupiter.api.Test;

class DeltaSchemaNormalizerTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void normalizeAssignsFieldIdsToDeltaPrimitiveSchema() throws Exception {
    String deltaSchemaJson =
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"name\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}";

    String normalized = DeltaSchemaNormalizer.normalizeSchemaJson(deltaSchemaJson, 7);

    @SuppressWarnings("unchecked")
    Map<String, Object> root = JSON.readValue(normalized, Map.class);
    assertEquals(7, root.get("schema-id"));
    assertEquals(2, root.get("last-column-id"));
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fields = (List<Map<String, Object>>) root.get("fields");
    assertEquals(1, fields.get(0).get("id"));
    assertEquals("int", fields.get(0).get("type"));
    assertEquals(false, fields.get(0).get("required"));
    assertEquals(2, fields.get(1).get("id"));
    assertEquals(true, fields.get(1).get("required"));

    Schema schema = SchemaParser.fromJson(normalized);
    assertEquals(2, schema.columns().size());
    assertEquals("id", schema.columns().get(0).name());
    assertEquals(1, schema.columns().get(0).fieldId());
  }

  @Test
  void normalizePreservesDeltaColumnMappingIdsAndNestedStructs() {
    String deltaSchemaJson =
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"meta\",\"type\":{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":5}},"
            + "{\"name\":\"b\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}"
            + "]},\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":4}}]}";

    Map<String, Object> normalized = DeltaSchemaNormalizer.normalizeSchemaMap(deltaSchemaJson, 0);
    assertNotNull(normalized);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fields = (List<Map<String, Object>>) normalized.get("fields");
    assertEquals(4, fields.get(0).get("id"));
    assertInstanceOf(Map.class, fields.get(0).get("type"));

    @SuppressWarnings("unchecked")
    Map<String, Object> metaType = (Map<String, Object>) fields.get(0).get("type");
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> nestedFields = (List<Map<String, Object>>) metaType.get("fields");
    assertEquals(5, nestedFields.get(0).get("id"));
    assertEquals(6, nestedFields.get(1).get("id"));
  }
}
