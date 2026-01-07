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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchemaMapperTest {

  @Test
  void normalizeSortOrderCanonicalizesNullOrder() {
    Map<String, Object> order =
        new LinkedHashMap<>(
            Map.of(
                "order-id",
                1,
                "fields",
                List.of(
                    new LinkedHashMap<>(
                        Map.of(
                            "source-id",
                            1,
                            "transform",
                            "identity",
                            "direction",
                            "desc",
                            "null-order",
                            "NULLS_LAST")),
                    new LinkedHashMap<>(
                        Map.of(
                            "source-id",
                            2,
                            "transform",
                            "identity",
                            "direction",
                            "asc",
                            "null-order",
                            "nulls_first")))));

    SchemaMapper.normalizeSortOrder(order);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fields = (List<Map<String, Object>>) order.get("fields");
    assertEquals("nulls-last", fields.get(0).get("null-order"));
    assertEquals("nulls-first", fields.get(1).get("null-order"));
  }

  @Test
  void normalizeSortOrderRejectsMissingOrderId() {
    Map<String, Object> order =
        new LinkedHashMap<>(
            Map.of(
                "sort-order-id",
                0,
                "fields",
                List.of(
                    new LinkedHashMap<>(
                        Map.of(
                            "source-id",
                            3,
                            "transform",
                            "identity",
                            "direction",
                            "asc",
                            "null-order",
                            "nulls-first")))));

    assertThrows(IllegalArgumentException.class, () -> SchemaMapper.normalizeSortOrder(order));
  }
}
