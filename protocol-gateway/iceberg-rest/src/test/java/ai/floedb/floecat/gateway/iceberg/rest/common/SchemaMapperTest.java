package ai.floedb.floecat.gateway.iceberg.rest.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
                            "source-id", 1, "transform", "identity", "null-order", "NULLS_LAST")),
                    new LinkedHashMap<>(Map.of("source-id", 2)))));

    SchemaMapper.normalizeSortOrder(order);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fields = (List<Map<String, Object>>) order.get("fields");
    assertEquals("nulls-last", fields.get(0).get("null-order"));
    assertEquals("nulls-first", fields.get(1).get("null-order"));
  }

  @Test
  void normalizeSortOrderAssignsPositiveOrderIdWhenSorted() {
    Map<String, Object> order =
        new LinkedHashMap<>(
            Map.of(
                "sort-order-id",
                0,
                "fields",
                List.of(new LinkedHashMap<>(Map.of("source-id", 3)))));

    SchemaMapper.normalizeSortOrder(order);

    assertEquals(1, order.get("sort-order-id"));
    assertEquals(1, order.get("order-id"));
  }
}
