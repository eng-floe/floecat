package io.floecat.tools.validator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.systemcatalog.registry.SystemCatalogValidationFormatter;
import java.util.List;
import org.junit.jupiter.api.Test;

class SystemCatalogValidationFormatterTest {

  @Test
  void sharedFormatterProducesDescriptions() {
    String description =
        SystemCatalogValidationFormatter.describeError("type.category.required:pg_catalog.int4");
    assertFalse(description.isBlank());
    assertTrue(description.contains("pg_catalog.int4"));
  }

  @Test
  void formatterHandlesMultipleCodes() {
    List<String> codes =
        List.of(
            "type.duplicate:pg_catalog.int4",
            "function.duplicate:pg_catalog.foobar",
            "operator.duplicate:pg_catalog.+(pg_catalog.int4,pg_catalog.int4)",
            "cast.duplicate:pg_catalog.int4->pg_catalog.text",
            "agg.duplicate:pg_catalog.sum(pg_catalog.int4)",
            "operator.function.missing:pg_catalog.missing",
            "type.element.required:pg_catalog._int4",
            "type.category.invalid:pg_catalog.bool");

    for (String code : codes) {
      String description = SystemCatalogValidationFormatter.describeError(code);
      assertFalse(description.isBlank(), "description blank for " + code);
      assertFalse(description.equals(code), "formatter returned raw code for " + code);
    }
  }
}
