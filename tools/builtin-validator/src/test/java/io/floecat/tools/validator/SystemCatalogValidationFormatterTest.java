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
