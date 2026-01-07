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

package ai.floedb.floecat.systemcatalog.registry;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.systemcatalog.def.*;
import java.util.List;
import org.junit.jupiter.api.Test;

final class SystemCatalogValidatorTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static NameRef name(String n) {
    return NameRef.newBuilder().setName(n).build();
  }

  private static SystemTypeDef type(String name) {
    return new SystemTypeDef(name(name), "scalar", false, null, List.of());
  }

  // ---------------------------------------------------------------------------
  // Null / empty catalog
  // ---------------------------------------------------------------------------

  @Test
  void validate_nullCatalog() {
    List<String> errors = SystemCatalogValidator.validate(null);
    assertThat(errors).containsExactly("catalog.null");
  }

  @Test
  void validate_emptyTypes() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(), // functions
            List.of(), // operators
            List.of(), // types
            List.of(), // casts
            List.of(), // collations
            List.of(), // aggregates
            List.of(), // namespaces
            List.of(), // tables
            List.of() // views
            );

    List<String> errors = SystemCatalogValidator.validate(catalog);
    assertThat(errors).contains("types.empty");
  }

  // ---------------------------------------------------------------------------
  // Types
  // ---------------------------------------------------------------------------

  @Test
  void validate_duplicateTypes() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(type("int"), type("int")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<String> errors = SystemCatalogValidator.validate(catalog);
    assertThat(errors).contains("type.duplicate:int");
  }

  // ---------------------------------------------------------------------------
  // Functions
  // ---------------------------------------------------------------------------

  @Test
  void validate_functionWithUnknownReturnType() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    name("f"), List.of(), name("missing"), false, false, List.of())),
            List.of(),
            List.of(type("int")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<String> errors = SystemCatalogValidator.validate(catalog);
    assertThat(errors).contains("function.return:f.type.unknown:missing");
  }

  @Test
  void validate_functionWithUnknownArgumentType() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    name("f"), List.of(name("missing")), name("int"), false, false, List.of())),
            List.of(),
            List.of(type("int")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<String> errors = SystemCatalogValidator.validate(catalog);
    assertThat(errors).contains("function.arg:f.type.unknown:missing");
  }

  // ---------------------------------------------------------------------------
  // Operators
  // ---------------------------------------------------------------------------

  @Test
  void validate_operatorUnknownTypes() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(
                new SystemOperatorDef(
                    name("+"), name("int"), name("missing"), name("int"), true, true, List.of())),
            List.of(type("int")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<String> errors = SystemCatalogValidator.validate(catalog);
    assertThat(errors).contains("operator.right:+.type.unknown:missing");
  }

  // ---------------------------------------------------------------------------
  // Casts
  // ---------------------------------------------------------------------------

  @Test
  void validate_duplicateCasts() {
    SystemCastDef cast =
        new SystemCastDef(
            name("c"), name("int"), name("int"), SystemCastMethod.IMPLICIT, List.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(type("int")),
            List.of(cast, cast),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<String> errors = SystemCatalogValidator.validate(catalog);
    assertThat(errors).contains("cast.duplicate:c");
  }

  @Test
  void validate_castUnknownTypes() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(type("int")),
            List.of(
                new SystemCastDef(
                    name("c"), name("missing"), name("int"), SystemCastMethod.EXPLICIT, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<String> errors = SystemCatalogValidator.validate(catalog);
    assertThat(errors).contains("cast.source.type.unknown:missing");
  }

  // ---------------------------------------------------------------------------
  // Collations
  // ---------------------------------------------------------------------------

  @Test
  void validate_duplicateCollations() {
    SystemCollationDef coll = new SystemCollationDef(name("c"), "en_US", List.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(type("int")),
            List.of(),
            List.of(coll, coll),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<String> errors = SystemCatalogValidator.validate(catalog);
    assertThat(errors).contains("collation.duplicate:c");
  }

  // ---------------------------------------------------------------------------
  // Aggregates
  // ---------------------------------------------------------------------------

  @Test
  void validate_aggregateUnknownTypes() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(type("int")),
            List.of(),
            List.of(),
            List.of(
                new SystemAggregateDef(
                    name("sum"), List.of(name("missing")), name("int"), name("int"), List.of())),
            List.of(),
            List.of(),
            List.of());

    List<String> errors = SystemCatalogValidator.validate(catalog);
    assertThat(errors).contains("agg.arg:sum.type.unknown:missing");
  }

  // ---------------------------------------------------------------------------
  // Happy path
  // ---------------------------------------------------------------------------

  @Test
  void validate_validCatalog_hasNoErrors() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    name("f"), List.of(name("int")), name("int"), false, false, List.of())),
            List.of(),
            List.of(type("int")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<String> errors = SystemCatalogValidator.validate(catalog);
    assertThat(errors).isEmpty();
  }
}
