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

package ai.floedb.floecat.systemcatalog.validation;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.systemcatalog.def.*;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class SystemCatalogValidatorTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static NameRef name(String n) {
    return NameRef.newBuilder().setName(n).build();
  }

  private static SystemTypeDef type(String name) {
    return new SystemTypeDef(name(name), "S", false, null, List.of());
  }

  private static List<String> codes(List<ValidationIssue> issues) {
    return issues.stream().map(ValidationIssue::code).toList();
  }

  private static ValidationIssue findFirst(List<ValidationIssue> issues, String code) {
    return issues.stream().filter(i -> code.equals(i.code())).findFirst().orElse(null);
  }

  // ---------------------------------------------------------------------------
  // Null / empty catalog
  // ---------------------------------------------------------------------------

  @Test
  void validate_nullCatalog() {
    List<ValidationIssue> issues = SystemCatalogValidator.validate(null);
    assertThat(codes(issues)).containsExactly("catalog.null");

    ValidationIssue issue = issues.get(0);
    assertThat(issue.ctx()).isEqualTo("catalog");
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
            List.of(), // views
            List.of() // registry engineSpecific
            );

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("types.empty");
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
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("type.duplicate");

    ValidationIssue dup = findFirst(issues, "type.duplicate");
    assertThat(dup).isNotNull();
    assertThat(dup.ctx()).isEqualTo("type:int");
    assertThat(dup.args()).containsExactly("int");
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
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("function.return.type.unknown");

    ValidationIssue issue = findFirst(issues, "function.return.type.unknown");
    assertThat(issue).isNotNull();
    assertThat(issue.ctx()).isEqualTo("function:f.returnType");
    assertThat(issue.args()).containsExactly("missing");
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
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("function.arg.type.unknown");

    ValidationIssue issue = findFirst(issues, "function.arg.type.unknown");
    assertThat(issue).isNotNull();
    assertThat(issue.ctx()).isEqualTo("function:f.argTypes[0]");
    assertThat(issue.args()).containsExactly("missing");
  }

  @Test
  void validate_functionOverloadsAllowed() {
    SystemFunctionDef intFn =
        new SystemFunctionDef(
            name("f"), List.of(name("int")), name("int"), false, false, List.of());
    SystemFunctionDef textFn =
        new SystemFunctionDef(
            name("f"), List.of(name("text")), name("text"), false, false, List.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(intFn, textFn),
            List.of(),
            List.of(type("int"), type("text")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(issues).isEmpty();
  }

  @Test
  void validate_functionDuplicateSignature() {
    SystemFunctionDef fn =
        new SystemFunctionDef(
            name("f"), List.of(name("int")), name("int"), false, false, List.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(fn, fn),
            List.of(),
            List.of(type("int")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("function.duplicate");

    ValidationIssue dup = findFirst(issues, "function.duplicate");
    assertThat(dup).isNotNull();
    assertThat(dup.ctx()).isEqualTo("function:f");
    assertThat(dup.args()).containsExactly("f(int)");

    // functions that only differ by return type still conflict
    var fIntReturn =
        new SystemFunctionDef(
            name("f"), List.of(name("int")), name("int"), false, false, List.of());
    var fTextReturn =
        new SystemFunctionDef(
            name("f"), List.of(name("int")), name("text"), false, false, List.of());

    catalog =
        new SystemCatalogData(
            List.of(fIntReturn, fTextReturn),
            List.of(),
            List.of(type("int"), type("text")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("function.duplicate");
    dup = findFirst(issues, "function.duplicate");
    assertThat(dup).isNotNull();
    assertThat(dup.args()).containsExactly("f(int)");
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
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("operator.right.type.unknown");

    ValidationIssue issue = findFirst(issues, "operator.right.type.unknown");
    assertThat(issue).isNotNull();
    assertThat(issue.ctx()).isEqualTo("operator:+.rightType");
    assertThat(issue.args()).containsExactly("missing");
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
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("cast.name.duplicate");

    ValidationIssue dup = findFirst(issues, "cast.name.duplicate");
    assertThat(dup).isNotNull();
    assertThat(dup.ctx()).isEqualTo("cast:c");
    assertThat(dup.args()).containsExactly("c");
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
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("cast.source.type.unknown");

    ValidationIssue issue = findFirst(issues, "cast.source.type.unknown");
    assertThat(issue).isNotNull();
    assertThat(issue.ctx()).isEqualTo("cast:c.sourceType");
    assertThat(issue.args()).containsExactly("missing");
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
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("collation.duplicate");

    ValidationIssue dup = findFirst(issues, "collation.duplicate");
    assertThat(dup).isNotNull();
    assertThat(dup.ctx()).isEqualTo("collation:c");
    assertThat(dup.args()).containsExactly("c:en_US");
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
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("agg.arg.type.unknown");

    ValidationIssue issue = findFirst(issues, "agg.arg.type.unknown");
    assertThat(issue).isNotNull();
    assertThat(issue.ctx()).isEqualTo("aggregate:sum.argTypes[0]");
    assertThat(issue.args()).containsExactly("missing");
  }

  @Test
  void validate_engineSpecificRulesRequirePayloadType() {
    EngineSpecificRule rule = new EngineSpecificRule("pg", "", "", "", new byte[0], Map.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    name("f"), List.of(), name("int"), false, false, List.of(rule))),
            List.of(),
            List.of(type("int")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("engine_specific.payload_type.required");

    ValidationIssue issue = findFirst(issues, "engine_specific.payload_type.required");
    assertThat(issue).isNotNull();
    assertThat(issue.ctx()).isEqualTo("function:f.engineSpecific[0]");
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
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(issues).isEmpty();
  }
}
