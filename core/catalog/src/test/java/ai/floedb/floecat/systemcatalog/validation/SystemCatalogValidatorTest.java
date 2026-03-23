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

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.ForeignKeyActionRule;
import ai.floedb.floecat.catalog.rpc.ForeignKeyMatchOption;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.TableBackendKind;
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

  private static NameRef name(String namespace, String n) {
    return NameRef.newBuilder().addPath(namespace).setName(n).build();
  }

  private static SystemNamespaceDef namespace(String n) {
    return new SystemNamespaceDef(name(n), n, List.of());
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
  void validate_emptyFragmentsAreAllowed() {
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
    assertThat(codes(issues)).doesNotContain("types.empty", "functions.empty");
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
            name("pg_catalog", "f"),
            List.of(name("pg_catalog", "int")),
            name("pg_catalog", "int"),
            false,
            false,
            List.of());
    SystemFunctionDef textFn =
        new SystemFunctionDef(
            name("pg_catalog", "f"),
            List.of(name("pg_catalog", "text")),
            name("pg_catalog", "text"),
            false,
            false,
            List.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(intFn, textFn),
            List.of(),
            List.of(
                new SystemTypeDef(name("pg_catalog", "int"), "S", false, null, List.of()),
                new SystemTypeDef(name("pg_catalog", "text"), "S", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("pg_catalog")),
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
  void validate_namespaceScopedObjectsMustBeQualified() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    name("f"), List.of(name("int")), name("int"), false, false, List.of())),
            List.of(
                new SystemOperatorDef(
                    name("op"), name("int"), name("int"), name("int"), true, true, List.of())),
            List.of(type("int")),
            List.of(
                new SystemCastDef(
                    name("c"), name("int"), name("int"), SystemCastMethod.EXPLICIT, List.of())),
            List.of(new SystemCollationDef(name("coll"), "en_US", List.of())),
            List.of(
                new SystemAggregateDef(
                    name("agg"), List.of(name("int")), name("int"), name("int"), List.of())),
            List.of(namespace("pg_catalog")),
            List.of(),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues))
        .contains("function.name.qualified.required", "type.name.qualified.required")
        .doesNotContain(
            "operator.name.qualified.required",
            "cast.name.qualified.required",
            "collation.name.qualified.required",
            "agg.name.qualified.required");
  }

  @Test
  void validate_namespaceScopedObjectsMustReferenceKnownNamespace() {
    NameRef unknownInt = name("unknown_ns", "int");
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    name("unknown_ns", "f"),
                    List.of(unknownInt),
                    unknownInt,
                    false,
                    false,
                    List.of())),
            List.of(
                new SystemOperatorDef(
                    name("unknown_ns", "op"),
                    unknownInt,
                    unknownInt,
                    unknownInt,
                    true,
                    true,
                    List.of())),
            List.of(new SystemTypeDef(unknownInt, "S", false, null, List.of())),
            List.of(
                new SystemCastDef(
                    name("unknown_ns", "c"),
                    unknownInt,
                    unknownInt,
                    SystemCastMethod.EXPLICIT,
                    List.of())),
            List.of(new SystemCollationDef(name("unknown_ns", "coll"), "en_US", List.of())),
            List.of(
                new SystemAggregateDef(
                    name("unknown_ns", "agg"),
                    List.of(unknownInt),
                    unknownInt,
                    unknownInt,
                    List.of())),
            List.of(namespace("pg_catalog")),
            List.of(),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues))
        .contains("function.namespace.unknown", "type.namespace.unknown")
        .doesNotContain(
            "operator.namespace.unknown",
            "cast.namespace.unknown",
            "collation.namespace.unknown",
            "agg.namespace.unknown");
  }

  @Test
  void validate_namespaceScopedObjectsCanBeStrictlyEnforcedByPolicy() {
    NameRef unknownInt = name("unknown_ns", "int");
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    name("unknown_ns", "f"),
                    List.of(unknownInt),
                    unknownInt,
                    false,
                    false,
                    List.of())),
            List.of(
                new SystemOperatorDef(
                    name("unknown_ns", "op"),
                    unknownInt,
                    unknownInt,
                    unknownInt,
                    true,
                    true,
                    List.of())),
            List.of(new SystemTypeDef(unknownInt, "S", false, null, List.of())),
            List.of(
                new SystemCastDef(
                    name("unknown_ns", "c"),
                    unknownInt,
                    unknownInt,
                    SystemCastMethod.EXPLICIT,
                    List.of())),
            List.of(new SystemCollationDef(name("unknown_ns", "coll"), "en_US", List.of())),
            List.of(
                new SystemAggregateDef(
                    name("unknown_ns", "agg"),
                    List.of(unknownInt),
                    unknownInt,
                    unknownInt,
                    List.of())),
            List.of(namespace("pg_catalog")),
            List.of(),
            List.of(),
            List.of());

    List<ValidationIssue> issues =
        SystemCatalogValidator.validate(
            catalog, SystemCatalogValidator.NamespaceScopePolicy.strictAll());
    assertThat(codes(issues))
        .contains(
            "function.namespace.unknown",
            "operator.namespace.unknown",
            "type.namespace.unknown",
            "cast.namespace.unknown",
            "collation.namespace.unknown",
            "agg.namespace.unknown");
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

  @Test
  void validate_tableConstraintUnknownColumnRef() {
    ConstraintDefinition badConstraint =
        ConstraintDefinition.newBuilder()
            .setName("pk_tables")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .addColumns(
                ConstraintColumnRef.newBuilder().setColumnName("missing_col").setOrdinal(1).build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(
                        new SystemColumnDef(
                            "table_name", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(badConstraint))),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("table.constraint.column_ref.unknown");
  }

  @Test
  void validate_tableConstraintRejectsDuplicateConstraintName() {
    ConstraintDefinition c1 =
        ConstraintDefinition.newBuilder()
            .setName("dup_name")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .addColumns(
                ConstraintColumnRef.newBuilder().setColumnName("table_name").setOrdinal(1).build())
            .build();
    ConstraintDefinition c2 =
        ConstraintDefinition.newBuilder()
            .setName("dup_name")
            .setType(ConstraintType.CT_UNIQUE)
            .addColumns(
                ConstraintColumnRef.newBuilder().setColumnName("table_name").setOrdinal(1).build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(
                        new SystemColumnDef(
                            "table_name", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(c1, c2))),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("table.constraint.name.duplicate");
  }

  @Test
  void validate_notNullConstraintMustHaveExactlyOneColumn() {
    ConstraintDefinition notNullWithTwoColumns =
        ConstraintDefinition.newBuilder()
            .setName("bad_nn")
            .setType(ConstraintType.CT_NOT_NULL)
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c2").setOrdinal(2).build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(
                        new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of()),
                        new SystemColumnDef("c2", name("VARCHAR"), false, 2, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(notNullWithTwoColumns))),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("table.constraint.not_null.single_column");
  }

  @Test
  void validate_fkConstraintRequiresReferencedTable() {
    ConstraintDefinition fkMissingReferencedTable =
        ConstraintDefinition.newBuilder()
            .setName("fk_tables")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .addReferencedColumns(
                ConstraintColumnRef.newBuilder().setColumnName("x1").setOrdinal(1).build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(fkMissingReferencedTable))),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("table.constraint.fk.referenced_table.required");
  }

  @Test
  void validate_nonFkCannotSetReferencedFields() {
    ConstraintDefinition pkWithReferencedFields =
        ConstraintDefinition.newBuilder()
            .setName("pk_bad")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .setReferencedTable(name("other"))
            .setReferencedConstraintName("pk_other")
            .setMatchOption(ForeignKeyMatchOption.FK_MATCH_OPTION_FULL)
            .setUpdateRule(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE)
            .setDeleteRule(ForeignKeyActionRule.FK_ACTION_RULE_RESTRICT)
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(pkWithReferencedFields))),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues))
        .contains(
            "table.constraint.fk_only.field.not_allowed",
            "table.constraint.fk_only.behavior.not_allowed");
  }

  @Test
  void validate_fkAllowsBehaviorMetadata() {
    ConstraintDefinition fkWithBehavior =
        ConstraintDefinition.newBuilder()
            .setName("fk_ok")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .setReferencedTable(name("information_schema", "ref_table"))
            .setReferencedConstraintName("pk_ref_table")
            .addReferencedColumns(
                ConstraintColumnRef.newBuilder().setColumnName("r1").setOrdinal(1).build())
            .setMatchOption(ForeignKeyMatchOption.FK_MATCH_OPTION_NONE)
            .setUpdateRule(ForeignKeyActionRule.FK_ACTION_RULE_NO_ACTION)
            .setDeleteRule(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE)
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(fkWithBehavior)),
                new SystemTableDef(
                    name("information_schema", "ref_table"),
                    "ref_table",
                    List.of(new SystemColumnDef("r1", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "ref_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of())),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues))
        .doesNotContain(
            "table.constraint.fk_only.field.not_allowed",
            "table.constraint.fk_only.behavior.not_allowed");
  }

  @Test
  void validate_checkRequiresExpressionAndOnlyCheckCanUseExpression() {
    ConstraintDefinition checkWithoutExpression =
        ConstraintDefinition.newBuilder()
            .setName("check_bad")
            .setType(ConstraintType.CT_CHECK)
            .build();
    ConstraintDefinition pkWithCheckExpression =
        ConstraintDefinition.newBuilder()
            .setName("pk_bad_expr")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .setCheckExpression("c1 > 0")
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(checkWithoutExpression, pkWithCheckExpression))),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues))
        .contains(
            "table.constraint.check.expression.required",
            "table.constraint.check_only.expression.not_allowed");
  }

  @Test
  void validate_validCompositePrimaryKeyPasses() {
    ConstraintDefinition compositePk =
        ConstraintDefinition.newBuilder()
            .setName("pk_composite")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c2").setOrdinal(2).build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(
                        new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of()),
                        new SystemColumnDef("c2", name("VARCHAR"), false, 2, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(compositePk))),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(issues).isEmpty();
  }

  @Test
  void validate_validCompositeForeignKeyPasses() {
    ConstraintDefinition compositeFk =
        ConstraintDefinition.newBuilder()
            .setName("fk_composite")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setReferencedTable(name("information_schema", "other_table"))
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c2").setOrdinal(2).build())
            .addReferencedColumns(
                ConstraintColumnRef.newBuilder().setColumnName("r1").setOrdinal(1).build())
            .addReferencedColumns(
                ConstraintColumnRef.newBuilder().setColumnName("r2").setOrdinal(2).build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(
                        new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of()),
                        new SystemColumnDef("c2", name("VARCHAR"), false, 2, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(compositeFk)),
                new SystemTableDef(
                    name("information_schema", "other_table"),
                    "other_table",
                    List.of(
                        new SystemColumnDef("r1", name("VARCHAR"), false, 1, null, List.of()),
                        new SystemColumnDef("r2", name("VARCHAR"), false, 2, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "other_tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of())),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(issues).isEmpty();
  }

  @Test
  void validate_selfReferencingForeignKeyChecksReferencedColumnExistence() {
    ConstraintDefinition selfFkWithUnknownReferencedColumn =
        ConstraintDefinition.newBuilder()
            .setName("fk_self")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setReferencedTable(name("tables"))
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .addReferencedColumns(
                ConstraintColumnRef.newBuilder().setColumnName("missing_ref").setOrdinal(1).build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(selfFkWithUnknownReferencedColumn))),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("table.constraint.column_ref.unknown");
  }

  @Test
  void validate_foreignKeyReferencedColumnsCheckedAgainstReferencedTableWhenKnown() {
    ConstraintDefinition fkWithUnknownReferencedColumn =
        ConstraintDefinition.newBuilder()
            .setName("fk_known_ref")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setReferencedTable(name("information_schema", "ref_table"))
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .addReferencedColumns(
                ConstraintColumnRef.newBuilder().setColumnName("missing_ref").setOrdinal(1).build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(fkWithUnknownReferencedColumn)),
                new SystemTableDef(
                    name("information_schema", "ref_table"),
                    "ref_table",
                    List.of(
                        new SystemColumnDef("ref_id", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "ref_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of())),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("table.constraint.column_ref.unknown");
  }

  @Test
  void validate_foreignKeyReferencedColumnIdNameMismatchIsRejected() {
    ConstraintDefinition fkWithMismatchedReferencedRef =
        ConstraintDefinition.newBuilder()
            .setName("fk_ref_mismatch")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setReferencedTable(name("information_schema", "ref_table"))
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .addReferencedColumns(
                ConstraintColumnRef.newBuilder()
                    .setColumnId(1L)
                    .setColumnName("ref_two")
                    .setOrdinal(1)
                    .build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(fkWithMismatchedReferencedRef)),
                new SystemTableDef(
                    name("information_schema", "ref_table"),
                    "ref_table",
                    List.of(
                        new SystemColumnDef("ref_one", name("VARCHAR"), false, 1, 1L, List.of()),
                        new SystemColumnDef("ref_two", name("VARCHAR"), false, 2, 2L, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "ref_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of())),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("table.constraint.column_ref.id_name.mismatch");
  }

  @Test
  void validate_foreignKeyReferencedTableNameMustResolveWhenNoReferencedTableId() {
    ConstraintDefinition fkUnknownTable =
        ConstraintDefinition.newBuilder()
            .setName("fk_unknown_table")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setReferencedTable(name("information_schema", "missing_table"))
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .addReferencedColumns(
                ConstraintColumnRef.newBuilder().setColumnName("id").setOrdinal(1).build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(new SystemColumnDef("c1", name("VARCHAR"), false, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(fkUnknownTable))),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("table.constraint.fk.referenced_table.unknown");
  }

  @Test
  void validate_notNullConstraintOnNullableColumnIsRejected() {
    ConstraintDefinition explicitNotNull =
        ConstraintDefinition.newBuilder()
            .setName("nn_bad")
            .setType(ConstraintType.CT_NOT_NULL)
            .addColumns(ConstraintColumnRef.newBuilder().setColumnName("c1").setOrdinal(1).build())
            .build();

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("information_schema")),
            List.of(
                new SystemTableDef(
                    name("information_schema", "tables"),
                    "tables",
                    List.of(new SystemColumnDef("c1", name("VARCHAR"), true, 1, null, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(explicitNotNull))),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(codes(issues)).contains("table.constraint.not_null.column_is_nullable");
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
                    name("pg_catalog", "f"),
                    List.of(name("pg_catalog", "int")),
                    name("pg_catalog", "int"),
                    false,
                    false,
                    List.of())),
            List.of(),
            List.of(new SystemTypeDef(name("pg_catalog", "int"), "S", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace("pg_catalog")),
            List.of(),
            List.of(),
            List.of());

    List<ValidationIssue> issues = SystemCatalogValidator.validate(catalog);
    assertThat(issues).isEmpty();
  }
}
