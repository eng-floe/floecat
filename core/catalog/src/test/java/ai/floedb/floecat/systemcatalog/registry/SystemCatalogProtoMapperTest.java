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

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.systemcatalog.def.*;
import ai.floedb.floecat.systemcatalog.engine.ScopedMetadataRule;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class SystemCatalogProtoMapperTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static NameRef name(String n) {
    return NameRef.newBuilder().setName(n).build();
  }

  private static ScopedMetadataRule rule(String engine) {
    return new ScopedMetadataRule(
        engine, "1.0.0", "9.9.9", "payload/type", new byte[] {1, 2, 3}, Map.of("k", "v"));
  }

  // ---------------------------------------------------------------------------
  // Round-trip test
  // ---------------------------------------------------------------------------

  @Test
  void roundTrip_preservesAllDefinitions() {
    SystemCatalogData input =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    name("f"),
                    List.of(name("int")),
                    name("int"),
                    false,
                    false,
                    List.of(rule("spark")))),
            List.of(
                new SystemOperatorDef(
                    name("+"), name("int"), name("int"), name("int"), true, true, List.of())),
            List.of(
                new SystemTypeDef(name("int"), "scalar", false, null, List.of()),
                new SystemTypeDef(name("int_array"), "array", true, name("int"), List.of())),
            List.of(
                new SystemCastDef(
                    name("cast"), name("int"), name("int"), SystemCastMethod.IMPLICIT, List.of())),
            List.of(new SystemCollationDef(name("en_US"), "en_US", List.of())),
            List.of(
                new SystemAggregateDef(
                    name("sum"), List.of(name("int")), name("int"), name("int"), List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    SystemObjectsRegistry proto = SystemCatalogProtoMapper.toProto(input);
    SystemCatalogData output = SystemCatalogProtoMapper.fromProto(proto, "spark");

    // Assert sizes and names for top-level lists
    assertThat(output.functions()).hasSize(input.functions().size());
    assertThat(output.operators()).hasSize(input.operators().size());
    assertThat(output.types()).hasSize(input.types().size());
    assertThat(output.casts()).hasSize(input.casts().size());
    assertThat(output.collations()).hasSize(input.collations().size());
    assertThat(output.aggregates()).hasSize(input.aggregates().size());

    // Assert function names
    assertThat(output.functions().get(0).name()).isEqualTo(input.functions().get(0).name());

    // Assert ScopedMetadataRule fields individually
    ScopedMetadataRule inputRule = input.functions().get(0).scopedMetadata().get(0);
    ScopedMetadataRule outputRule = output.functions().get(0).scopedMetadata().get(0);

    assertThat(outputRule.kind()).isEqualTo(inputRule.kind());
    assertThat(outputRule.minVersion()).isEqualTo(inputRule.minVersion());
    assertThat(outputRule.maxVersion()).isEqualTo(inputRule.maxVersion());
    assertThat(outputRule.payloadType()).isEqualTo(inputRule.payloadType());
    assertThat(outputRule.properties()).isEqualTo(inputRule.properties());
    assertThat(Arrays.equals(outputRule.extensionPayload(), inputRule.extensionPayload())).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Default engine fallback
  // ---------------------------------------------------------------------------

  @Test
  void fromProto_appliesDefaultEngineWhenMissing() {
    ScopedMetadataRule ruleWithoutEngine =
        new ScopedMetadataRule("", "1.0", "2.0", "payload/type", new byte[0], Map.of());

    SystemCatalogData input =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    name("f"), List.of(), name("int"), false, false, List.of(ruleWithoutEngine))),
            List.of(),
            List.of(new SystemTypeDef(name("int"), "scalar", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    SystemObjectsRegistry proto = SystemCatalogProtoMapper.toProto(input);
    SystemCatalogData output = SystemCatalogProtoMapper.fromProto(proto, "postgres");

    ScopedMetadataRule restored = output.functions().get(0).scopedMetadata().get(0);

    assertThat(restored.kind()).isEqualTo("postgres");
  }

  @Test
  void fromProto_doesNotApplyEngineDefaultToEnvironmentRule() {
    var environmentRule =
        ai.floedb.floecat.query.rpc.ScopedMetadataRule.newBuilder()
            .setScope(ai.floedb.floecat.query.rpc.ScopedMetadataRule.Scope.ENVIRONMENT)
            .setMinVersion("1")
            .setMaxVersion("2")
            .setPayloadType("environment.pg_class")
            .build();
    var proto = SystemObjectsRegistry.newBuilder().addScopedMetadata(environmentRule).build();

    var output = SystemCatalogProtoMapper.fromProto(proto, "postgres");

    assertThat(output.registryScopedMetadata())
        .singleElement()
        .satisfies(
            rule -> {
              assertThat(rule.scope()).isEqualTo(ScopedMetadataRule.Scope.ENVIRONMENT);
              assertThat(rule.kind()).isEmpty();
            });
  }

  // ---------------------------------------------------------------------------
  // Array type handling
  // ---------------------------------------------------------------------------

  @Test
  void roundTrip_preservesArrayElementType() {
    SystemCatalogData input =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(
                new SystemTypeDef(name("int"), "scalar", false, null, List.of()),
                new SystemTypeDef(name("int_array"), "array", true, name("int"), List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    SystemObjectsRegistry proto = SystemCatalogProtoMapper.toProto(input);
    SystemCatalogData output = SystemCatalogProtoMapper.fromProto(proto);

    SystemTypeDef arrayType =
        output.types().stream().filter(t -> t.array()).findFirst().orElseThrow();

    assertThat(arrayType.elementType()).isEqualTo(name("int"));
  }

  // ---------------------------------------------------------------------------
  // Empty engine-specific list
  // ---------------------------------------------------------------------------

  @Test
  void roundTrip_emptyScopedMetadataIsPreserved() {
    SystemCatalogData input =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(name("f"), List.of(), name("int"), false, false, List.of())),
            List.of(),
            List.of(new SystemTypeDef(name("int"), "scalar", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    SystemObjectsRegistry proto = SystemCatalogProtoMapper.toProto(input);
    SystemCatalogData output = SystemCatalogProtoMapper.fromProto(proto);

    assertThat(output.functions().get(0).scopedMetadata()).isEmpty();
  }

  @Test
  void roundTrip_preservesSystemTableConstraints() {
    ConstraintDefinition constraint =
        ConstraintDefinition.newBuilder()
            .setName("pk_tables")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .addColumns(
                ConstraintColumnRef.newBuilder()
                    .setColumnName("table_name")
                    .setColumnId(1L)
                    .setOrdinal(1)
                    .build())
            .build();
    SystemCatalogData input =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new SystemTableDef(
                    NameRef.newBuilder().addPath("information_schema").setName("tables").build(),
                    "tables",
                    List.of(
                        new SystemColumnDef(
                            "table_name", name("VARCHAR"), false, 1, 1L, List.of())),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "tables_scanner",
                    "",
                    "",
                    List.of(),
                    null,
                    List.of(constraint))),
            List.of(),
            List.of());

    SystemObjectsRegistry proto = SystemCatalogProtoMapper.toProto(input);
    SystemCatalogData output = SystemCatalogProtoMapper.fromProto(proto, "floedb");

    assertThat(output.tables()).hasSize(1);
    assertThat(output.tables().get(0).constraints()).hasSize(1);
    assertThat(output.tables().get(0).constraints().get(0).toByteString())
        .isEqualTo(constraint.toByteString());
  }

  @Test
  void roundTrip_preservesRegistryScopedMetadata() {
    ScopedMetadataRule registryRule = rule("spark");

    SystemCatalogData input =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(registryRule));

    SystemObjectsRegistry proto = SystemCatalogProtoMapper.toProto(input);
    SystemCatalogData output = SystemCatalogProtoMapper.fromProto(proto, "spark");

    assertThat(output.registryScopedMetadata()).hasSize(1);
    ScopedMetadataRule restored = output.registryScopedMetadata().get(0);
    assertThat(restored.kind()).isEqualTo(registryRule.kind());
    assertThat(restored.scope()).isEqualTo(registryRule.scope());
    assertThat(restored.payloadType()).isEqualTo(registryRule.payloadType());
    assertThat(restored.minVersion()).isEqualTo(registryRule.minVersion());
    assertThat(restored.maxVersion()).isEqualTo(registryRule.maxVersion());
    assertThat(restored.properties()).isEqualTo(registryRule.properties());
    assertThat(Arrays.equals(restored.extensionPayload(), registryRule.extensionPayload()))
        .isTrue();
  }
}
