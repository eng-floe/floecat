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
import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import ai.floedb.floecat.systemcatalog.def.*;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
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

  private static EngineSpecificRule rule(String engine) {
    return new EngineSpecificRule(
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

    // Assert EngineSpecificRule fields individually
    EngineSpecificRule inputRule = input.functions().get(0).engineSpecific().get(0);
    EngineSpecificRule outputRule = output.functions().get(0).engineSpecific().get(0);

    assertThat(outputRule.engineKind()).isEqualTo(inputRule.engineKind());
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
    EngineSpecificRule ruleWithoutEngine =
        new EngineSpecificRule("", "1.0", "2.0", "", new byte[0], Map.of());

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
            List.of());

    SystemObjectsRegistry proto = SystemCatalogProtoMapper.toProto(input);
    SystemCatalogData output = SystemCatalogProtoMapper.fromProto(proto, "postgres");

    EngineSpecificRule restored = output.functions().get(0).engineSpecific().get(0);

    assertThat(restored.engineKind()).isEqualTo("postgres");
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
  void roundTrip_emptyEngineSpecificIsPreserved() {
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
            List.of());

    SystemObjectsRegistry proto = SystemCatalogProtoMapper.toProto(input);
    SystemCatalogData output = SystemCatalogProtoMapper.fromProto(proto);

    assertThat(output.functions().get(0).engineSpecific()).isEmpty();
  }
}
