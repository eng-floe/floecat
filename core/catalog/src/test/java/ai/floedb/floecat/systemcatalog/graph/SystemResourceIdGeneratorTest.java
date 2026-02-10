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
package ai.floedb.floecat.systemcatalog.graph;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

final class SystemResourceIdGeneratorTest {

  @Test
  void generatedIdsCarrySystemMarker() {
    ResourceId id =
        SystemNodeRegistry.resourceId("floedb", ResourceKind.RK_TABLE, "pg_catalog.pg_class");
    UUID uuid = UUID.fromString(id.getId());
    assertThat(SystemResourceIdGenerator.isSystemId(uuid)).isTrue();
    byte[] bytes = SystemResourceIdGenerator.bytesFromUuid(uuid);
    assertThat(bytes[0]).isEqualTo((byte) 0xAB);
    assertThat(bytes[1]).isEqualTo((byte) 0xCD);
  }

  @Test
  void randomUuidIsNotMarked() {
    UUID random = UUID.fromString("00000000-0000-0000-0000-000000000000");
    assertThat(SystemResourceIdGenerator.isSystemId(random)).isFalse();
  }

  @Test
  void baseRespectsRfc4122Bits() {
    byte[] base = SystemResourceIdGenerator.base(ResourceKind.RK_TABLE, "pg_catalog.pg_class");
    int version = (base[6] >> 4) & 0x0F;
    int variant = (base[8] & 0xC0) >> 6;
    assertThat(version).isEqualTo(4);
    assertThat(variant).isEqualTo(2);
  }

  @Test
  void maskedUuidRetainsMarkerAndRfcBits() {
    ResourceId id =
        SystemNodeRegistry.resourceId("floedb", ResourceKind.RK_TABLE, "pg_catalog.pg_class");
    byte[] masked = SystemResourceIdGenerator.bytesFromResourceId(id.getId());
    assertThat(SystemResourceIdGenerator.isSystemId(UUID.fromString(id.getId()))).isTrue();
    int version = (masked[6] >> 4) & 0x0F;
    int variant = (masked[8] & 0xC0) >> 6;
    assertThat(version).isEqualTo(4);
    assertThat(variant).isEqualTo(2);
  }

  @Test
  void translatedDefaultIdMatchesDirectBuilder() {
    String canonical = "pg_catalog.pg_class";
    ResourceId engineId = SystemNodeRegistry.resourceId("floedb", ResourceKind.RK_TABLE, canonical);
    UUID translated = translateToDefault(engineId, "floedb");
    ResourceId expectedDefault =
        SystemNodeRegistry.resourceId("", ResourceKind.RK_TABLE, canonical);
    assertThat(translated).isEqualTo(UUID.fromString(expectedDefault.getId()));
  }

  @Test
  void fallbackRewritesToDefaultOne() {
    ResourceId engineId =
        SystemNodeRegistry.resourceId("floedb", ResourceKind.RK_TABLE, "pg_catalog.pg_class");

    byte[] base =
        SystemResourceIdGenerator.xor(
            SystemResourceIdGenerator.bytesFromResourceId(engineId.getId()),
            SystemResourceIdGenerator.mask("floedb"));
    byte[] defaultBytes =
        SystemResourceIdGenerator.xor(
            base, SystemResourceIdGenerator.mask(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG));
    UUID translated = SystemResourceIdGenerator.uuidFromBytes(defaultBytes);
    ResourceId fallback =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_TABLE)
            .setId(translated.toString())
            .build();

    ResourceId expectedDefault =
        SystemNodeRegistry.resourceId("", ResourceKind.RK_TABLE, "pg_catalog.pg_class");

    assertThat(fallback.getId()).isEqualTo(expectedDefault.getId());
  }

  @Test
  void sameSignatureDifferentEnginesProduceDifferentIds() {
    ResourceId idA =
        SystemNodeRegistry.resourceId("floedb", ResourceKind.RK_VIEW, "pg_catalog.pg_type");
    ResourceId idB =
        SystemNodeRegistry.resourceId("floe-demo", ResourceKind.RK_VIEW, "pg_catalog.pg_type");

    assertThat(idA.getId()).isNotEqualTo(idB.getId());
  }

  @Test
  void maskUniquenessGuard() {
    List<String> engines =
        List.of("floedb", "floe-demo", EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
    long uniqueCount =
        engines.stream()
            .map(SystemResourceIdGenerator::mask)
            .map(SystemResourceIdGenerator::uuidFromBytes)
            .distinct()
            .count();

    assertThat(uniqueCount).isEqualTo(engines.size());
  }

  private static UUID translateToDefault(ResourceId engineId, String engineKind) {
    byte[] incoming = SystemResourceIdGenerator.bytesFromResourceId(engineId.getId());
    byte[] base =
        SystemResourceIdGenerator.xor(incoming, SystemResourceIdGenerator.mask(engineKind));
    byte[] fallbackBytes =
        SystemResourceIdGenerator.xor(
            base, SystemResourceIdGenerator.mask(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG));
    return SystemResourceIdGenerator.uuidFromBytes(fallbackBytes);
  }
}
