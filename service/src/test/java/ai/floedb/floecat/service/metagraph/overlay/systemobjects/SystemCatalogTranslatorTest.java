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
package ai.floedb.floecat.service.metagraph.overlay.systemobjects;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import org.junit.jupiter.api.Test;

final class SystemCatalogTranslatorTest {

  @Test
  void normalizeSwitchesAccountForSystemIds() {
    ResourceId systemId =
        SystemNodeRegistry.resourceId(
            "floecat_internal", ResourceKind.RK_TABLE, "information_schema.tables");
    ResourceId request = systemId.toBuilder().setAccountId("some-user").build();

    ResourceId normalized = SystemCatalogTranslator.normalizeSystemId(request);

    assertThat(normalized).isNotNull();
    assertThat(normalized.getAccountId()).isEqualTo(SystemNodeRegistry.SYSTEM_ACCOUNT);
    assertThat(normalized.getId()).isEqualTo(systemId.getId());
  }

  @Test
  void normalizeReturnsNullForRandomIds() {
    ResourceId request =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("00000000-0000-0000-0000-000000000000")
            .build();

    assertThat(SystemCatalogTranslator.normalizeSystemId(request)).isNull();
  }

  @Test
  void toSystemNamespaceRefUsesEngineKind() {
    NameRef userRef =
        NameRef.newBuilder()
            .setCatalog("examples")
            .addPath("pg_catalog")
            .setName("information_schema")
            .build();
    EngineContext ctx = EngineContext.of("pg", "1.0");

    NameRef translated = SystemCatalogTranslator.toSystemNamespaceRef(userRef, ctx);

    assertThat(translated.getCatalog()).isEqualTo("pg");
    assertThat(translated.getPathList()).containsExactly("pg_catalog");
    assertThat(translated.getName()).isEqualTo("information_schema");
  }

  @Test
  void toSystemRelationRefUsesEngineKind() {
    NameRef userRef =
        NameRef.newBuilder()
            .setCatalog("examples")
            .addPath("information_schema")
            .setName("tables")
            .build();
    EngineContext ctx = EngineContext.of("pg", "1.0");

    NameRef translated = SystemCatalogTranslator.toSystemRelationRef(userRef, ctx);

    assertThat(translated.getCatalog()).isEqualTo("pg");
    assertThat(translated.getPathList()).containsExactly("information_schema");
    assertThat(translated.getName()).isEqualTo("tables");
  }

  @Test
  void aliasBackToUserCatalogKeepsCatalogName() {
    NameRef input = NameRef.newBuilder().setCatalog("examples").setName("tables").build();
    NameRef system = NameRef.newBuilder().setCatalog("floecat_internal").setName("tables").build();

    NameRef alias = SystemCatalogTranslator.aliasToUserCatalog(input, system);

    assertThat(alias.getCatalog()).isEqualTo("examples");
    assertThat(alias.getName()).isEqualTo(system.getName());
  }
}
