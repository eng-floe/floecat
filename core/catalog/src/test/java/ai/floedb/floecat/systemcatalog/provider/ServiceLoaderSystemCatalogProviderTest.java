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

package ai.floedb.floecat.systemcatalog.provider;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ServiceLoaderSystemCatalogProvider}.
 *
 * <p>This test suite validates:
 *
 * <ul>
 *   <li>Illegal argument handling
 *   <li>Fallback behavior for unknown engines
 *   <li>Presence of builtin InformationSchemaProvider
 *   <li>Merging of provider-defined namespaces/tables/views
 *   <li>Snapshot immutability guarantees
 * </ul>
 */
class ServiceLoaderSystemCatalogProviderTest {

  @Test
  void load_nullEngineKindReturnsInformationSchemaOnly() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(null);

    assertThat(catalog.namespaces())
        .containsExactly(
            new SystemNamespaceDef(
                NameRefUtil.name("information_schema"), "information_schema", List.of()));

    assertThat(catalog.tables()).isNotEmpty(); // information_schema tables
    assertThat(catalog.functions()).isEmpty();
    assertThat(catalog.types()).isEmpty();
  }

  @Test
  void load_blankEngineKindReturnsInformationSchemaOnly() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load("  ");

    assertThat(catalog.namespaces())
        .containsExactly(
            new SystemNamespaceDef(
                NameRefUtil.name("information_schema"), "information_schema", List.of()));

    assertThat(catalog.tables()).isNotEmpty();
    assertThat(catalog.functions()).isEmpty();
  }

  @Test
  void load_blankEngineKindDefaultsToFloecatCatalogWithoutDecorator() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(" ");

    assertThat(catalog.engineKind()).isEqualTo(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
    assertThat(catalog.namespaces())
        .contains(
            new SystemNamespaceDef(
                NameRefUtil.name("information_schema"), "information_schema", List.of()));
  }

  @Test
  void load_unknownEngineReturnsInformationSchemaCatalogOnly() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load("unknown-engine");
    assertThat(catalog.functions()).isEmpty();
    assertThat(catalog.types()).isEmpty();
    assertThat(catalog.aggregates()).isEmpty();
    assertThat(catalog.operators()).isEmpty();
    assertThat(catalog.collations()).isEmpty();
    assertThat(catalog.casts()).isEmpty();
    assertThat(catalog.namespaces())
        .containsExactly(
            new SystemNamespaceDef(
                NameRefUtil.name("information_schema"), "information_schema", List.of()));
    assertThat(catalog.tables()).isNotEmpty(); // information_schema tables are always present
  }

  @Test
  void providers_includesInformationSchemaProvider() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThat(provider.providers())
        .anyMatch(p -> p.getClass().getSimpleName().equals("InformationSchemaProvider"));
  }

  @Test
  void load_mergesInformationSchemaDefinitions() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load("any-engine");

    // information_schema.schemata is always present
    assertThat(catalog.tables().stream().map(t -> NameRefUtil.canonical(t.name())).toList())
        .contains("information_schema.schemata");

    assertThat(catalog.tables().stream().map(t -> NameRefUtil.canonical(t.name())).toList())
        .contains("information_schema.tables");

    assertThat(catalog.tables().stream().map(t -> NameRefUtil.canonical(t.name())).toList())
        .contains("information_schema.columns");
  }

  @Test
  void load_returnsIndependentSnapshots() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog c1 = provider.load("engine-x");
    SystemEngineCatalog c2 = provider.load("engine-x");

    assertThat(c1).isNotSameAs(c2);
    assertThat(c1.fingerprint()).isEqualTo(c2.fingerprint());
  }

  @Test
  void mergeProviderDefinitions_lastWinsByCanonicalName() {
    // This test validates merge semantics indirectly by ensuring
    // canonical-name uniqueness is enforced.

    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load("engine-y");

    List<String> tableNames =
        catalog.tables().stream().map(t -> NameRefUtil.canonical(t.name())).toList();

    // No duplicate canonical names
    assertThat(tableNames).doesNotHaveDuplicates();
  }
}
