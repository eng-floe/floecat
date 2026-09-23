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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.scanner.utils.EnvironmentContext;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ServiceLoaderSystemCatalogProvider}.
 *
 * <p>The loader resolves only the explicitly selected engine catalog. These tests focus on the
 * selection semantics:
 *
 * <ul>
 *   <li>Blank selections are not implicit internal selections
 *   <li>Unknown selections fail instead of falling back
 *   <li>Explicit selection of the floecat_internal provider
 *   <li>Snapshot immutability guarantees
 * </ul>
 */
class ServiceLoaderSystemCatalogProviderTest {

  @Test
  void load_nullEngineKindReturnsEmptyCatalog() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(context(EngineContext.of(null, null)));

    assertThat(catalog.engineKind()).isEmpty();
    assertThat(catalog.functions()).isEmpty();
    assertThat(catalog.tables()).isEmpty();
  }

  @Test
  void load_blankEngineKindReturnsEmptyCatalog() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(context(EngineContext.of("   ", null)));

    assertThat(catalog.engineKind()).isEmpty();
    assertThat(catalog.tables()).isEmpty();
  }

  @Test
  void load_unknownHeaderFailsWithoutFallback() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThatThrownBy(() -> provider.load(context(EngineContext.of("unknown-engine", ""))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown engine kind");
  }

  @Test
  void load_unknownEnvironmentFailsWithoutFallback() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    CatalogContext context =
        CatalogContext.of(
            EnvironmentContext.of("unknown-environment", ""), EngineContext.of("test-engine", ""));

    assertThatThrownBy(() -> provider.load(context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown catalog environment kind");
  }

  @Test
  void explicitInternalSelectionProvidesFloecatInternal() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog =
        provider.load(context(EngineContext.of(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, "")));

    assertThat(catalog.engineKind()).isEqualTo(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
    assertInfoSchemaTablesPresent(catalog);
  }

  @Test
  void internalSelectionKeepsItsIdentityUnderAnEnvironment() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog =
        provider.load(
            CatalogContext.of(
                EnvironmentContext.of("test-env", ""),
                EngineContext.of(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, "")));

    // Ownership is classified from this identity. Stamping the environment onto the internal
    // catalog makes it read as engine-owned, which rejects its own FLOECAT-backed tables.
    assertThat(catalog.engineKind()).isEqualTo(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
    assertInfoSchemaTablesPresent(catalog);
  }

  @Test
  void internalProvider_isFloecatInternal() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThat(provider.internalProvider()).isInstanceOf(FloecatInternalProvider.class);
  }

  @Test
  void liveEngineProvider_isDiscoveredSeparatelyFromStaticExtensions() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThat(provider.engineKinds()).contains("test-engine");
    assertThat(provider.providerFor(" Test-Engine "))
        .containsInstanceOf(DynamicTestEngineCatalogProvider.class);
    assertThat(provider.providers()).anyMatch(DynamicTestEngineCatalogProvider.class::isInstance);
  }

  @Test
  void environmentProviders_areDiscoveredSeparatelyFromEngineProviders() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThat(provider.environmentProviders())
        .anyMatch(environment -> environment.environmentKind().equals("test-env"));
  }

  @Test
  void load_returnsIndependentSnapshots() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog c1 = provider.load(context(EngineContext.of("test-engine", "")));
    SystemEngineCatalog c2 = provider.load(context(EngineContext.of("test-engine", "")));

    assertThat(c1).isNotSameAs(c2);
    assertThat(c1.fingerprint()).isEqualTo(c2.fingerprint());
  }

  @Test
  void load_invalidExtensionCatalog_throwsValidationError() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThatThrownBy(
            () -> provider.load(context(EngineContext.of(InvalidCatalogExtension.ENGINE_KIND, ""))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("System catalog validation failed");
  }

  private static void assertInfoSchemaTablesPresent(SystemEngineCatalog catalog) {
    assertThat(catalog.tables()).isNotEmpty();
    assertThat(catalog.tables())
        .extracting(def -> NameRefUtil.identityKey(def.name()))
        .contains(
            "information_schema.tables",
            "information_schema.columns",
            "information_schema.schemata");
  }

  private static CatalogContext context(EngineContext engine) {
    return CatalogContext.of(EnvironmentContext.empty(), engine);
  }
}
