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

import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ServiceLoaderSystemCatalogProvider}.
 *
 * <p>Because the provider serves only the raw engine catalog (without the floecat_internal merge),
 * {@link SystemNodeRegistry} is responsible for seeding {@code information_schema}. These tests
 * focus on the loader semantics:
 *
 * <ul>
 *   <li>Fallback behavior for incomplete engine contexts
 *   <li>Presence of the floecat_internal base provider
 *   <li>Snapshot immutability guarantees
 * </ul>
 */
class ServiceLoaderSystemCatalogProviderTest {

  @Test
  void load_nullEngineKindReturnsFloecatInternalCatalog() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(EngineContext.of(null, null));

    assertThat(catalog.engineKind()).isEqualTo(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
    assertThat(catalog.functions()).isEmpty();
    assertThat(catalog.tables()).isEmpty();
  }

  @Test
  void load_blankEngineKindReturnsFloecatInternalCatalog() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(EngineContext.of("   ", null));

    assertThat(catalog.engineKind()).isEqualTo(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
    assertThat(catalog.tables()).isEmpty();
  }

  @Test
  void load_unknownEngineFallsBackToFloecatInternal() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(EngineContext.of("unknown-engine", ""));
    assertThat(catalog.engineKind()).isEqualTo(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
    assertThat(catalog.tables()).isEmpty();
  }

  @Test
  void load_withoutHeadersStillProvidesFloecatInternal() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(EngineContext.empty());

    assertThat(catalog.engineKind()).isEqualTo(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
    assertThat(catalog.tables()).isEmpty();
  }

  @Test
  void providers_includesFloecatInternalProvider() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThat(provider.providers()).anyMatch(p -> p instanceof FloecatInternalProvider);
  }

  @Test
  void load_returnsIndependentSnapshots() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog c1 = provider.load(EngineContext.of("engine-x", ""));
    SystemEngineCatalog c2 = provider.load(EngineContext.of("engine-x", ""));

    assertThat(c1).isNotSameAs(c2);
    assertThat(c1.fingerprint()).isEqualTo(c2.fingerprint());
  }
}
