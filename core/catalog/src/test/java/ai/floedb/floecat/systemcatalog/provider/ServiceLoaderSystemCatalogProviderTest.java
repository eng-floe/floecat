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

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Optional;
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
    assertInfoSchemaTablesPresent(catalog);
  }

  @Test
  void load_blankEngineKindReturnsFloecatInternalCatalog() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(EngineContext.of("   ", null));

    assertThat(catalog.engineKind()).isEqualTo(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
    assertInfoSchemaTablesPresent(catalog);
  }

  @Test
  void load_unknownHeaderReturnsFloecatInternalContentUnderUnknownHeader() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(EngineContext.of("unknown-engine", ""));
    assertThat(catalog.engineKind()).isEqualTo("unknown-engine");
    assertInfoSchemaTablesPresent(catalog);
  }

  @Test
  void load_withoutHeadersStillProvidesFloecatInternal() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load(EngineContext.empty());

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

    assertThat(provider.engineKinds()).contains("duckdb");
    assertThat(provider.engineProviderFor(" DuckDB "))
        .containsInstanceOf(DynamicDuckCatalogProvider.class);
    assertThat(provider.providers()).anyMatch(DynamicDuckCatalogProvider.class::isInstance);
  }

  @Test
  void load_returnsIndependentSnapshots() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog c1 = provider.load(EngineContext.of("engine-x", ""));
    SystemEngineCatalog c2 = provider.load(EngineContext.of("engine-x", ""));

    assertThat(c1).isNotSameAs(c2);
    assertThat(c1.fingerprint()).isEqualTo(c2.fingerprint());
  }

  @Test
  void load_invalidExtensionCatalog_throwsValidationError() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThatThrownBy(
            () -> provider.load(EngineContext.of(InvalidCatalogExtension.ENGINE_KIND, "")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("System catalog validation failed");
  }

  private static void assertInfoSchemaTablesPresent(SystemEngineCatalog catalog) {
    assertThat(catalog.tables()).isNotEmpty();
    assertThat(catalog.tables())
        .extracting(def -> NameRefUtil.canonical(def.name()))
        .contains(
            "information_schema.tables",
            "information_schema.columns",
            "information_schema.schemata");
  }

  @Test
  void expectsDecoration_dependsOnRegistrationAndDecorator() {
    ServiceLoaderSystemCatalogProvider provider =
        new ServiceLoaderSystemCatalogProvider(
            List.of(
                new FakeExtension("floedb", new EngineMetadataDecorator() {}),
                new FakeExtension("duckdb", null)));

    // No engine headers at all: there is no engine to decorate for.
    assertThat(provider.expectsDecoration(EngineContext.empty())).isFalse();
    // Registered with a decorator: decorated, as before this method existed.
    assertThat(provider.expectsDecoration(EngineContext.of("floedb", "1"))).isTrue();
    // Registered without one: wants no decoration, and is served as-is.
    assertThat(provider.expectsDecoration(EngineContext.of("duckdb", "1"))).isFalse();
    // Registered for nothing: a misconfigured or misspelled kind still fails closed.
    assertThat(provider.expectsDecoration(EngineContext.of("not-registered", "1"))).isTrue();
  }

  /** Registers an engine kind, with a decorator or deliberately without one. */
  private static final class FakeExtension implements EngineSystemCatalogExtension {
    private final String kind;
    private final EngineMetadataDecorator decorator;

    FakeExtension(String kind, EngineMetadataDecorator decorator) {
      this.kind = kind;
      this.decorator = decorator;
    }

    @Override
    public String engineKind() {
      return kind;
    }

    @Override
    public SystemCatalogData loadSystemCatalog() {
      return SystemCatalogData.empty();
    }

    @Override
    public Optional<EngineMetadataDecorator> decorator() {
      return Optional.ofNullable(decorator);
    }

    @Override
    public List<SystemObjectDef> definitions() {
      return List.of();
    }

    @Override
    public boolean supportsEngine(String engineKind) {
      return kind.equals(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind) {
      return false;
    }

    @Override
    public Optional<SystemObjectScanner> provide(
        String scannerId, String engineKind, String engineVersion) {
      return Optional.empty();
    }
  }
}
