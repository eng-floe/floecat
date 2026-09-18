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

package ai.floedb.floecat.systemcatalog.spi;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import org.junit.jupiter.api.Test;

final class EngineCatalogProviderTest {

  @Test
  void dynamicProviderDoesNotRequireStaticDefinitions() {
    EngineCatalogProvider provider = new DuckProvider();

    assertThat(provider.definitions()).isEmpty();
    assertThat(provider.supportsEngine(" DuckDB ")).isTrue();
    assertThat(provider.supportsEngine("floedb")).isFalse();
  }

  @Test
  void providerCanSupplyVersionAwareTypeMapper() {
    EngineCatalogProvider provider = new DuckProvider();
    EngineContext engine = EngineContext.of("duckdb", "1.2");

    assertThat(provider.typeMapper(engine)).isSameAs(DuckProvider.MAPPER);
  }

  private static final class DuckProvider implements EngineCatalogProvider {
    private static final EngineTypeMapper MAPPER =
        (logicalType, lookup) -> java.util.Optional.empty();

    @Override
    public String engineKind() {
      return "duckdb";
    }

    @Override
    public EngineTypeMapper typeMapper(EngineContext engine) {
      return MAPPER;
    }

    @Override
    public boolean supports(ai.floedb.floecat.common.rpc.NameRef name, String engineKind) {
      return supportsEngine(engineKind);
    }

    @Override
    public java.util.Optional<ai.floedb.floecat.scanner.spi.SystemObjectScanner> provide(
        String scannerId, String engineKind, String engineVersion) {
      return java.util.Optional.empty();
    }
  }
}
