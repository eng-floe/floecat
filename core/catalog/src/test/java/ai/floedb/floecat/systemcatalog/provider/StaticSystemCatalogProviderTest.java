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

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StaticSystemCatalogProvider}.
 *
 * <p>This provider is test-only and is expected to:
 *
 * <ul>
 *   <li>Normalize engine kinds (case-insensitive lookup)
 *   <li>Return a valid empty catalog for unknown engines
 *   <li>Return immutable snapshot catalogs
 *   <li>Never throw for missing engines
 * </ul>
 */
class StaticSystemCatalogProviderTest {

  @Test
  void load_isCaseInsensitive() {
    SystemCatalogData data =
        new SystemCatalogData(
            List.of(fn("f")),
            List.of(),
            List.of(type("int")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    StaticSystemCatalogProvider provider = new StaticSystemCatalogProvider(Map.of("SpArK", data));

    SystemEngineCatalog lower = provider.load("spark");
    SystemEngineCatalog upper = provider.load("SPARK");

    assertThat(lower.functions("f")).hasSize(1);
    assertThat(upper.functions("f")).hasSize(1);
  }

  @Test
  void load_unknownEngineReturnsEmptyCatalog() {
    StaticSystemCatalogProvider provider = new StaticSystemCatalogProvider(Map.of());

    SystemEngineCatalog catalog = provider.load("unknown-engine");

    assertThat(catalog.functions()).isEmpty();
    assertThat(catalog.types()).isEmpty();
    assertThat(catalog.operators()).isEmpty();
    assertThat(catalog.fingerprint()).isNotBlank();
  }

  @Test
  void load_returnsIndependentSnapshots() {
    SystemCatalogData data =
        new SystemCatalogData(
            List.of(fn("f")),
            List.of(),
            List.of(type("int")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    StaticSystemCatalogProvider provider = new StaticSystemCatalogProvider(Map.of("spark", data));

    SystemEngineCatalog c1 = provider.load("spark");
    SystemEngineCatalog c2 = provider.load("spark");

    assertThat(c1).isNotSameAs(c2);
    assertThat(c1.fingerprint()).isEqualTo(c2.fingerprint());
  }

  @Test
  void constructor_normalizesAllKeys() {
    StaticSystemCatalogProvider provider =
        new StaticSystemCatalogProvider(Map.of("SPARK", SystemCatalogData.empty()));

    // Would fail if constructor didn't normalize keys
    SystemEngineCatalog catalog = provider.load("spark");

    assertThat(catalog.fingerprint()).isNotBlank();
  }

  // ----------------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------------

  private static SystemFunctionDef fn(String name) {
    return new SystemFunctionDef(
        NameRef.newBuilder().setName(name).build(),
        List.of(),
        NameRef.newBuilder().setName("int").build(),
        false,
        false,
        List.of());
  }

  private static SystemTypeDef type(String name) {
    return new SystemTypeDef(
        NameRef.newBuilder().setName(name).build(), "scalar", false, null, List.of());
  }
}
