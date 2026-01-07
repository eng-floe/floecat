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

import ai.floedb.floecat.systemcatalog.provider.SystemCatalogProvider;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class SystemDefinitionRegistryTest {

  @Test
  void catalog_lowercasesEngineKind_andCaches() {
    AtomicInteger loadCount = new AtomicInteger();

    SystemCatalogProvider provider =
        new SystemCatalogProvider() {
          @Override
          public SystemEngineCatalog load(String engineKind) {
            loadCount.incrementAndGet();
            return SystemEngineCatalog.empty(engineKind);
          }

          @Override
          public List<String> engineKinds() {
            return List.of("spark");
          }
        };

    SystemDefinitionRegistry registry = new SystemDefinitionRegistry(provider);

    SystemEngineCatalog c1 = registry.catalog("Spark");
    SystemEngineCatalog c2 = registry.catalog("spark");
    SystemEngineCatalog c3 = registry.catalog("SPARK");

    // Same instance due to lowercasing + caching
    assertThat(c1).isSameAs(c2);
    assertThat(c2).isSameAs(c3);

    // Provider invoked only once
    assertThat(loadCount.get()).isEqualTo(1);
  }

  @Test
  void clear_evictionForcesReload() {
    AtomicInteger loadCount = new AtomicInteger();

    SystemCatalogProvider provider =
        new SystemCatalogProvider() {
          @Override
          public SystemEngineCatalog load(String engineKind) {
            loadCount.incrementAndGet();
            return SystemEngineCatalog.empty(engineKind);
          }

          @Override
          public List<String> engineKinds() {
            return List.of("spark");
          }
        };

    SystemDefinitionRegistry registry = new SystemDefinitionRegistry(provider);

    SystemEngineCatalog first = registry.catalog("spark");
    assertThat(loadCount.get()).isEqualTo(1);

    // Clear cache
    registry.clear();

    SystemEngineCatalog second = registry.catalog("spark");
    assertThat(loadCount.get()).isEqualTo(2);

    // Different instance after clear
    assertThat(second).isNotSameAs(first);
  }
}
