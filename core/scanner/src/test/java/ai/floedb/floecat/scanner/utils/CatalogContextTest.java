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

package ai.floedb.floecat.scanner.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

final class CatalogContextTest {

  @Test
  void emptyEnvironmentRemainsIndependentFromEngine() {
    EngineContext engine = EngineContext.of("duckdb", "1.0");

    CatalogContext context = CatalogContext.of(EnvironmentContext.empty(), engine);

    assertThat(context.environment()).isEqualTo(EnvironmentContext.empty());
    assertThat(context.engine()).isEqualTo(engine);
  }

  @Test
  void explicitEnvironmentIsIndependentOfEngine() {
    EngineContext engine = EngineContext.of("duckdb", "1.0");
    EnvironmentContext environment = EnvironmentContext.of("floedb", "2.0");

    CatalogContext context = CatalogContext.of(environment, engine);

    assertThat(context.environment().normalizedKind()).isEqualTo("floedb");
    assertThat(context.engine().normalizedKind()).isEqualTo("duckdb");
  }

  @Test
  void forEngine_selectsInternalOnlyWhenEngineIsAbsent() {
    CatalogContext absent = CatalogContext.forEngine(EngineContext.empty());
    CatalogContext explicit = CatalogContext.forEngine(EngineContext.of("duckdb", "1.0"));

    assertThat(absent).isEqualTo(CatalogContext.floecatInternal());
    assertThat(explicit.environment()).isEqualTo(EnvironmentContext.empty());
    assertThat(explicit.engine().normalizedKind()).isEqualTo("duckdb");
  }

  @Test
  void forRequest_selectsInternalOnlyWhenBothAxesAreAbsent() {
    CatalogContext absent =
        CatalogContext.forRequest(EnvironmentContext.empty(), EngineContext.empty());
    CatalogContext environmentOnly =
        CatalogContext.forRequest(EnvironmentContext.of("floe", "3.1"), EngineContext.empty());

    assertThat(absent).isEqualTo(CatalogContext.floecatInternal());
    assertThat(environmentOnly.environment().normalizedKind()).isEqualTo("floe");
    assertThat(environmentOnly.engine()).isEqualTo(EngineContext.empty());
  }

  @Test
  void systemCatalogKindUsesEngineThenEnvironmentThenInternal() {
    assertThat(
            CatalogContext.of(EnvironmentContext.empty(), EngineContext.of("duckdb", "1.0"))
                .effectiveSystemCatalogKind())
        .isEqualTo("duckdb");
    assertThat(
            CatalogContext.of(EnvironmentContext.of("floe", "3.1"), EngineContext.empty())
                .effectiveSystemCatalogKind())
        .isEqualTo("floe");
    assertThat(CatalogContext.empty().effectiveSystemCatalogKind())
        .isEqualTo(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
  }
}
