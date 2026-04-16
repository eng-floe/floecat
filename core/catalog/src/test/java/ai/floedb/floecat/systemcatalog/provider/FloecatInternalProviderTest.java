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
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import org.junit.jupiter.api.Test;

class FloecatInternalProviderTest {

  @Test
  void catalogDataContainsInformationSchemaTables() {
    SystemCatalogData catalog = FloecatInternalProvider.catalogData();

    assertThat(catalog.tables()).isNotEmpty();
    assertThat(catalog.tables().stream().map(table -> NameRefUtil.canonical(table.name())).toList())
        .contains(
            "information_schema.tables",
            "information_schema.columns",
            "information_schema.schemata");
  }

  @Test
  void catalogDataContainsSysStatsTables() {
    SystemCatalogData catalog = FloecatInternalProvider.catalogData();

    assertThat(catalog.tables().stream().map(table -> NameRefUtil.canonical(table.name())).toList())
        .contains(
            "sys.stats_snapshot", "sys.stats_table", "sys.stats_column", "sys.stats_expression");
  }

  @Test
  void catalogDataContainsSystemNamespaces() {
    SystemCatalogData catalog = FloecatInternalProvider.catalogData();
    assertThat(
            catalog.namespaces().stream()
                .map(namespace -> NameRefUtil.canonical(namespace.name()))
                .toList())
        .contains("information_schema", "sys");
  }

  @Test
  void supportsSysStatsTablesByName() {
    FloecatInternalProvider provider = new FloecatInternalProvider();

    assertThat(
            provider.supports(
                NameRef.newBuilder().addPath("sys").setName("stats_table").build(), "duckdb"))
        .isTrue();
  }

  @Test
  void supportsInformationSchemaTablesByName() {
    FloecatInternalProvider provider = new FloecatInternalProvider();

    assertThat(
            provider.supports(
                NameRef.newBuilder().addPath("information_schema").setName("tables").build(),
                "duckdb"))
        .isTrue();
  }

  @Test
  void providesStatsSnapshotScanner() {
    FloecatInternalProvider provider = new FloecatInternalProvider();
    assertThat(provider.provide("stats_snapshot_scanner", "duckdb", "1.0")).isPresent();
  }

  @Test
  void providesStatsTableScanner() {
    FloecatInternalProvider provider = new FloecatInternalProvider();
    assertThat(provider.provide("stats_table_scanner", "duckdb", "1.0")).isPresent();
  }

  @Test
  void providesStatsColumnScanner() {
    FloecatInternalProvider provider = new FloecatInternalProvider();
    assertThat(provider.provide("stats_column_scanner", "duckdb", "1.0")).isPresent();
  }

  @Test
  void providesStatsExpressionScanner() {
    FloecatInternalProvider provider = new FloecatInternalProvider();
    assertThat(provider.provide("stats_expression_scanner", "duckdb", "1.0")).isPresent();
  }

  @Test
  void providesInformationSchemaScanner() {
    FloecatInternalProvider provider = new FloecatInternalProvider();
    assertThat(provider.provide("tables_scanner", "duckdb", "1.0")).isPresent();
  }

  @Test
  void provideReturnsEmptyForNullAndUnknownScannerIds() {
    FloecatInternalProvider provider = new FloecatInternalProvider();
    assertThat(provider.provide(null, "duckdb", "1.0")).isEmpty();
    assertThat(provider.provide("unknown_scanner", "duckdb", "1.0")).isEmpty();
  }
}
