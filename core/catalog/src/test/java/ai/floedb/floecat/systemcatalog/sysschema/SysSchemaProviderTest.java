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

package ai.floedb.floecat.systemcatalog.sysschema;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.systemcatalog.statssystable.StatsColumnScanner;
import ai.floedb.floecat.systemcatalog.statssystable.StatsExpressionScanner;
import ai.floedb.floecat.systemcatalog.statssystable.StatsSnapshotScanner;
import ai.floedb.floecat.systemcatalog.statssystable.StatsTableScanner;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import org.junit.jupiter.api.Test;

class SysSchemaProviderTest {

  private final SysSchemaProvider provider = new SysSchemaProvider();

  @Test
  void definitions_areEmpty() {
    assertThat(provider.definitions()).isEmpty();
  }

  @Test
  void supports_recognizesStatsTables() {
    assertThat(provider.supports(NameRefUtil.name("sys", "stats_snapshot"), "duckdb")).isTrue();
    assertThat(provider.supports(NameRefUtil.name("sys", "stats_table"), "duckdb")).isTrue();
    assertThat(provider.supports(NameRefUtil.name("sys", "stats_column"), "duckdb")).isTrue();
    assertThat(provider.supports(NameRefUtil.name("sys", "stats_expression"), "duckdb")).isTrue();
  }

  @Test
  void supports_rejectsNonSysTables() {
    assertThat(provider.supports(NameRefUtil.name("information_schema", "tables"), "duckdb"))
        .isFalse();
  }

  @Test
  void supports_returnsFalseForNullName() {
    assertThat(provider.supports(null, "duckdb")).isFalse();
  }

  @Test
  void provide_returnsExpectedScannerTypes() {
    assertThat(provider.provide("stats_snapshot_scanner", "duckdb", "1.0"))
        .get()
        .isInstanceOf(StatsSnapshotScanner.class);
    assertThat(provider.provide("stats_table_scanner", "duckdb", "1.0"))
        .get()
        .isInstanceOf(StatsTableScanner.class);
    assertThat(provider.provide("stats_column_scanner", "duckdb", "1.0"))
        .get()
        .isInstanceOf(StatsColumnScanner.class);
    assertThat(provider.provide("stats_expression_scanner", "duckdb", "1.0"))
        .get()
        .isInstanceOf(StatsExpressionScanner.class);
  }

  @Test
  void provide_returnsEmptyForUnknownOrNullScannerId() {
    assertThat(provider.provide("unknown_scanner", "duckdb", "1.0")).isEmpty();
    assertThat(provider.provide(null, "duckdb", "1.0")).isEmpty();
  }

  @Test
  void supports_isCaseInsensitive() {
    NameRef mixedCase = NameRef.newBuilder().addPath("SyS").setName("StAtS_TaBlE").build();
    assertThat(provider.supports(mixedCase, "duckdb")).isTrue();
    assertThat(provider.provide("StAtS_TaBlE_sCaNnEr", "duckdb", "1.0")).isPresent();
  }
}
