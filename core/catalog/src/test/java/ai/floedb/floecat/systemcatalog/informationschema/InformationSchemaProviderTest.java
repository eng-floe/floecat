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

package ai.floedb.floecat.systemcatalog.informationschema;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class InformationSchemaProviderTest {

  private final InformationSchemaProvider provider = new InformationSchemaProvider();

  // ------------------------------------------------------------------------
  // definitions() correctness
  // ------------------------------------------------------------------------
  @Test
  void definitions_areEmpty() {
    assertThat(provider.definitions()).isEmpty();
  }

  // ------------------------------------------------------------------------
  // supports(NameRef) logic
  // ------------------------------------------------------------------------
  @Test
  void supports_recognizesInformationSchemaTables() {
    NameRef ref = NameRefUtil.name("information_schema", "tables");

    assertThat(provider.supports(ref, "spark")).isTrue();
  }

  @Test
  void supports_rejectsWrongSchema() {
    NameRef ref = NameRefUtil.name("not_schema", "tables");
    assertThat(provider.supports(ref, "spark")).isFalse();
  }

  @Test
  void supports_rejectsUnknownObject() {
    NameRef ref = NameRefUtil.name("information_schema", "unknown");
    assertThat(provider.supports(ref, "spark")).isFalse();
  }

  @Test
  void supports_isCaseInsensitive() {
    NameRef ref = NameRefUtil.name("InFoRmAtIoN_sChEmA", "TaBlEs");
    assertThat(provider.supports(ref, "spark")).isTrue();
  }

  @Test
  void supports_rejectsUnsupportedInformationSchemaObject() {
    NameRef ref = NameRefUtil.name("information_schema", "sequences");
    assertThat(provider.supports(ref, "spark")).isFalse();
  }

  @Test
  void supports_returnsFalseForNullName() {
    assertThat(provider.supports(null, "spark")).isFalse();
  }

  // ------------------------------------------------------------------------
  // provide(ScannerId) logic
  // ------------------------------------------------------------------------
  @Test
  void provide_returnsCorrectScannerForTables() {
    Optional<SystemObjectScanner> scanner = provider.provide("tables_scanner", "spark", "3.5.0");

    assertThat(scanner).isPresent();
    assertThat(scanner.get()).isInstanceOf(TablesScanner.class);
  }

  @Test
  void provide_returnsCorrectScannerForColumns() {
    Optional<SystemObjectScanner> scanner = provider.provide("columns_scanner", "spark", "3.5.0");

    assertThat(scanner).isPresent();
    assertThat(scanner.get()).isInstanceOf(ColumnsScanner.class);
  }

  @Test
  void provide_returnsCorrectScannerForSchemata() {
    Optional<SystemObjectScanner> scanner = provider.provide("schemata_scanner", "spark", "3.5.0");

    assertThat(scanner).isPresent();
    assertThat(scanner.get()).isInstanceOf(SchemataScanner.class);
  }

  @Test
  void provide_returnsEmptyForUnknownObject() {
    Optional<SystemObjectScanner> scanner = provider.provide("nope_scanner", "spark", "3.5.0");

    assertThat(scanner).isEmpty();
  }

  @Test
  void provide_returnsEmptyForNullScannerId() {
    assertThat(provider.provide(null, "spark", "3.5.0")).isEmpty();
  }

  @Test
  void provide_isCaseInsensitive() {
    Optional<SystemObjectScanner> scanner = provider.provide("TaBlEs_scanner", "spark", "3.5.0");
    assertThat(scanner).isPresent();
    assertThat(scanner.get()).isInstanceOf(TablesScanner.class);
  }

  // ------------------------------------------------------------------------
  // Provider should not care about engine kind/version
  // ------------------------------------------------------------------------
  @Test
  void supports_isEngineAgnostic() {
    NameRef ref = NameRefUtil.name("information_schema", "tables");

    assertThat(provider.supports(ref, "duckdb")).isTrue();
    assertThat(provider.provide("tables_scanner", "trino", "450")).isPresent();
  }
}
