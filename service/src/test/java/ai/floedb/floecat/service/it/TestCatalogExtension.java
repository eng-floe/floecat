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

package ai.floedb.floecat.service.catalog.it;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastMethod;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Test-only {@link EngineSystemCatalogExtension} for {@code SystemObjectsServiceIT}.
 *
 * <p>Provides a fully programmatic catalog for the {@code "test-engine"} engine kind, with exactly
 * the objects that {@code SystemObjectsServiceIT} asserts on. This avoids any dependency on the
 * FloeDB extension's classpath resources, so the integration tests remain stable after the FloeDB
 * extension is removed.
 *
 * <p>Registered via {@code META-INF/services} in the test classpath.
 */
public final class TestCatalogExtension implements EngineSystemCatalogExtension {

  static final String ENGINE_KIND = "test-engine";

  /**
   * Version-constraining rule: functions carrying this rule are only served when the engine reports
   * version {@code "16.0"} or higher. Functions with no rules are always served.
   */
  private static final EngineSpecificRule RULE_MIN_16 =
      new EngineSpecificRule(ENGINE_KIND, "16.0", "", "test.payload", new byte[0], Map.of());

  // ---------------------------------------------------------------------------
  // EngineSystemCatalogExtension
  // ---------------------------------------------------------------------------

  @Override
  public String engineKind() {
    return ENGINE_KIND;
  }

  @Override
  public SystemCatalogData loadSystemCatalog() {
    return new SystemCatalogData(
        functions(),
        operators(),
        types(),
        casts(),
        collations(),
        aggregates(),
        namespaces(),
        /* tables= */ List.of(),
        /* views= */ List.of(),
        /* registryEngineSpecific= */ List.of());
  }

  // ---------------------------------------------------------------------------
  // Catalog contents
  // ---------------------------------------------------------------------------

  /** Three rule-free functions returned for any version, plus four gated on min_version 16.0. */
  private static List<SystemFunctionDef> functions() {
    // Rule-free: always returned regardless of engine version.
    var textLength =
        new SystemFunctionDef(
            nr("pg_catalog.text_length"),
            List.of(nr("pg_catalog.text")),
            nr("pg_catalog.int4"),
            false,
            false,
            List.of());
    var int4Add =
        new SystemFunctionDef(
            nr("pg_catalog.int4_add"),
            List.of(nr("pg_catalog.int4"), nr("pg_catalog.int4")),
            nr("pg_catalog.int4"),
            false,
            false,
            List.of());
    var textConcat =
        new SystemFunctionDef(
            nr("pg_catalog.text_concat"),
            List.of(nr("pg_catalog.text"), nr("pg_catalog.text")),
            nr("pg_catalog.text"),
            false,
            false,
            List.of());

    // Version-constrained: only returned when engine version >= "16.0".
    var textUpper =
        new SystemFunctionDef(
            nr("pg_catalog.text_upper"),
            List.of(nr("pg_catalog.text")),
            nr("pg_catalog.text"),
            false,
            false,
            List.of(RULE_MIN_16));
    var int4Abs =
        new SystemFunctionDef(
            nr("pg_catalog.int4_abs"),
            List.of(nr("pg_catalog.int4")),
            nr("pg_catalog.int4"),
            false,
            false,
            List.of(RULE_MIN_16));
    var sumInt4State =
        new SystemFunctionDef(
            nr("pg_catalog.sum_int4_state"),
            List.of(nr("pg_catalog.int8"), nr("pg_catalog.int4")),
            nr("pg_catalog.int8"),
            false,
            false,
            List.of(RULE_MIN_16));
    var sumInt4Final =
        new SystemFunctionDef(
            nr("pg_catalog.sum_int4_final"),
            List.of(nr("pg_catalog.int8")),
            nr("pg_catalog.int8"),
            false,
            false,
            List.of(RULE_MIN_16));

    return List.of(textLength, int4Add, textConcat, textUpper, int4Abs, sumInt4State, sumInt4Final);
  }

  /** Two operators asserted by the IT test: {@code +} and {@code ||}. */
  private static List<SystemOperatorDef> operators() {
    var plus =
        new SystemOperatorDef(
            nr("+"),
            nr("pg_catalog.int4"),
            nr("pg_catalog.int4"),
            nr("pg_catalog.int4"),
            true,
            true,
            List.of());
    var concat =
        new SystemOperatorDef(
            nr("||"),
            nr("pg_catalog.text"),
            nr("pg_catalog.text"),
            nr("pg_catalog.text"),
            false,
            false,
            List.of());
    return List.of(plus, concat);
  }

  /**
   * Four types. The IT test asserts {@code pg_catalog._int4} is present; the scalar types are
   * referenced by functions and the array type fulfills the {@code contains("pg_catalog._int4")}
   * assertion.
   */
  private static List<SystemTypeDef> types() {
    var int4 = new SystemTypeDef(nr("pg_catalog.int4"), "N", false, null, List.of());
    var int8 = new SystemTypeDef(nr("pg_catalog.int8"), "N", false, null, List.of());
    var text = new SystemTypeDef(nr("pg_catalog.text"), "S", false, null, List.of());
    var int4Array =
        new SystemTypeDef(nr("pg_catalog._int4"), "A", true, nr("pg_catalog.int4"), List.of());
    return List.of(int4, int8, text, int4Array);
  }

  /** One cast whose source type is {@code pg_catalog.text} (as asserted by the IT test). */
  private static List<SystemCastDef> casts() {
    return List.of(
        new SystemCastDef(
            nr("pg_catalog.text_to_int4"),
            nr("pg_catalog.text"),
            nr("pg_catalog.int4"),
            SystemCastMethod.EXPLICIT,
            List.of()));
  }

  /** One collation (the IT test only asserts {@code hasSize(1)}). */
  private static List<SystemCollationDef> collations() {
    return List.of(new SystemCollationDef(nr("pg_catalog.default"), "en_US", List.of()));
  }

  /** One aggregate {@code pg_catalog.sum} as asserted by the IT test. */
  private static List<SystemAggregateDef> aggregates() {
    return List.of(
        new SystemAggregateDef(
            nr("pg_catalog.sum"),
            List.of(nr("pg_catalog.int4")),
            nr("pg_catalog.int8"),
            nr("pg_catalog.int8"),
            List.of()));
  }

  /** Namespace declaration for {@code pg_catalog}. */
  private static List<SystemNamespaceDef> namespaces() {
    return List.of(new SystemNamespaceDef(nr("pg_catalog"), "pg_catalog", List.of()));
  }

  // ---------------------------------------------------------------------------
  // SystemObjectScannerProvider — no scanner-backed objects in this extension
  // ---------------------------------------------------------------------------

  @Override
  public List<SystemObjectDef> definitions() {
    return List.of();
  }

  @Override
  public boolean supportsEngine(String engineKind) {
    return ENGINE_KIND.equals(engineKind);
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

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  /**
   * Builds a {@link NameRef} from {@code "path.name"} notation (single-level path) or bare {@code
   * "name"} (no path).
   */
  private static NameRef nr(String full) {
    int idx = full.indexOf('.');
    if (idx < 0) {
      return NameRef.newBuilder().setName(full).build();
    }
    return NameRef.newBuilder()
        .addPath(full.substring(0, idx))
        .setName(full.substring(idx + 1))
        .build();
  }
}
