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

package ai.floedb.floecat.extensions.floedb.pgcatalog;

import static ai.floedb.floecat.extensions.floedb.pgcatalog.PgCatalogTestSupport.*;
import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.Descriptor.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.extensions.floedb.utils.MissingSystemOidException;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for pg_catalog.pg_proc scanner.
 *
 * <p>This test validates:
 *
 * <ul>
 *   <li>FunctionNode → pg_proc mapping
 *   <li>EngineHint decoding
 *   <li>SYSTEM objects require persisted hints (no fallback)
 * </ul>
 *
 * <p>NOTE: There are currently no USER functions; tests must not create user FunctionNodes.
 */
final class PgProcScannerTest {

  private final SystemObjectScanner scanner = new PgProcScanner();

  @Test
  void scan_emitsRowsForAllFunctions() {
    SystemObjectScanContext ctx = contextWithSystemFunctions();

    List<SystemObjectRow> rows = scanner.scan(ctx).toList();

    assertThat(rows)
        .extracting(r -> r.values()[1]) // proname
        .containsExactlyInAnyOrder("int4_abs", "text_length", "sum_int4_state");
  }

  @Test
  void scan_usesEnginePayloadWhenPresent() {
    SystemObjectRow row =
        scanner
            .scan(contextWithSystemFunctions())
            .filter(r -> "int4_abs".equals(r.values()[1]))
            .findFirst()
            .orElseThrow();

    Object[] v = row.values();

    assertThat(v[0]).isEqualTo(1250); // oid
    assertThat(v[5]).isEqualTo(false); // proisagg
  }

  @Test
  void scan_marksAggregatesCorrectly() {
    List<SystemObjectRow> rows =
        scanner
            .scan(contextWithSystemFunctions())
            .filter(r -> (boolean) r.values()[5]) // proisagg
            .toList();

    assertThat(rows).extracting(r -> r.values()[1]).containsExactly("sum_int4_state");
  }

  @Test
  void scan_marksWindowFunctionsCorrectly() {
    SystemObjectScanContext ctx = contextWithSystemWindowFunction();

    SystemObjectRow windowRow =
        scanner
            .scan(ctx)
            .filter(r -> "my_window_fn".equals(r.values()[1]))
            .findFirst()
            .orElseThrow();

    Object[] vals = windowRow.values();
    assertThat(vals[6]).isEqualTo(true); // proiswindow
    assertThat(vals[5]).isEqualTo(false); // proisagg
  }

  @Test
  void scan_throws_whenSystemPayloadMissing() {
    assertThatThrownBy(() -> scanner.scan(contextWithSystemFunctionMissingPayload()).toList())
        .isInstanceOf(MissingSystemOidException.class);
  }

  // ----------------------------------------------------------------------
  // Test fixtures
  // ----------------------------------------------------------------------

  private static SystemObjectScanContext contextWithSystemFunctions() {
    NamespaceNode ns = systemPgCatalogNamespace();
    FunctionNode int4Abs =
        systemFunction(
            ns.id(),
            "int4_abs",
            false,
            false,
            functionHint(1250, "int4_abs", 23, false, false, 23));
    FunctionNode textLength =
        systemFunction(
            ns.id(),
            "text_length",
            false,
            false,
            functionHint(1300, "text_length", 23, false, false, 25));
    FunctionNode sumState =
        systemFunction(
            ns.id(),
            "sum_int4_state",
            true,
            false,
            functionHint(1400, "sum_int4_state", 23, true, false, 23));

    return contextWithFunctions(ns, int4Abs, textLength, sumState);
  }

  private static SystemObjectScanContext contextWithSystemWindowFunction() {
    NamespaceNode ns = systemPgCatalogNamespace();
    FunctionNode windowFn =
        systemFunction(
            ns.id(),
            "my_window_fn",
            false,
            true,
            functionHint(1500, "my_window_fn", 23, false, true, 23));

    return contextWithFunctions(ns, windowFn);
  }

  private static SystemObjectScanContext contextWithSystemFunctionMissingPayload() {
    NamespaceNode ns = systemPgCatalogNamespace();
    FunctionNode missingPayload = systemFunction(ns.id(), "text_length", false, false, Map.of());

    return contextWithFunctions(ns, missingPayload);
  }

  private static Map<EngineHintKey, EngineHint> functionHint(
      int oid, String name, int prorettype, boolean isAgg, boolean isWindow, int... argtypes) {
    FloeFunctionSpecific.Builder builder =
        FloeFunctionSpecific.newBuilder()
            .setOid(oid)
            .setProname(name)
            .setProisagg(isAgg)
            .setProiswindow(isWindow)
            .setProrettype(prorettype);
    for (int arg : argtypes) {
      builder.addProargtypes(arg);
    }
    return Map.of(
        new EngineHintKey(
            ENGINE_CTX.normalizedKind(), ENGINE_CTX.normalizedVersion(), FUNCTION.type()),
        new EngineHint(FUNCTION.type(), builder.build().toByteArray()));
  }
}
