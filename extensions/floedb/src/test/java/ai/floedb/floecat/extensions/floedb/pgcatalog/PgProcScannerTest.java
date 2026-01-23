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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.spi.scanner.*;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for pg_catalog.pg_proc scanner.
 *
 * <p>This test validates: - FunctionNode → pg_proc mapping - EngineHint decoding - Default behavior
 * when hints are missing
 *
 * <p>It does NOT test SystemGraph or pbtxt loading.
 */
final class PgProcScannerTest {

  private final SystemObjectScanner scanner = new PgProcScanner();
  private static final EngineContext ENGINE_CTX = EngineContext.of("floe-demo", "1.0");

  @Test
  void scan_emitsRowsForAllFunctions() {
    SystemObjectScanContext ctx = contextWithFunctions();

    List<SystemObjectRow> rows = scanner.scan(ctx).toList();

    assertThat(rows)
        .extracting(r -> r.values()[1]) // proname
        .containsExactlyInAnyOrder("int4_abs", "text_length", "sum_int4_state");
  }

  @Test
  void scan_usesEnginePayloadWhenPresent() {
    SystemObjectRow row =
        scanner
            .scan(contextWithFunctions())
            .filter(r -> "int4_abs".equals(r.values()[1]))
            .findFirst()
            .orElseThrow();

    Object[] v = row.values();

    assertThat(v[0]).isEqualTo(1250); // oid
    assertThat(v[5]).isEqualTo(false); // proisagg
  }

  @Test
  void scan_defaultsWhenPayloadMissing() {
    SystemObjectRow row =
        scanner
            .scan(contextWithFunctions())
            .filter(r -> "text_length".equals(r.values()[1]))
            .findFirst()
            .orElseThrow();

    Object[] v = row.values();

    assertThat(v[0]).isInstanceOf(Integer.class);
    assertThat((int) v[0]).isNotZero();
    assertThat(v[5]).isEqualTo(false);
  }

  @Test
  void scan_marksAggregatesCorrectly() {
    List<SystemObjectRow> rows =
        scanner
            .scan(contextWithFunctions())
            .filter(r -> (boolean) r.values()[5]) // proisagg
            .toList();

    assertThat(rows).extracting(r -> r.values()[1]).containsExactly("sum_int4_state");
  }

  @Test
  void scan_marksWindowFunctionsCorrectly() {
    SystemObjectScanContext ctx = contextWithWindowFunction();

    List<SystemObjectRow> rows = scanner.scan(ctx).toList();

    SystemObjectRow windowRow =
        rows.stream().filter(r -> "my_window_fn".equals(r.values()[1])).findFirst().orElseThrow();

    Object[] vals = windowRow.values();

    assertThat(vals[6]).isEqualTo(true); // proiswindow
    assertThat(vals[5]).isEqualTo(false); // proisagg
  }

  @Test
  void scan_getNodeOidIsStable() {
    SystemObjectScanContext ctx = contextWithFunctions();

    List<SystemObjectRow> rows1 = scanner.scan(ctx).toList();
    List<SystemObjectRow> rows2 = scanner.scan(ctx).toList();

    int oid1 =
        rows1.stream()
            .filter(r -> "text_length".equals(r.values()[1]))
            .findFirst()
            .map(r -> (int) r.values()[0])
            .orElseThrow();

    int oid2 =
        rows2.stream()
            .filter(r -> "text_length".equals(r.values()[1]))
            .findFirst()
            .map(r -> (int) r.values()[0])
            .orElseThrow();

    assertThat(oid1).isEqualTo(oid2);
  }

  // ----------------------------------------------------------------------
  // Test fixtures
  // ----------------------------------------------------------------------

  private static SystemObjectScanContext contextWithFunctions() {
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("floe-demo:pg_catalog")
            .build();

    NamespaceNode pgCatalog =
        new NamespaceNode(
            namespaceId,
            1,
            Instant.EPOCH,
            ridCatalog(),
            List.of("pg_catalog"),
            "pg_catalog",
            GraphNodeOrigin.SYSTEM,
            Map.of(),
            Optional.empty(),
            Map.of());

    FunctionNode int4Abs =
        new FunctionNode(
            rid("int4_abs"),
            1,
            Instant.EPOCH,
            "15",
            "int4_abs",
            List.of(),
            null,
            false,
            false,
            Map.of(
                new EngineHintKey("floe-demo", "1.0", "floe.function+proto"),
                new EngineHint(
                    "floe.function+proto",
                    FloeFunctionSpecific.newBuilder()
                        .setOid(1250)
                        .setProname("int4_abs")
                        .setProisagg(false)
                        .build()
                        .toByteArray())));

    FunctionNode textLength =
        new FunctionNode(
            rid("text_length"),
            1,
            Instant.EPOCH,
            "15",
            "text_length",
            List.of(),
            null,
            false,
            false,
            Map.of());

    FunctionNode sumState =
        new FunctionNode(
            rid("sum_int4_state"),
            1,
            Instant.EPOCH,
            "15",
            "sum_int4_state",
            List.of(),
            null,
            true,
            false,
            Map.of());

    TestCatalogOverlay graph =
        new TestCatalogOverlay()
            .addNode(pgCatalog)
            .addFunction(namespaceId, int4Abs)
            .addFunction(namespaceId, textLength)
            .addFunction(namespaceId, sumState);

    return new SystemObjectScanContext(
        graph, NameRef.getDefaultInstance(), ridCatalog(), ENGINE_CTX);
  }

  private static SystemObjectScanContext contextWithWindowFunction() {
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("floe-demo:pg_catalog")
            .build();

    NamespaceNode pgCatalog =
        new NamespaceNode(
            namespaceId,
            1,
            Instant.EPOCH,
            ridCatalog(),
            List.of("pg_catalog"),
            "pg_catalog",
            GraphNodeOrigin.SYSTEM,
            Map.of(),
            Optional.empty(),
            Map.of());

    FunctionNode int4Abs =
        new FunctionNode(
            rid("int4_abs"),
            1,
            Instant.EPOCH,
            "15",
            "int4_abs",
            List.of(),
            null,
            false,
            false,
            Map.of(
                new EngineHintKey("floe-demo", "1.0", "floe.function+proto"),
                new EngineHint(
                    "floe.function+proto",
                    FloeFunctionSpecific.newBuilder()
                        .setOid(1250)
                        .setProname("int4_abs")
                        .setProisagg(false)
                        .build()
                        .toByteArray())));

    FunctionNode textLength =
        new FunctionNode(
            rid("text_length"),
            1,
            Instant.EPOCH,
            "15",
            "text_length",
            List.of(),
            null,
            false,
            false,
            Map.of());

    FunctionNode sumState =
        new FunctionNode(
            rid("sum_int4_state"),
            1,
            Instant.EPOCH,
            "15",
            "sum_int4_state",
            List.of(),
            null,
            true,
            false,
            Map.of());

    FunctionNode myWindowFn =
        new FunctionNode(
            rid("my_window_fn"),
            1,
            Instant.EPOCH,
            "15",
            "my_window_fn",
            List.of(),
            null,
            false,
            true,
            Map.of());

    TestCatalogOverlay graph =
        new TestCatalogOverlay()
            .addNode(pgCatalog)
            .addFunction(namespaceId, int4Abs)
            .addFunction(namespaceId, textLength)
            .addFunction(namespaceId, sumState)
            .addFunction(namespaceId, myWindowFn);

    return new SystemObjectScanContext(
        graph, NameRef.getDefaultInstance(), ridCatalog(), ENGINE_CTX);
  }

  private static ResourceId rid(String name) {
    return ResourceId.newBuilder()
        .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_FUNCTION)
        .setId("postgres:" + name)
        .build();
  }

  private static ResourceId ridCatalog() {
    return ResourceId.newBuilder()
        .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_CATALOG)
        .setId("postgres:system")
        .build();
  }
}
