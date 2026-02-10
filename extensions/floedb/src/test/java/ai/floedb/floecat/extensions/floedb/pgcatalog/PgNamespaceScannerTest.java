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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for pg_catalog.pg_namespace scanner.
 *
 * <p>Validates: - Engine-specific payload decoding - Fallback behavior when payload is missing -
 * Stable deterministic OIDs
 */
final class PgNamespaceScannerTest {

  private final PgNamespaceScanner scanner = new PgNamespaceScanner();
  private static final EngineContext ENGINE_CTX = EngineContext.of("floedb", "1.0");

  @Test
  void scan_usesEngineSpecificPayload_whenPresent() {
    NamespaceNode pgCatalog =
        namespace(
            "pg_catalog",
            Map.of(
                new EngineHintKey("floedb", "1.0", "floe.namespace+proto"),
                new EngineHint(
                    "floe.namespace+proto",
                    FloeNamespaceSpecific.newBuilder()
                        .setOid(11)
                        .setNspname("pg_catalog")
                        .build()
                        .toByteArray())));

    SystemObjectScanContext ctx = contextWith(pgCatalog);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isEqualTo(11); // oid
    assertThat(v[1]).isEqualTo("pg_catalog"); // nspname
  }

  @Test
  void scan_fallsBack_whenPayloadMissing() {
    NamespaceNode ns = namespace("public", Map.of());

    SystemObjectScanContext ctx = contextWith(ns);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isInstanceOf(Integer.class); // oid fallback
    assertThat(v[1]).isEqualTo("public"); // displayName
  }

  @Test
  void scan_oidFallback_isStable() {
    NamespaceNode ns = namespace("analytics", Map.of());

    SystemObjectScanContext ctx = contextWith(ns);

    int oid1 = (int) scanner.scan(ctx).findFirst().orElseThrow().values()[0];
    int oid2 = (int) scanner.scan(ctx).findFirst().orElseThrow().values()[0];

    assertThat(oid1).isEqualTo(oid2);
  }

  // ----------------------------------------------------------------------
  // Test fixtures
  // ----------------------------------------------------------------------

  private static SystemObjectScanContext contextWith(NamespaceNode... namespaces) {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    for (NamespaceNode ns : namespaces) {
      overlay.addNode(ns);
    }

    return new SystemObjectScanContext(overlay, null, catalogId(), ENGINE_CTX);
  }

  private static NamespaceNode namespace(String name, Map<EngineHintKey, EngineHint> engineHints) {
    return new NamespaceNode(
        PgCatalogTestIds.namespace(name),
        1,
        Instant.EPOCH,
        catalogId(),
        List.of(),
        name,
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        engineHints);
  }

  private static ResourceId catalogId() {
    return PgCatalogTestIds.catalog();
  }
}
