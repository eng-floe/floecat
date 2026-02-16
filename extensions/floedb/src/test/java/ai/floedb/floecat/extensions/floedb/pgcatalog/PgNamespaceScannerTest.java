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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.floedb.utils.MissingSystemOidException;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for pg_catalog.pg_namespace scanner.
 *
 * <p>Validates:
 *
 * <ul>
 *   <li>Engine-specific payload decoding
 *   <li>SYSTEM objects require persisted hints (no fallback)
 *   <li>USER namespaces may fall back when payload is missing
 *   <li>Stable deterministic OIDs (for USER fallback)
 * </ul>
 */
final class PgNamespaceScannerTest {

  private final PgNamespaceScanner scanner = new PgNamespaceScanner();

  @Test
  void scan_usesEngineSpecificPayload_whenPresent() {
    NamespaceNode pgCatalog =
        systemNamespace(
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

    SystemObjectScanContext ctx = contextWithNamespaces(pgCatalog);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isEqualTo(11);
    assertThat(v[1]).isEqualTo("pg_catalog");
  }

  @Test
  void scan_throws_whenSystemPayloadMissing() {
    NamespaceNode ns = systemNamespace("public", Map.of());
    SystemObjectScanContext ctx = contextWithNamespaces(ns);

    assertThatThrownBy(() -> scanner.scan(ctx).findFirst().orElseThrow())
        .isInstanceOf(MissingSystemOidException.class);
  }

  @Test
  void scan_fallsBack_whenUserPayloadMissing() {
    NamespaceNode ns = userNamespace("public", Map.of());
    SystemObjectScanContext ctx = contextWithNamespaces(ns);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isInstanceOf(Integer.class);
    assertThat((int) v[0]).isNotZero();
    assertThat(v[1]).isEqualTo("public");
  }

  @Test
  void scan_oidFallback_isStable() {
    NamespaceNode ns = userNamespace("analytics", Map.of());
    SystemObjectScanContext ctx = contextWithNamespaces(ns);

    int oid1 = (int) scanner.scan(ctx).findFirst().orElseThrow().values()[0];
    int oid2 = (int) scanner.scan(ctx).findFirst().orElseThrow().values()[0];

    assertThat(oid1).isEqualTo(oid2);
  }
}
