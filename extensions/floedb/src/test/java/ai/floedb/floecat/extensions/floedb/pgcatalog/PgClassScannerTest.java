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

import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.utils.MissingSystemOidException;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for pg_catalog.pg_class scanner.
 *
 * <p>Validates:
 *
 * <ul>
 *   <li>Engine-specific payload decoding
 *   <li>SYSTEM objects require persisted hints (no fallback)
 *   <li>USER tables may fall back when payload is missing
 *   <li>Stable deterministic OIDs (for USER fallback)
 *   <li>relkind derivation (table vs view)
 * </ul>
 */
final class PgClassScannerTest {

  private final PgClassScanner scanner = new PgClassScanner();

  @Test
  void scan_usesEngineSpecificPayload_whenPresent() {

    NamespaceNode ns = systemPgCatalogNamespace();
    TableNode table =
        systemTable(
            ns.id(),
            "my_table",
            List.of(),
            Map.of(),
            Map.of(
                new EngineHintKey("floedb", "1.0", RELATION.type()),
                new EngineHint(
                    RELATION.type(),
                    FloeRelationSpecific.newBuilder()
                        .setOid(1001)
                        .setRelname("my_table")
                        .setRelnamespace(11)
                        .setRelkind("r")
                        .setReltuples(42)
                        .build()
                        .toByteArray())));

    SystemObjectScanContext ctx = contextWithRelations(ns, table);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isEqualTo(1001);
    assertThat(v[1]).isEqualTo("my_table");
    assertThat(v[2]).isEqualTo(11);
    assertThat(v[3]).isEqualTo("r");
    assertThat(v[4]).isEqualTo(42.0f);
  }

  @Test
  void scan_throws_whenSystemPayloadMissing() {
    NamespaceNode ns = systemPgCatalogNamespace();
    TableNode table = systemTable(ns.id(), "no_payload", List.of(), Map.of(), Map.of());
    SystemObjectScanContext ctx = contextWithRelations(ns, table);

    assertThatThrownBy(() -> scanner.scan(ctx).findFirst().orElseThrow())
        .isInstanceOf(MissingSystemOidException.class);
  }

  @Test
  void scan_fallsBack_whenUserPayloadMissing() {
    NamespaceNode ns = userPgCatalogNamespace();
    TableNode table = userTable(ns.id(), "no_payload", List.of(), Map.of(), Map.of());
    SystemObjectScanContext ctx = contextWithRelations(ns, table);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isInstanceOf(Integer.class);
    assertThat((int) v[0]).isNotZero();
    assertThat(v[1]).isEqualTo("no_payload");
    assertThat(v[2]).isEqualTo(135017987); // default OID geenrated for the namespace
    assertThat(v[3]).isEqualTo("r");
    assertThat(v[4]).isEqualTo(0.0f);
  }

  @Test
  void scan_viewHasRelkind_v() {

    NamespaceNode ns = systemPgCatalogNamespace();
    ViewNode view =
        systemView(
            "my_view",
            Map.of(
                new EngineHintKey("floedb", "1.0", RELATION.type()),
                new EngineHint(
                    RELATION.type(),
                    FloeRelationSpecific.newBuilder()
                        .setOid(2001)
                        .setRelkind("v")
                        .build()
                        .toByteArray())));

    SystemObjectScanContext ctx = contextWithRelations(ns, view);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    assertThat(row.values()[3]).isEqualTo("v");
  }

  @Test
  void scan_oidFallback_isStable() {

    NamespaceNode ns = userPgCatalogNamespace();
    TableNode table = userTable(ns.id(), "stable_oid", List.of(), Map.of(), Map.of());

    SystemObjectScanContext ctx = contextWithRelations(ns, table);

    int oid1 = (int) scanner.scan(ctx).findFirst().orElseThrow().values()[0];
    int oid2 = (int) scanner.scan(ctx).findFirst().orElseThrow().values()[0];

    assertThat(oid1).isEqualTo(oid2);
  }

  private static ViewNode systemView(String name, Map<EngineHintKey, EngineHint> hints) {
    NamespaceNode ns = systemPgCatalogNamespace();
    return new ViewNode(
        PgCatalogTestIds.view(name),
        1,
        Instant.EPOCH,
        catalogId(),
        ns.id(),
        name,
        "",
        "",
        List.of(),
        List.of(),
        List.of(),
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Optional.empty(),
        Map.of(),
        hints);
  }
}
