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
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
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
 *   <li>Fallback behavior when payload is missing
 *   <li>Stable deterministic OIDs
 *   <li>relkind derivation (table vs view)
 * </ul>
 */
final class PgClassScannerTest {

  private final PgClassScanner scanner = new PgClassScanner();
  private static final EngineContext ENGINE_CTX = EngineContext.of("floedb", "1.0");

  @Test
  void scan_usesEngineSpecificPayload_whenPresent() {

    TableNode table =
        table(
            "my_table",
            Map.of(
                new EngineHintKey("floedb", "1.0", FloePayloads.RELATION.type()),
                new EngineHint(
                    "floe.relation+proto",
                    FloeRelationSpecific.newBuilder()
                        .setOid(1001)
                        .setRelname("my_table")
                        .setRelnamespace(11)
                        .setRelkind("r")
                        .setReltuples(42)
                        .build()
                        .toByteArray())));

    SystemObjectScanContext ctx = contextWith(table);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isEqualTo(1001); // oid
    assertThat(v[1]).isEqualTo("my_table"); // relname
    assertThat(v[2]).isEqualTo(11); // relnamespace
    assertThat(v[3]).isEqualTo("r"); // relkind
    assertThat(v[4]).isEqualTo(42.0f); // reltuples
  }

  @Test
  void scan_fallsBack_whenPayloadMissing() {

    TableNode table = table("no_payload", Map.of());

    SystemObjectScanContext ctx = contextWith(table);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isInstanceOf(Integer.class); // oid fallback
    assertThat(v[1]).isEqualTo("no_payload"); // displayName
    assertThat(v[2]).isEqualTo(11); // default pg_catalog namespace
    assertThat(v[3]).isEqualTo("r"); // default table relkind
    assertThat(v[4]).isEqualTo(0.0f); // reltuples fallback
  }

  @Test
  void scan_viewHasRelkind_v() {

    ViewNode view = view("my_view", Map.of());

    SystemObjectScanContext ctx = contextWith(view);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[3]).isEqualTo("v"); // relkind
  }

  @Test
  void scan_oidFallback_isStable() {

    TableNode table = table("stable_oid", Map.of());

    SystemObjectScanContext ctx = contextWith(table);

    int oid1 = (int) scanner.scan(ctx).findFirst().orElseThrow().values()[0];
    int oid2 = (int) scanner.scan(ctx).findFirst().orElseThrow().values()[0];

    assertThat(oid1).isEqualTo(oid2);
  }

  // ----------------------------------------------------------------------
  // Test fixtures
  // ----------------------------------------------------------------------

  private static NamespaceNode pgCatalogNamespace() {
    return new NamespaceNode(
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("pg_catalog")
            .build(),
        1,
        Instant.EPOCH,
        catalogId(),
        List.of(),
        "pg_catalog",
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Optional.empty(),
        Map.of());
  }

  private static SystemObjectScanContext contextWith(GraphNode... nodes) {
    TestCatalogOverlay overlay = new TestCatalogOverlay();

    NamespaceNode ns = pgCatalogNamespace();
    overlay.addNode(ns);

    for (GraphNode n : nodes) {
      overlay.addRelation(ns.id(), n);
    }

    return new SystemObjectScanContext(overlay, null, catalogId(), ENGINE_CTX);
  }

  private static TableNode table(String name, Map<EngineHintKey, EngineHint> hints) {

    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_TABLE)
            .setId("pg:" + name)
            .build();

    return new SystemTableNode(
        id,
        1,
        Instant.EPOCH,
        "15",
        name,
        pgCatalogNamespace().id(),
        List.of(),
        "scanner_id",
        hints);
  }

  private static ViewNode view(String name, Map<EngineHintKey, EngineHint> hints) {

    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_VIEW)
            .setId("pg:" + name)
            .build();

    return new ViewNode(
        id,
        1,
        Instant.EPOCH,
        catalogId(),
        pgCatalogNamespace().id(),
        name,
        "",
        "",
        List.of(),
        List.of(),
        List.of(),
        Map.of(),
        Optional.empty(),
        hints);
  }

  private static ResourceId catalogId() {
    return ResourceId.newBuilder()
        .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_CATALOG)
        .setId("pg")
        .build();
  }
}
