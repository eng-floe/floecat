package ai.floedb.floecat.extensions.floedb.pgcatalog;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for pg_catalog.pg_namespace scanner.
 *
 * <p>Validates: - Engine-specific payload decoding - Fallback behavior when payload is missing -
 * Stable deterministic OIDs
 */
final class PgNamespaceScannerTest {

  private final PgNamespaceScanner scanner = new PgNamespaceScanner();

  @Test
  void scan_usesEngineSpecificPayload_whenPresent() {
    NamespaceNode pgCatalog =
        namespace(
            "pg_catalog",
            Map.of(
                new EngineKey("floedb", "1.0"),
                new EngineHint(
                    "floe.namespace+proto",
                    FloeNamespaceSpecific.newBuilder()
                        .setOid(11)
                        .setNspname("pg_catalog")
                        .setNspowner(10)
                        .addNspacl("=UC/postgres")
                        .build()
                        .toByteArray())));

    SystemObjectScanContext ctx = contextWith(pgCatalog);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isEqualTo(11); // oid
    assertThat(v[1]).isEqualTo("pg_catalog"); // nspname
    assertThat(v[2]).isEqualTo(10); // nspowner
    assertThat((String[]) v[3]).containsExactly("=UC/postgres"); // nspacl
  }

  @Test
  void scan_fallsBack_whenPayloadMissing() {
    NamespaceNode ns = namespace("public", Map.of());

    SystemObjectScanContext ctx = contextWith(ns);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isInstanceOf(Integer.class); // oid fallback
    assertThat(v[1]).isEqualTo("public"); // displayName
    assertThat(v[2]).isEqualTo(10); // default owner
    assertThat((String[]) v[3]).isEmpty(); // empty ACL
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

    return new SystemObjectScanContext(overlay, null, catalogId());
  }

  private static NamespaceNode namespace(String name, Map<EngineKey, EngineHint> engineHints) {

    ResourceId nsId =
        ResourceId.newBuilder()
            .setAccountId("_system")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("pg:" + name)
            .build();

    return new NamespaceNode(
        nsId,
        1,
        Instant.EPOCH,
        catalogId(),
        List.of(),
        name,
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Optional.empty(),
        engineHints);
  }

  private static ResourceId catalogId() {
    return ResourceId.newBuilder()
        .setAccountId("_system")
        .setKind(ResourceKind.RK_CATALOG)
        .setId("pg")
        .build();
  }
}
