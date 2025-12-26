package ai.floedb.floecat.extensions.floedb.pgcatalog;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for pg_catalog.pg_type scanner.
 *
 * <p>Validates:
 *
 * <ul>
 *   <li>Engine-specific payload decoding
 *   <li>Fallback behavior when payload is missing
 *   <li>Stable deterministic OIDs
 *   <li>Default pg_catalog namespace semantics
 * </ul>
 */
final class PgTypeScannerTest {

  private final PgTypeScanner scanner = new PgTypeScanner();

  @Test
  void scan_usesEngineSpecificPayload_whenPresent() {
    TypeNode type =
        type(
            "int4",
            Map.of(
                new EngineKey("floedb", "1.0"),
                new EngineHint(
                    "floe.type+proto",
                    FloeTypeSpecific.newBuilder()
                        .setOid(23)
                        .setTypname("int4")
                        .setTypnamespace(11)
                        .setTyplen(4)
                        .setTypbyval(true)
                        .setTyptype("b")
                        .setTypcategory("N")
                        .setTypowner(10)
                        .build()
                        .toByteArray())));

    SystemObjectScanContext ctx = contextWith(type);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isEqualTo(23); // oid
    assertThat(v[1]).isEqualTo("int4"); // typname
    assertThat(v[2]).isEqualTo(11); // typnamespace
    assertThat(v[3]).isEqualTo(4); // typlen
    assertThat(v[4]).isEqualTo(true); // typbyval
    assertThat(v[5]).isEqualTo("b"); // typtype
    assertThat(v[6]).isEqualTo("N"); // typcategory
    assertThat(v[7]).isEqualTo(10); // typowner
  }

  @Test
  void scan_fallsBack_whenPayloadMissing() {
    TypeNode type = type("custom_type", Map.of());

    SystemObjectScanContext ctx = contextWith(type);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isInstanceOf(Integer.class); // oid fallback
    assertThat(v[1]).isEqualTo("custom_type"); // displayName
    assertThat(v[2]).isEqualTo(11); // default pg_catalog namespace
    assertThat(v[3]).isEqualTo(-1); // typlen fallback
    assertThat(v[4]).isEqualTo(false); // typbyval fallback
    assertThat(v[5]).isEqualTo("b"); // typtype fallback
    assertThat(v[6]).isEqualTo("U"); // typcategory fallback
    assertThat(v[7]).isEqualTo(10); // default owner
  }

  @Test
  void scan_oidFallback_isStable() {
    TypeNode type = type("uuid", Map.of());

    SystemObjectScanContext ctx = contextWith(type);

    int oid1 = (int) scanner.scan(ctx).findFirst().orElseThrow().values()[0];
    int oid2 = (int) scanner.scan(ctx).findFirst().orElseThrow().values()[0];

    assertThat(oid1).isEqualTo(oid2);
  }

  @Test
  void scan_nameDoesNotAffectNamespace() {
    TypeNode type = type("pg_catalog.int4", Map.of());

    SystemObjectScanContext ctx = contextWith(type);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[2]).isEqualTo(11); // still pg_catalog
  }

  // ----------------------------------------------------------------------
  // Test fixtures
  // ----------------------------------------------------------------------

  private static SystemObjectScanContext contextWith(TypeNode... types) {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    for (TypeNode t : types) {
      overlay.addNode(t);
    }
    return new SystemObjectScanContext(overlay, null, catalogId());
  }

  private static TypeNode type(String name, Map<EngineKey, EngineHint> engineHints) {

    ResourceId typeId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_TYPE)
            .setId("pg:" + name)
            .build();

    return new TypeNode(
        typeId,
        1,
        Instant.EPOCH,
        "15",
        name,
        "U",
        false,
        ResourceId.getDefaultInstance(),
        engineHints);
  }

  private static ResourceId catalogId() {
    return ResourceId.newBuilder()
        .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_CATALOG)
        .setId("pg")
        .build();
  }
}
