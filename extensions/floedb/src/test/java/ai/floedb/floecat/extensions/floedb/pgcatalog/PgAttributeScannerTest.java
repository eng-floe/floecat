package ai.floedb.floecat.extensions.floedb.pgcatalog;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import ai.floedb.floecat.systemcatalog.spi.types.TypeLookup;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import ai.floedb.floecat.types.LogicalType;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for pg_catalog.pg_attribute scanner.
 *
 * <p>Validates:
 *
 * <ul>
 *   <li>Logical → TypeNode resolution via EngineTypeMapper
 *   <li>atttypid matches pg_type.oid
 *   <li>nullability mapping
 *   <li>ordinal position stability
 *   <li>payload fallback behavior
 * </ul>
 */
final class PgAttributeScannerTest {

  private static final EngineKey ENGINE = new EngineKey("floedb", "1.0");

  private final PgAttributeScanner scanner = new PgAttributeScanner(new TestTypeMapper());

  @Test
  void scan_resolvesType_andEmitsCorrectAttributeRow() {

    // --- type (pg_type)
    TypeNode int4 =
        typeNode(
            NameRef.newBuilder().addPath("pg_catalog").setName("int4").build(),
            Map.of(
                ENGINE,
                new EngineHint(
                    FloePayloads.TYPE.type(),
                    FloeTypeSpecific.newBuilder()
                        .setOid(23)
                        .setTyplen(4)
                        .setTypalign("i")
                        .setTypstorage("p")
                        .setTypndims(0)
                        .setTypcollation(0)
                        .build()
                        .toByteArray())));

    // --- table
    TableNode table =
        table(
            "t",
            Map.of(
                ENGINE,
                new EngineHint(
                    FloePayloads.RELATION.type(),
                    FloeRelationSpecific.newBuilder().setOid(100).build().toByteArray())));

    // --- schema
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("id")
                .setLogicalType("INT32")
                .setNullable(false)
                .build());

    SystemObjectScanContext ctx = contextWith(int4, table, schema);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isEqualTo(100); // attrelid
    assertThat(v[1]).isEqualTo("id"); // attname
    assertThat(v[2]).isEqualTo(23); // atttypid (pg_type.oid)
    assertThat(v[3]).isEqualTo(1); // attnum
    assertThat(v[4]).isEqualTo(4); // attlen
    assertThat(v[5]).isEqualTo(true); // attnotnull
    assertThat(v[6]).isEqualTo(false); // attisdropped
    assertThat(v[7]).isEqualTo("i"); // attalign
    assertThat(v[8]).isEqualTo("p"); // attstorage
    assertThat(v[9]).isEqualTo(0); // attndims
    assertThat(v[10]).isEqualTo(0); // attcollation
  }

  @Test
  void scan_fallsBack_whenTypePayloadMissing() {

    TypeNode int4 =
        typeNode(NameRef.newBuilder().addPath("pg_catalog").setName("int4").build(), Map.of());
    TableNode table = table("t", Map.of());

    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("x")
                .setLogicalType("INT32")
                .setNullable(true)
                .build());

    SystemObjectScanContext ctx = contextWith(int4, table, schema);

    Object[] v = scanner.scan(ctx).findFirst().orElseThrow().values();

    assertThat(v[2]).isInstanceOf(Integer.class); // deterministic fallback OID
    assertThat(v[5]).isEqualTo(false); // nullable → not not-null
    assertThat(v[4]).isEqualTo(-1); // attlen fallback
  }

  // ----------------------------------------------------------------------
  // Fixtures
  // ----------------------------------------------------------------------

  private static class TestTypeMapper implements EngineTypeMapper {
    @Override
    public Optional<TypeNode> resolve(LogicalType t, TypeLookup lookup) {
      if (t.kind().name().equals("INT32")) {
        return lookup.findByName("pg_catalog", "int4");
      }
      return Optional.empty();
    }
  }

  private static SystemObjectScanContext contextWith(
      TypeNode type, TableNode table, List<SchemaColumn> schema) {

    TestCatalogOverlay overlay = new TestCatalogOverlay();

    NamespaceNode pg =
        new NamespaceNode(
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

    overlay.addNode(pg);
    overlay.addType(pg.id(), type);
    overlay.addRelation(pg.id(), table);
    overlay.setTableSchema(table.id(), schema);

    return new SystemObjectScanContext(overlay, null, catalogId());
  }

  private static TypeNode typeNode(NameRef name, Map<EngineKey, EngineHint> hints) {
    return new TypeNode(
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_TYPE)
            .setId("pg:" + SystemNodeRegistry.safeName(name))
            .build(),
        1,
        Instant.EPOCH,
        "15",
        SystemNodeRegistry.safeName(name),
        "N",
        false,
        null,
        hints);
  }

  private static TableNode table(String name, Map<EngineKey, EngineHint> hints) {
    return new SystemTableNode(
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_TABLE)
            .setId("pg:" + name)
            .build(),
        1,
        Instant.EPOCH,
        "15",
        name,
        pgCatalogNamespace().id(),
        List.of(),
        "scanner",
        hints);
  }

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

  private static ResourceId catalogId() {
    return ResourceId.newBuilder()
        .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_CATALOG)
        .setId("pg")
        .build();
  }
}
