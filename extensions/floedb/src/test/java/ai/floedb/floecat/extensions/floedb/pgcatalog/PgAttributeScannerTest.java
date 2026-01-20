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
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import ai.floedb.floecat.systemcatalog.spi.types.TypeLookup;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
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

  private static final EngineContext ENGINE_CTX = EngineContext.of("floedb", "1.0");

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
                        .setTypbyval(true)
                        .build()
                        .toByteArray())));
    TypeNode numeric =
        typeNode(
            NameRef.newBuilder().addPath("pg_catalog").setName("numeric").build(),
            Map.of(
                ENGINE,
                new EngineHint(
                    FloePayloads.TYPE.type(),
                    FloeTypeSpecific.newBuilder()
                        .setOid(1700)
                        .setTyplen(-1)
                        .setTypalign("d")
                        .setTypbyval(false)
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
                .build(),
            SchemaColumn.newBuilder()
                .setName("amount")
                .setLogicalType("DECIMAL(10,2)")
                .setNullable(true)
                .build());

    SystemObjectScanContext ctx = contextWith(List.of(int4, numeric), List.of(table), schema);

    List<SystemObjectRow> rows = scanner.scan(ctx).toList();

    Object[] v0 = rows.get(0).values();

    assertThat(v0[0]).isEqualTo(100); // attrelid
    assertThat(v0[1]).isEqualTo("id"); // attname
    assertThat(v0[2]).isEqualTo(23); // atttypid (pg_type.oid)
    assertThat(v0[3]).isEqualTo(-1); // atttypmod
    assertThat(v0[4]).isEqualTo(1); // attnum
    assertThat(v0[5]).isEqualTo(4); // attlen
    assertThat(v0[6]).isEqualTo(true); // attbyval
    assertThat(v0[7]).isEqualTo(true); // attnotnull
    assertThat(v0[8]).isEqualTo(false); // attisdropped
    assertThat(v0[9]).isEqualTo("i"); // attalign
    assertThat(v0[10]).isEqualTo("p"); // attstorage
    assertThat(v0[11]).isEqualTo(0); // attndims
    assertThat(v0[12]).isEqualTo(0); // attcollation

    Object[] v1 = rows.get(1).values();

    assertThat(v1[0]).isEqualTo(100); // attrelid
    assertThat(v1[1]).isEqualTo("amount"); // attname
    assertThat(v1[2]).isEqualTo(1700); // atttypid (pg_catalog.numeric oid)
    assertThat(v1[3]).isEqualTo((10 << 16) | 2); // atttypmod (precision and scale)
    assertThat(v1[4]).isEqualTo(2); // attnum
    assertThat(v1[5]).isEqualTo(-1); // attlen (varlena)
    assertThat(v1[6]).isEqualTo(false); // attbyval
    assertThat(v1[7]).isEqualTo(false); // attnotnull
    assertThat(v1[8]).isEqualTo(false); // attisdropped
    assertThat(v1[9]).isEqualTo("d"); // attalign
    assertThat(v1[10]).isEqualTo("p"); // attstorage
    assertThat(v1[11]).isEqualTo(0); // attndims
    assertThat(v1[12]).isEqualTo(0); // attcollation
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

    SystemObjectScanContext ctx = contextWith(List.of(int4), List.of(table), schema);

    Object[] v = scanner.scan(ctx).findFirst().orElseThrow().values();
    int expectedRelOid =
        ScannerUtils.oid(ctx, table.id(), FloePayloads.RELATION, FloeRelationSpecific::getOid);

    assertThat(v[0]).isEqualTo(expectedRelOid); // attrelid falls back deterministically
    assertThat(v[1]).isEqualTo("x"); // attname
    assertThat(v[2]).isInstanceOf(Integer.class); // atttypid fallback
    assertThat(v[3]).isEqualTo(-1); // atttypmod fallback
    assertThat(v[4]).isEqualTo(1); // attnum (position 1)
    assertThat(v[5]).isEqualTo(-1); // attlen fallback
    assertThat(v[6]).isEqualTo(true); // attbyval default
    assertThat(v[7]).isEqualTo(false); // nullable → not not-null
    assertThat(v[8]).isEqualTo(false); // attisdropped default
    assertThat(v[9]).isEqualTo("i"); // attalign default
    assertThat(v[10]).isEqualTo("p"); // attstorage default
    assertThat(v[11]).isEqualTo(0); // attndims default
    assertThat(v[12]).isEqualTo(0); // attcollation default
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
      if (t.kind().name().equals("DECIMAL")) {
        return lookup.findByName("pg_catalog", "numeric");
      }
      return Optional.empty();
    }
  }

  private static SystemObjectScanContext contextWith(
      List<TypeNode> types, List<TableNode> tables, List<SchemaColumn> schema) {

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

    for (TypeNode type : types) {
      overlay.addType(pg.id(), type);
    }
    for (TableNode table : tables) {
      overlay.addRelation(pg.id(), table);
      overlay.setTableSchema(table.id(), schema);
    }

    return new SystemObjectScanContext(overlay, null, catalogId(), ENGINE_CTX);
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
