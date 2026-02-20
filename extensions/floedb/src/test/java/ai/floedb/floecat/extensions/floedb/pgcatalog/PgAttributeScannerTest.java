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
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.MissingSystemOidException;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import ai.floedb.floecat.systemcatalog.spi.types.TypeLookup;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import ai.floedb.floecat.types.LogicalType;
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
 *   <li>SYSTEM objects require persisted hints (no fallback)
 *   <li>USER tables may fall back deterministically when relation hints are missing
 * </ul>
 */
final class PgAttributeScannerTest {

  private static final EngineKey ENGINE = new EngineKey("floedb", "1.0");
  private static final EngineContext ENGINE_CTX = EngineContext.of("floedb", "1.0");

  private final PgAttributeScanner scanner = new PgAttributeScanner(new TestTypeMapper());

  @Test
  void scan_resolvesType_andEmitsCorrectAttributeRow() {

    TypeNode int4 =
        systemType(
            "pg_catalog.int4",
            Map.of(
                new EngineHintKey(ENGINE.engineKind(), ENGINE.engineVersion(), TYPE.type()),
                new EngineHint(
                    TYPE.type(),
                    FloeTypeSpecific.newBuilder()
                        .setOid(23)
                        .setTyplen(4)
                        .setTypalign("i")
                        .setTypbyval(true)
                        .build()
                        .toByteArray())));

    TypeNode numeric =
        systemType(
            "pg_catalog.numeric",
            Map.of(
                new EngineHintKey(ENGINE.engineKind(), ENGINE.engineVersion(), TYPE.type()),
                new EngineHint(
                    TYPE.type(),
                    FloeTypeSpecific.newBuilder()
                        .setOid(1700)
                        .setTyplen(-1)
                        .setTypalign("d")
                        .setTypbyval(false)
                        .build()
                        .toByteArray())));

    TableNode table =
        systemTable(
            systemPgCatalogNamespace().id(),
            "t",
            List.of(),
            Map.of(),
            Map.of(
                new EngineHintKey(ENGINE.engineKind(), ENGINE.engineVersion(), RELATION.type()),
                new EngineHint(
                    RELATION.type(),
                    FloeRelationSpecific.newBuilder().setOid(100).build().toByteArray())));

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

    SystemObjectScanContext ctx =
        systemCatalogContext(
            systemPgCatalogNamespace(), List.of(int4, numeric), List.of(table), schema);

    List<SystemObjectRow> rows = scanner.scan(ctx).toList();

    Object[] v0 = rows.get(0).values();
    assertThat(v0[0]).isEqualTo(100); // attrelid
    assertThat(v0[1]).isEqualTo("id");
    assertThat(v0[2]).isEqualTo(23); // atttypid
    assertThat(v0[3]).isEqualTo(-1);
    assertThat(v0[4]).isEqualTo(1);
    assertThat(v0[5]).isEqualTo(4);
    assertThat(v0[6]).isEqualTo(true);
    assertThat(v0[7]).isEqualTo(true);
    assertThat(v0[8]).isEqualTo(false);
    assertThat(v0[9]).isEqualTo("i");
    assertThat(v0[10]).isEqualTo("p");
    assertThat(v0[11]).isEqualTo(0);
    assertThat(v0[12]).isEqualTo(0);

    Object[] v1 = rows.get(1).values();
    assertThat(v1[0]).isEqualTo(100);
    assertThat(v1[1]).isEqualTo("amount");
    assertThat(v1[2]).isEqualTo(1700);
    assertThat(v1[3]).isEqualTo((10 << 16) | 2);
    assertThat(v1[4]).isEqualTo(2);
    assertThat(v1[5]).isEqualTo(-1);
    assertThat(v1[6]).isEqualTo(false);
    assertThat(v1[7]).isEqualTo(false);
    assertThat(v1[8]).isEqualTo(false);
    assertThat(v1[9]).isEqualTo("d");
    assertThat(v1[10]).isEqualTo("p");
    assertThat(v1[11]).isEqualTo(0);
    assertThat(v1[12]).isEqualTo(0);
  }

  @Test
  void scan_throws_whenSystemHintsMissing() {
    TypeNode int4 =
        systemType(
            "pg_catalog.int4",
            Map.of(
                new EngineHintKey(ENGINE.engineKind(), ENGINE.engineVersion(), TYPE.type()),
                new EngineHint(
                    TYPE.type(), FloeTypeSpecific.newBuilder().setOid(23).build().toByteArray())));
    TableNode table =
        systemTable(systemPgCatalogNamespace().id(), "t", List.of(), Map.of(), Map.of());

    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("x")
                .setLogicalType("INT32")
                .setNullable(true)
                .build());

    SystemObjectScanContext ctx =
        systemCatalogContext(systemPgCatalogNamespace(), List.of(int4), List.of(table), schema);

    assertThatThrownBy(() -> scanner.scan(ctx).findFirst().orElseThrow())
        .isInstanceOf(MissingSystemOidException.class);
  }

  @Test
  void scan_fallsBack_whenUserTableRelationHintsMissing() {
    // USER table exists; USER types do not. So: USER table + SYSTEM type (with hints).

    TypeNode int4 =
        systemType(
            "pg_catalog.int4",
            Map.of(
                new EngineHintKey(ENGINE.engineKind(), ENGINE.engineVersion(), TYPE.type()),
                new EngineHint(
                    TYPE.type(),
                    FloeTypeSpecific.newBuilder()
                        .setOid(23)
                        .setTyplen(4)
                        .setTypalign("i")
                        .setTypbyval(true)
                        .build()
                        .toByteArray())));

    NamespaceNode userNs = userNamespace("public", Map.of());
    TableNode userTable =
        userTable(
            userNs.id(),
            "t_user",
            List.of(),
            Map.of(),
            Map.of()); // user table without relation hint

    TestCatalogOverlay overlay = new TestCatalogOverlay();
    overlay.addNode(userNs);
    overlay.addRelation(userNs.id(), userTable);
    overlay.setTableSchema(
        userTable.id(),
        List.of(
            SchemaColumn.newBuilder()
                .setName("x")
                .setLogicalType("INT32")
                .setNullable(true)
                .build()));

    NamespaceNode pgCatalog = systemPgCatalogNamespace();
    overlay.addNode(pgCatalog);
    overlay.addType(pgCatalog.id(), int4);

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            overlay, null, userCatalogId(), ENGINE_CTX); // user catalog for fallback branch

    Object[] v = scanner.scan(ctx).findFirst().orElseThrow().values();

    // attrelid should be fallback (USER table => not SYSTEM id)
    assertThat(v[0]).isInstanceOf(Integer.class);
    assertThat((int) v[0]).isNotZero();

    // type oid should still resolve from SYSTEM int4 hints
    assertThat(v[2]).isEqualTo(23);
  }

  // ----------------------------------------------------------------------
  // Fixtures
  // ----------------------------------------------------------------------

  private static class TestTypeMapper implements EngineTypeMapper {
    @Override
    public Optional<TypeNode> resolve(LogicalType t, TypeLookup lookup) {
      if ("INT32".equals(t.kind().name())) {
        return lookup.findByName("pg_catalog", "int4");
      }
      if ("DECIMAL".equals(t.kind().name())) {
        return lookup.findByName("pg_catalog", "numeric");
      }
      return Optional.empty();
    }
  }
}
