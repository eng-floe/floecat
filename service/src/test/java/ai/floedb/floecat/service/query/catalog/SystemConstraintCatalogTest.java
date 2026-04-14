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

package ai.floedb.floecat.service.query.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintEnforcement;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SystemConstraintCatalogTest {

  @Test
  void lookupBySystemRelationIdUsesPbtxtDeclaredConstraints() {
    SystemNodeRegistry registry = mock(SystemNodeRegistry.class);
    ResourceId tableId = tableId();
    when(registry.nodesFor(any(EngineContext.class))).thenReturn(builtinNodes(tableId));

    SystemConstraintCatalog catalog = new SystemConstraintCatalog(registry);
    var view = catalog.constraints(EngineContext.of("floedb", "1.0"), tableId).orElseThrow();

    assertEquals(tableId, view.relationId());
    assertEquals(1, view.constraints().size());
    assertEquals(ConstraintType.CT_NOT_NULL, view.constraints().get(0).getType());
    assertEquals("tables_table_name_nn", view.constraints().get(0).getName());
    assertEquals("floedb", view.properties().get("engine_kind"));
  }

  @Test
  void lookupSynthesizesNotNullFromNonNullableColumnsWhenNotExplicitlyDeclared() {
    SystemNodeRegistry registry = mock(SystemNodeRegistry.class);
    ResourceId tableId = tableId();
    when(registry.nodesFor(any(EngineContext.class)))
        .thenReturn(builtinNodesWithoutExplicitConstraint(tableId));

    SystemConstraintCatalog catalog = new SystemConstraintCatalog(registry);
    var view = catalog.constraints(EngineContext.of("floedb", "1.0"), tableId).orElseThrow();

    assertEquals(1, view.constraints().size());
    assertEquals(ConstraintType.CT_NOT_NULL, view.constraints().get(0).getType());
    assertEquals("nn_10", view.constraints().get(0).getName());
    assertEquals(ConstraintEnforcement.CE_ENFORCED, view.constraints().get(0).getEnforcement());
  }

  @Test
  void lookupDedupesExplicitAndImplicitNotNullForSameColumn() {
    SystemNodeRegistry registry = mock(SystemNodeRegistry.class);
    ResourceId tableId = tableId();
    when(registry.nodesFor(any(EngineContext.class))).thenReturn(builtinNodes(tableId));

    SystemConstraintCatalog catalog = new SystemConstraintCatalog(registry);
    var view = catalog.constraints(EngineContext.of("floedb", "1.0"), tableId).orElseThrow();

    assertEquals(1, view.constraints().size());
    assertEquals("tables_table_name_nn", view.constraints().get(0).getName());
  }

  @Test
  void lookupDedupesExplicitAndImplicitNotNullWhenExplicitKeyedByColumnId() {
    SystemNodeRegistry registry = mock(SystemNodeRegistry.class);
    ResourceId tableId = tableId();
    when(registry.nodesFor(any(EngineContext.class)))
        .thenReturn(
            builtinNodesWithColumnsAndConstraints(
                tableId,
                List.of(
                    new SystemColumnDef(
                        "c1",
                        NameRef.newBuilder().setName("VARCHAR").build(),
                        false,
                        1,
                        10L,
                        List.of())),
                List.of(
                    ConstraintDefinition.newBuilder()
                        .setName("nn_c1_explicit_id")
                        .setType(ConstraintType.CT_NOT_NULL)
                        .addColumns(
                            ConstraintColumnRef.newBuilder().setColumnId(10L).setOrdinal(1).build())
                        .build())));

    SystemConstraintCatalog catalog = new SystemConstraintCatalog(registry);
    var view = catalog.constraints(EngineContext.of("floedb", "1.0"), tableId).orElseThrow();

    assertEquals(1, view.constraints().size());
    assertEquals("nn_c1_explicit_id", view.constraints().get(0).getName());
  }

  @Test
  void lookupDedupesExplicitAndImplicitNotNullWhenExplicitKeyedByColumnName() {
    SystemNodeRegistry registry = mock(SystemNodeRegistry.class);
    ResourceId tableId = tableId();
    when(registry.nodesFor(any(EngineContext.class)))
        .thenReturn(
            builtinNodesWithColumnsAndConstraints(
                tableId,
                List.of(
                    new SystemColumnDef(
                        "c1",
                        NameRef.newBuilder().setName("VARCHAR").build(),
                        false,
                        1,
                        null,
                        List.of())),
                List.of(
                    ConstraintDefinition.newBuilder()
                        .setName("nn_c1_explicit_name")
                        .setType(ConstraintType.CT_NOT_NULL)
                        .addColumns(
                            ConstraintColumnRef.newBuilder()
                                .setColumnName("c1")
                                .setOrdinal(1)
                                .build())
                        .build())));

    SystemConstraintCatalog catalog = new SystemConstraintCatalog(registry);
    var view = catalog.constraints(EngineContext.of("floedb", "1.0"), tableId).orElseThrow();

    assertEquals(1, view.constraints().size());
    assertEquals("nn_c1_explicit_name", view.constraints().get(0).getName());
  }

  @Test
  void lookupMalformedExplicitNotNullStillSynthesizesImplicitNotNull() {
    SystemNodeRegistry registry = mock(SystemNodeRegistry.class);
    ResourceId tableId = tableId();
    when(registry.nodesFor(any(EngineContext.class)))
        .thenReturn(
            builtinNodesWithColumnsAndConstraints(
                tableId,
                List.of(
                    new SystemColumnDef(
                        "c1",
                        NameRef.newBuilder().setName("VARCHAR").build(),
                        false,
                        1,
                        10L,
                        List.of())),
                List.of(
                    ConstraintDefinition.newBuilder()
                        .setName("nn_malformed")
                        .setType(ConstraintType.CT_NOT_NULL)
                        .addColumns(ConstraintColumnRef.newBuilder().setOrdinal(1).build())
                        .build())));

    SystemConstraintCatalog catalog = new SystemConstraintCatalog(registry);
    var view = catalog.constraints(EngineContext.of("floedb", "1.0"), tableId).orElseThrow();

    assertEquals(2, view.constraints().size());
    assertEquals("nn_malformed", view.constraints().get(0).getName());
    assertEquals("nn_10", view.constraints().get(1).getName());
  }

  @Test
  void lookupSynthesizesImplicitNotNullForEachNonNullableColumn() {
    SystemNodeRegistry registry = mock(SystemNodeRegistry.class);
    ResourceId tableId = tableId();
    when(registry.nodesFor(any(EngineContext.class)))
        .thenReturn(
            builtinNodesWithColumnsAndConstraints(
                tableId,
                List.of(
                    new SystemColumnDef(
                        "c1",
                        NameRef.newBuilder().setName("VARCHAR").build(),
                        false,
                        1,
                        1L,
                        List.of()),
                    new SystemColumnDef(
                        "c2",
                        NameRef.newBuilder().setName("VARCHAR").build(),
                        false,
                        2,
                        2L,
                        List.of()),
                    new SystemColumnDef(
                        "c3",
                        NameRef.newBuilder().setName("VARCHAR").build(),
                        true,
                        3,
                        3L,
                        List.of())),
                List.of()));

    SystemConstraintCatalog catalog = new SystemConstraintCatalog(registry);
    var view = catalog.constraints(EngineContext.of("floedb", "1.0"), tableId).orElseThrow();

    assertEquals(2, view.constraints().size());
    assertEquals("nn_1", view.constraints().get(0).getName());
    assertEquals("nn_2", view.constraints().get(1).getName());
  }

  @Test
  void lookupPreservesExplicitOrderAndAppendsMissingImplicitNotNulls() {
    SystemNodeRegistry registry = mock(SystemNodeRegistry.class);
    ResourceId tableId = tableId();
    when(registry.nodesFor(any(EngineContext.class))).thenReturn(builtinNodesWithOrder(tableId));

    SystemConstraintCatalog catalog = new SystemConstraintCatalog(registry);
    var view = catalog.constraints(EngineContext.of("floedb", "1.0"), tableId).orElseThrow();

    assertEquals(4, view.constraints().size());
    assertEquals("pk_tables", view.constraints().get(0).getName());
    assertEquals("nn_c2_explicit", view.constraints().get(1).getName());
    assertEquals("ck_tables", view.constraints().get(2).getName());
    assertEquals("nn_1", view.constraints().get(3).getName());
  }

  @Test
  void lookupNormalizesSystemMarkerIdsAcrossAccounts() {
    SystemNodeRegistry registry = mock(SystemNodeRegistry.class);
    ResourceId systemId = tableId();
    when(registry.nodesFor(any(EngineContext.class))).thenReturn(builtinNodes(systemId));

    SystemConstraintCatalog catalog = new SystemConstraintCatalog(registry);
    ResourceId userAccountAlias = systemId.toBuilder().setAccountId("acct").build();

    var view =
        catalog.constraints(EngineContext.of("floedb", "1.0"), userAccountAlias).orElseThrow();
    assertEquals(systemId, view.relationId());
  }

  @Test
  void catalogIsCachedPerEngineVersionKey() {
    SystemNodeRegistry registry = mock(SystemNodeRegistry.class);
    ResourceId tableId = tableId();
    when(registry.nodesFor(any(EngineContext.class))).thenReturn(builtinNodes(tableId));

    SystemConstraintCatalog catalog = new SystemConstraintCatalog(registry);
    assertTrue(catalog.constraints(EngineContext.of("floedb", "1.0"), tableId).isPresent());
    assertTrue(catalog.constraints(EngineContext.of("floedb", "1.0"), tableId).isPresent());

    verify(registry, times(1)).nodesFor(any(EngineContext.class));
  }

  private static SystemNodeRegistry.BuiltinNodes builtinNodes(ResourceId tableId) {
    ConstraintDefinition tableNameNotNull =
        ConstraintDefinition.newBuilder()
            .setName("tables_table_name_nn")
            .setType(ConstraintType.CT_NOT_NULL)
            .addColumns(
                ConstraintColumnRef.newBuilder()
                    .setColumnId(10L)
                    .setColumnName("table_name")
                    .setOrdinal(1)
                    .build())
            .build();
    SystemTableDef tableDef =
        new SystemTableDef(
            NameRef.newBuilder().addPath("information_schema").setName("tables").build(),
            "tables",
            List.of(
                new SystemColumnDef(
                    "table_name",
                    NameRef.newBuilder().setName("VARCHAR").build(),
                    false,
                    1,
                    10L,
                    List.of()),
                new SystemColumnDef(
                    "table_catalog",
                    NameRef.newBuilder().setName("VARCHAR").build(),
                    true,
                    2,
                    11L,
                    List.of())),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "tables_scanner",
            "",
            "",
            List.of(),
            null,
            List.of(tableNameNotNull));
    return new SystemNodeRegistry.BuiltinNodes(
        "floedb",
        "1.0",
        "fingerprint",
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        Map.of(),
        Map.of(),
        Map.of("information_schema.tables", tableId),
        Map.of(),
        Map.of(),
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(tableDef),
            List.of(),
            List.of()));
  }

  private static ResourceId tableId() {
    return SystemNodeRegistry.resourceId(
        "floedb",
        ResourceKind.RK_TABLE,
        NameRef.newBuilder()
            .setCatalog("floedb")
            .addPath("information_schema")
            .setName("tables")
            .build());
  }

  private static SystemNodeRegistry.BuiltinNodes builtinNodesWithoutExplicitConstraint(
      ResourceId tableId) {
    ResourceId namespaceId =
        SystemNodeRegistry.resourceId(
            "floedb",
            ResourceKind.RK_NAMESPACE,
            NameRef.newBuilder().setCatalog("floedb").setName("information_schema").build());
    SystemTableDef tableDef =
        new SystemTableDef(
            NameRef.newBuilder().addPath("information_schema").setName("tables").build(),
            "tables",
            List.of(
                new SystemColumnDef(
                    "table_name",
                    NameRef.newBuilder().setName("VARCHAR").build(),
                    false,
                    1,
                    10L,
                    List.of())),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "tables_scanner",
            "",
            "",
            List.of(),
            null,
            List.of());
    return new SystemNodeRegistry.BuiltinNodes(
        "floedb",
        "1.0",
        "fingerprint",
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        Map.of(),
        Map.of(),
        Map.of("information_schema.tables", tableId),
        Map.of(),
        Map.of("information_schema", namespaceId),
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(tableDef),
            List.of(),
            List.of()));
  }

  private static SystemNodeRegistry.BuiltinNodes builtinNodesWithOrder(ResourceId tableId) {
    ResourceId namespaceId =
        SystemNodeRegistry.resourceId(
            "floedb",
            ResourceKind.RK_NAMESPACE,
            NameRef.newBuilder().setCatalog("floedb").setName("information_schema").build());
    ConstraintDefinition pk =
        ConstraintDefinition.newBuilder()
            .setName("pk_tables")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .addColumns(
                ConstraintColumnRef.newBuilder()
                    .setColumnId(1L)
                    .setColumnName("c1")
                    .setOrdinal(1)
                    .build())
            .build();
    ConstraintDefinition explicitNotNullC2 =
        ConstraintDefinition.newBuilder()
            .setName("nn_c2_explicit")
            .setType(ConstraintType.CT_NOT_NULL)
            .addColumns(
                ConstraintColumnRef.newBuilder()
                    .setColumnId(2L)
                    .setColumnName("c2")
                    .setOrdinal(1)
                    .build())
            .build();
    ConstraintDefinition check =
        ConstraintDefinition.newBuilder()
            .setName("ck_tables")
            .setType(ConstraintType.CT_CHECK)
            .setCheckExpression("c1 > 0")
            .build();
    SystemTableDef tableDef =
        new SystemTableDef(
            NameRef.newBuilder().addPath("information_schema").setName("tables").build(),
            "tables",
            List.of(
                new SystemColumnDef(
                    "c1", NameRef.newBuilder().setName("VARCHAR").build(), false, 1, 1L, List.of()),
                new SystemColumnDef(
                    "c2",
                    NameRef.newBuilder().setName("VARCHAR").build(),
                    false,
                    2,
                    2L,
                    List.of())),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "tables_scanner",
            "",
            "",
            List.of(),
            null,
            List.of(pk, explicitNotNullC2, check));
    return new SystemNodeRegistry.BuiltinNodes(
        "floedb",
        "1.0",
        "fingerprint",
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        Map.of(),
        Map.of(),
        Map.of("information_schema.tables", tableId),
        Map.of(),
        Map.of("information_schema", namespaceId),
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(tableDef),
            List.of(),
            List.of()));
  }

  private static SystemNodeRegistry.BuiltinNodes builtinNodesWithColumnsAndConstraints(
      ResourceId tableId, List<SystemColumnDef> columns, List<ConstraintDefinition> constraints) {
    ResourceId namespaceId =
        SystemNodeRegistry.resourceId(
            "floedb",
            ResourceKind.RK_NAMESPACE,
            NameRef.newBuilder().setCatalog("floedb").setName("information_schema").build());
    SystemTableDef tableDef =
        new SystemTableDef(
            NameRef.newBuilder().addPath("information_schema").setName("tables").build(),
            "tables",
            columns,
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "tables_scanner",
            "",
            "",
            List.of(),
            null,
            constraints);
    return new SystemNodeRegistry.BuiltinNodes(
        "floedb",
        "1.0",
        "fingerprint",
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        Map.of(),
        Map.of(),
        Map.of("information_schema.tables", tableId),
        Map.of(),
        Map.of("information_schema", namespaceId),
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(tableDef),
            List.of(),
            List.of()));
  }
}
