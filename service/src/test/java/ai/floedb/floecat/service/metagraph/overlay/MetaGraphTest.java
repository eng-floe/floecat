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

package ai.floedb.floecat.service.metagraph.overlay;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.metagraph.cache.CatalogTopologyCache;
import ai.floedb.floecat.service.metagraph.overlay.systemobjects.SystemGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.metagraph.resolver.FullyQualifiedResolver;
import ai.floedb.floecat.service.testsupport.TestNodes;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MetaGraphTest {

  UserGraph user;
  SystemGraph system;
  MetaGraph meta;
  CatalogTopologyCache topologyCache;
  LogicalSchemaMapper schemaMapper;
  EngineContext context;

  ResourceId sysTable =
      ResourceId.newBuilder()
          .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
          .setKind(ResourceKind.RK_TABLE)
          .setId("sys")
          .build();

  ResourceId usrTable =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("usr")
          .build();

  @BeforeEach
  void setup() {
    user = mock(UserGraph.class);
    system = mock(SystemGraph.class);
    topologyCache = mock(CatalogTopologyCache.class);

    schemaMapper =
        new LogicalSchemaMapper() {
          @Override
          public SchemaDescriptor map(UserTableNode t) {
            return SchemaDescriptor.getDefaultInstance();
          }
        };

    EngineContextProvider engine = mock(EngineContextProvider.class);
    context = EngineContext.of("engine", "1");
    when(engine.engineContext()).thenReturn(context);
    when(engine.isPresent()).thenReturn(true);

    meta = new MetaGraph(user, schemaMapper, system, engine, topologyCache);
  }

  @AfterEach
  void cleanup() {
    reset(system, user);
  }

  @Test
  void systemFirstResolution_table() {
    NameRef ref = NameRef.newBuilder().setName("t").build();
    when(system.resolveTable(ref, context)).thenReturn(Optional.of(sysTable));
    when(user.resolveTable("c", ref)).thenReturn(Optional.empty());

    Optional<ResourceId> out = meta.resolveTable("c", ref);

    assertThat(out).contains(sysTable);
  }

  @Test
  void userResolutionWhenSystemAbsent_table() {
    NameRef ref = NameRef.newBuilder().setName("t").build();
    when(system.resolveTable(ref, context)).thenReturn(Optional.empty());
    when(user.resolveTable("c", ref)).thenReturn(Optional.of(usrTable));

    Optional<ResourceId> out = meta.resolveTable("c", ref);

    assertThat(out).contains(usrTable);
  }

  @Test
  void unresolvedThrowsError_table() {
    NameRef ref = NameRef.newBuilder().setName("x").build();
    when(system.resolveTable(ref, context)).thenReturn(Optional.empty());
    when(user.resolveTable("c", ref)).thenReturn(Optional.empty());

    assertThat(meta.resolveTable("c", ref)).isEmpty();
  }

  @Test
  void listRelations_mergesSystemAndUser() {
    RelationNode s =
        new RelationNode() {
          @Override
          public ResourceId id() {
            return sysTable;
          }

          @Override
          public ResourceId namespaceId() {
            return ResourceId.getDefaultInstance();
          }

          @Override
          public long version() {
            return 1;
          }

          @Override
          public String displayName() {
            return "sys";
          }

          @Override
          public Instant metadataUpdatedAt() {
            return Instant.now();
          }

          @Override
          public GraphNodeKind kind() {
            return GraphNodeKind.TABLE;
          }

          @Override
          public GraphNodeOrigin origin() {
            return GraphNodeOrigin.SYSTEM;
          }

          @Override
          public Map<EngineHintKey, EngineHint> engineHints() {
            return Map.of();
          }
        };
    UserTableNode u = TestNodes.tableNode(usrTable, "{}");
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns")
            .build();

    when(system.listRelations(any(), same(context))).thenReturn(List.of(s));
    when(topologyCache.listNamespaceRefs(catalogId))
        .thenReturn(
            List.of(new TopologyGraph.NamespaceRef(namespaceId, "ns", catalogId, List.of())));
    when(topologyCache.listRelationRefs(catalogId, namespaceId))
        .thenReturn(List.of(new TopologyGraph.RelationRef(usrTable, "usr", ResourceKind.RK_TABLE)));
    when(user.table(usrTable)).thenReturn(Optional.of(u));

    List<RelationNode> out = meta.listRelations(catalogId);

    assertThat(out).containsExactly(s, u);
    verify(user, never()).listRelations(catalogId);
  }

  @Test
  void snapshotPinFor_system_returnsNull() {
    SnapshotPin pin = meta.snapshotPinFor("c", sysTable, null, Optional.empty());

    assertThat(pin).isNull();
  }

  @Test
  void snapshotPinFor_user_delegates() {
    SnapshotPin expected = SnapshotPin.newBuilder().build();
    when(user.snapshotPinFor(any(), any(), any(), any())).thenReturn(expected);

    SnapshotPin pin = meta.snapshotPinFor("c", usrTable, null, Optional.empty());

    assertThat(pin).isEqualTo(expected);
  }

  @Test
  void tableName_userFirst() {
    NameRef expected = NameRef.newBuilder().setName("user").build();
    when(user.tableName(usrTable)).thenReturn(Optional.of(expected));

    Optional<NameRef> name = meta.tableName(usrTable);

    assertThat(name).isPresent();
    assertThat(name.get().getName()).isEqualTo("user");
  }

  @Test
  void resolveTable_returnsSystemMatchWithoutUserProbe() {
    NameRef ref = NameRef.newBuilder().setName("t").build();
    when(system.resolveTable(ref, context)).thenReturn(Optional.of(sysTable));

    Optional<ResourceId> resolved = meta.resolveTable("c", ref);

    assertThat(resolved).contains(sysTable);
    verify(user, never()).resolveTable("c", ref);
  }

  private UserGraph.ResolveResult userResolveResult(
      int total, String token, CatalogOverlay.QualifiedRelation... relations) {
    List<FullyQualifiedResolver.QualifiedRelation> delegate =
        Arrays.stream(relations)
            .map(rel -> new FullyQualifiedResolver.QualifiedRelation(rel.name(), rel.resourceId()))
            .toList();
    return new UserGraph.ResolveResult(
        new FullyQualifiedResolver.ResolveResult(delegate, total, token));
  }

  @Test
  void resolveTables_list_mergesSystemAndUser() {
    NameRef systemRef = NameRef.newBuilder().setCatalog("examples").setName("system_table").build();
    NameRef userRef = NameRef.newBuilder().setCatalog("examples").setName("user_table").build();
    ResourceId userId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("user-table-2")
            .build();

    when(system.resolveTable(any(NameRef.class), eq(context))).thenReturn(Optional.of(sysTable));
    when(system.tableName(sysTable, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("system_table").build()));
    when(system.resolveTable(argThat(ref -> ref.equals(userRef)), eq(context)))
        .thenReturn(Optional.empty());

    UserGraph.ResolveResult userResult =
        userResolveResult(1, "user-token", new CatalogOverlay.QualifiedRelation(userRef, userId));
    when(user.resolveTables(eq("cid"), anyList(), eq(1), eq(""))).thenReturn(userResult);

    CatalogOverlay.ResolveResult merged =
        meta.batchResolveTables("cid", List.of(systemRef, userRef), 2, "");

    assertThat(merged.relations()).hasSize(2);
    assertThat(merged.relations().get(0).resourceId()).isEqualTo(sysTable);
    assertThat(merged.relations().get(0).name().getCatalog()).isEqualTo(systemRef.getCatalog());
    assertThat(merged.relations().get(1).resourceId()).isEqualTo(userId);
    assertThat(merged.nextToken()).isEmpty();
  }

  @Test
  void resolveTables_list_skipsUserLookupForSystemName() {
    NameRef ref = NameRef.newBuilder().setCatalog("examples").setName("system_table").build();

    when(system.resolveTable(any(NameRef.class), eq(context))).thenReturn(Optional.of(sysTable));
    when(system.tableName(sysTable, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("system_table").build()));

    CatalogOverlay.ResolveResult resolved = meta.batchResolveTables("cid", List.of(ref), 1, "");

    assertThat(resolved.relations()).hasSize(1);
    assertThat(resolved.relations().get(0).resourceId()).isEqualTo(sysTable);
    verify(user, never()).resolveTables(any(), anyList(), anyInt(), any());
  }

  @Test
  void resolveTables_prefix_mergesSystemAndUser() {
    NameRef prefix = NameRef.newBuilder().setCatalog("examples").addPath("ns").build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys-ns")
            .build();

    when(system.resolveNamespace(any(NameRef.class), eq(context)))
        .thenReturn(Optional.of(namespaceId));

    TableNode systemNode = prefixSystemTable(sysTable, namespaceId, "sys_t");

    when(system.listRelationsInNamespace(ResourceId.getDefaultInstance(), namespaceId, context))
        .thenReturn(List.of(systemNode));
    when(system.tableName(sysTable, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("sys_t").build()));

    UserGraph.ResolveResult userResult =
        userResolveResult(
            1,
            "user-token",
            new CatalogOverlay.QualifiedRelation(
                NameRef.newBuilder().setCatalog("examples").addPath("ns").setName("user_t").build(),
                usrTable));
    when(user.resolveTables(eq("cid"), eq(prefix), eq(49), eq(""))).thenReturn(userResult);

    CatalogOverlay.ResolveResult merged = meta.listTablesByPrefix("cid", prefix, 50, "");

    assertThat(merged.relations()).hasSize(2);
    assertThat(merged.relations().get(0).resourceId()).isEqualTo(sysTable);
    assertThat(merged.nextToken()).isEqualTo("u:user-token");
  }

  @Test
  void resolveTables_prefix_handlesNamespaceMissingAfterSystemHit() {
    NameRef prefix =
        NameRef.newBuilder().setCatalog("examples").addPath("information_schema").build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys-ns")
            .build();
    when(system.resolveNamespace(any(NameRef.class), eq(context)))
        .thenReturn(Optional.of(namespaceId));

    TableNode systemNode = prefixSystemTable(sysTable, namespaceId, "system_table");

    when(system.listRelationsInNamespace(ResourceId.getDefaultInstance(), namespaceId, context))
        .thenReturn(List.of(systemNode));
    when(system.tableName(sysTable, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("system_table").build()));

    when(user.resolveTables(eq("cid"), eq(prefix), eq(49), eq("")))
        .thenReturn(
            new UserGraph.ResolveResult(
                new FullyQualifiedResolver.ResolveResult(List.of(), 0, "")));

    CatalogOverlay.ResolveResult merged = meta.listTablesByPrefix("cid", prefix, 50, "");

    assertThat(merged.relations()).hasSize(1);
    assertThat(merged.relations().get(0).resourceId()).isEqualTo(sysTable);
  }

  @Test
  void listTablesByPrefix_systemFillingPageDoesNotSkipUserRows() {
    // Regression: when system rows fill the page, the user graph must not be consulted for rows
    // (its cursor would advance past a row the page drops). The user row must surface on the next
    // page, fetched from a blank cursor.
    NameRef prefix =
        NameRef.newBuilder().setCatalog("examples").addPath("information_schema").build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys-ns")
            .build();
    when(system.resolveNamespace(any(NameRef.class), eq(context)))
        .thenReturn(Optional.of(namespaceId));
    when(system.listRelationsInNamespace(ResourceId.getDefaultInstance(), namespaceId, context))
        .thenReturn(List.of(prefixSystemTable(sysTable, namespaceId, "system_table")));
    when(system.tableName(sysTable, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("system_table").build()));

    UserGraph.ResolveResult userResult =
        userResolveResult(
            1,
            "",
            new CatalogOverlay.QualifiedRelation(
                NameRef.newBuilder().setCatalog("examples").addPath("ns").setName("user_t").build(),
                usrTable));
    when(user.resolveTables(eq("cid"), eq(prefix), eq(1), eq(""))).thenReturn(userResult);
    when(user.countTablesByPrefix(eq("cid"), eq(prefix))).thenReturn(1);

    CatalogOverlay.ResolveResult firstPage = meta.listTablesByPrefix("cid", prefix, 1, "");

    assertThat(firstPage.relations()).hasSize(1);
    assertThat(firstPage.relations().get(0).resourceId()).isEqualTo(sysTable);
    assertThat(firstPage.totalSize()).isEqualTo(2);

    CatalogOverlay.ResolveResult secondPage =
        meta.listTablesByPrefix("cid", prefix, 1, firstPage.nextToken());

    assertThat(secondPage.relations()).hasSize(1);
    assertThat(secondPage.relations().get(0).resourceId()).isEqualTo(usrTable);
    assertThat(secondPage.totalSize()).isEqualTo(2);
    // The boundary page's total now uses the count-only path (no discarded one-row probe); only the
    // second page fetches rows, from a blank cursor.
    verify(user, times(1)).resolveTables(eq("cid"), eq(prefix), eq(1), eq(""));
    verify(user).countTablesByPrefix(eq("cid"), eq(prefix));
  }

  @Test
  void listTablesByPrefix_systemExactlyFillsPageWithNoUserRows_emitsNoToken() {
    // Regression: system rows exactly fill the page and there are no user rows. The handoff must
    // not advertise a user-phase page that would come back empty.
    NameRef prefix =
        NameRef.newBuilder().setCatalog("examples").addPath("information_schema").build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys-ns")
            .build();
    when(system.resolveNamespace(any(NameRef.class), eq(context)))
        .thenReturn(Optional.of(namespaceId));
    when(system.listRelationsInNamespace(ResourceId.getDefaultInstance(), namespaceId, context))
        .thenReturn(List.of(prefixSystemTable(sysTable, namespaceId, "system_table")));
    when(system.tableName(sysTable, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("system_table").build()));
    when(user.countTablesByPrefix(eq("cid"), eq(prefix))).thenReturn(0);

    CatalogOverlay.ResolveResult page = meta.listTablesByPrefix("cid", prefix, 1, "");

    assertThat(page.relations()).hasSize(1);
    assertThat(page.relations().get(0).resourceId()).isEqualTo(sysTable);
    assertThat(page.totalSize()).isEqualTo(1);
    assertThat(page.nextToken()).isEmpty();
    // No user-phase row fetch — there is nothing to continue into.
    verify(user, never()).resolveTables(eq("cid"), eq(prefix), eq(1), eq(""));
  }

  @Test
  void listTablesByPrefix_pagesThroughSystemOverflowWithoutLoss() {
    // Regression: system rows beyond the page size were dropped (system was collected first-page
    // only, capped at the page size). They must flow across pages via the system-phase token.
    NameRef prefix =
        NameRef.newBuilder().setCatalog("examples").addPath("information_schema").build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys-ns")
            .build();
    ResourceId sysTableB =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_TABLE)
            .setId("sys-t-b")
            .build();
    when(system.resolveNamespace(any(NameRef.class), eq(context)))
        .thenReturn(Optional.of(namespaceId));
    when(system.listRelationsInNamespace(ResourceId.getDefaultInstance(), namespaceId, context))
        .thenReturn(
            List.of(
                prefixSystemTable(sysTableB, namespaceId, "b_sys"),
                prefixSystemTable(sysTable, namespaceId, "a_sys")));
    when(system.tableName(sysTable, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("a_sys").build()));
    when(system.tableName(sysTableB, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("b_sys").build()));
    when(user.resolveTables(eq("cid"), eq(prefix), eq(1), eq("")))
        .thenReturn(
            new UserGraph.ResolveResult(
                new FullyQualifiedResolver.ResolveResult(List.of(), 0, "")));

    var collected = new java.util.ArrayList<ResourceId>();
    String token = "";
    for (int guard = 0; guard < 10 && collected.size() < 2; guard++) {
      CatalogOverlay.ResolveResult page = meta.listTablesByPrefix("cid", prefix, 1, token);
      page.relations().forEach(rel -> collected.add(rel.resourceId()));
      assertThat(page.totalSize()).isEqualTo(2);
      token = page.nextToken();
      if (token.isBlank()) {
        break;
      }
    }

    // Sorted by canonical name: a_sys then b_sys, no gaps, no duplicates.
    assertThat(collected).containsExactly(sysTable, sysTableB);
  }

  @Test
  void listTablesByPrefix_mixedPageEmitsSystemThenUserWithUserCursor() {
    NameRef prefix =
        NameRef.newBuilder().setCatalog("examples").addPath("information_schema").build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys-ns")
            .build();
    when(system.resolveNamespace(any(NameRef.class), eq(context)))
        .thenReturn(Optional.of(namespaceId));
    when(system.listRelationsInNamespace(ResourceId.getDefaultInstance(), namespaceId, context))
        .thenReturn(List.of(prefixSystemTable(sysTable, namespaceId, "system_table")));
    when(system.tableName(sysTable, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("system_table").build()));

    ResourceId userTableB =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("usr-b")
            .build();
    UserGraph.ResolveResult userResult =
        userResolveResult(
            5,
            "more",
            new CatalogOverlay.QualifiedRelation(
                NameRef.newBuilder().setCatalog("examples").addPath("ns").setName("u1").build(),
                usrTable),
            new CatalogOverlay.QualifiedRelation(
                NameRef.newBuilder().setCatalog("examples").addPath("ns").setName("u2").build(),
                userTableB));
    when(user.resolveTables(eq("cid"), eq(prefix), eq(2), eq(""))).thenReturn(userResult);

    CatalogOverlay.ResolveResult page = meta.listTablesByPrefix("cid", prefix, 3, "");

    assertThat(page.relations()).hasSize(3);
    assertThat(page.relations().get(0).resourceId()).isEqualTo(sysTable);
    assertThat(page.relations().get(1).resourceId()).isEqualTo(usrTable);
    assertThat(page.relations().get(2).resourceId()).isEqualTo(userTableB);
    // System row count + user total; continuation carries the user graph's cursor.
    assertThat(page.totalSize()).isEqualTo(6);
    assertThat(page.nextToken()).isEqualTo("u:more");
  }

  @Test
  void listViewsByPrefix_systemFillingPageDoesNotSkipUserRows() {
    NameRef prefix =
        NameRef.newBuilder().setCatalog("examples").addPath("information_schema").build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys-ns")
            .build();
    ResourceId sysView =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_VIEW)
            .setId("sys-view")
            .build();
    ResourceId usrView =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_VIEW)
            .setId("usr-view")
            .build();
    when(system.resolveNamespace(any(NameRef.class), eq(context)))
        .thenReturn(Optional.of(namespaceId));
    when(system.listRelationsInNamespace(ResourceId.getDefaultInstance(), namespaceId, context))
        .thenReturn(List.of(prefixSystemView(sysView, namespaceId, "system_view")));
    when(system.viewName(sysView, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("system_view").build()));

    UserGraph.ResolveResult userResult =
        userResolveResult(
            1,
            "",
            new CatalogOverlay.QualifiedRelation(
                NameRef.newBuilder().setCatalog("examples").addPath("ns").setName("uv").build(),
                usrView));
    when(user.resolveViews(eq("cid"), eq(prefix), eq(1), eq(""))).thenReturn(userResult);
    when(user.countViewsByPrefix(eq("cid"), eq(prefix))).thenReturn(1);

    CatalogOverlay.ResolveResult firstPage = meta.listViewsByPrefix("cid", prefix, 1, "");

    assertThat(firstPage.relations()).hasSize(1);
    assertThat(firstPage.relations().get(0).resourceId()).isEqualTo(sysView);
    assertThat(firstPage.totalSize()).isEqualTo(2);

    CatalogOverlay.ResolveResult secondPage =
        meta.listViewsByPrefix("cid", prefix, 1, firstPage.nextToken());

    assertThat(secondPage.relations()).hasSize(1);
    assertThat(secondPage.relations().get(0).resourceId()).isEqualTo(usrView);
    // The boundary page's total now uses the count-only path (no discarded one-row probe); only the
    // second page fetches rows, from a blank cursor.
    verify(user, times(1)).resolveViews(eq("cid"), eq(prefix), eq(1), eq(""));
    verify(user).countViewsByPrefix(eq("cid"), eq(prefix));
  }

  @Test
  void listTablesByPrefix_rejectsMalformedSystemToken() {
    NameRef prefix =
        NameRef.newBuilder().setCatalog("examples").addPath("information_schema").build();

    assertThatThrownBy(() -> meta.listTablesByPrefix("cid", prefix, 1, "sys:%%%"))
        .isInstanceOf(io.grpc.StatusRuntimeException.class)
        .satisfies(
            ex ->
                assertThat(((io.grpc.StatusRuntimeException) ex).getStatus().getCode())
                    .isEqualTo(io.grpc.Status.Code.INVALID_ARGUMENT));
  }

  private static ai.floedb.floecat.metagraph.model.ViewNode prefixSystemView(
      ResourceId id, ResourceId namespaceId, String name) {
    return new ai.floedb.floecat.metagraph.model.ViewNode(
        id,
        1L,
        Instant.EPOCH,
        ResourceId.getDefaultInstance(),
        namespaceId,
        name,
        "select 1",
        "sql",
        List.of(),
        List.of(),
        List.of(),
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Optional.empty(),
        Map.of(),
        Map.of());
  }

  private static TableNode prefixSystemTable(ResourceId id, ResourceId namespaceId, String name) {
    return new TableNode() {
      @Override
      public ResourceId namespaceId() {
        return namespaceId;
      }

      @Override
      public String displayName() {
        return name;
      }

      @Override
      public ResourceId id() {
        return id;
      }

      @Override
      public long version() {
        return 1;
      }

      @Override
      public Instant metadataUpdatedAt() {
        return Instant.EPOCH;
      }

      @Override
      public GraphNodeOrigin origin() {
        return GraphNodeOrigin.SYSTEM;
      }

      @Override
      public Map<EngineHintKey, EngineHint> engineHints() {
        return Map.of();
      }
    };
  }

  @Test
  void resolveTables_prefix_resolvesSystemNamespaceWithNameSegment() {
    NameRef prefix =
        NameRef.newBuilder().setCatalog("examples").addPath("information_schema").build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys-ns")
            .build();

    ArgumentCaptor<NameRef> captor = ArgumentCaptor.forClass(NameRef.class);
    when(system.resolveNamespace(captor.capture(), eq(context)))
        .thenReturn(Optional.of(namespaceId));

    when(system.listRelationsInNamespace(ResourceId.getDefaultInstance(), namespaceId, context))
        .thenReturn(List.of());

    when(user.resolveTables(eq("cid"), eq(prefix), eq(50), eq("")))
        .thenReturn(
            new UserGraph.ResolveResult(
                new FullyQualifiedResolver.ResolveResult(List.of(), 0, "")));

    meta.listTablesByPrefix("cid", prefix, 50, "");

    NameRef captured = captor.getValue();
    assertThat(captured.getCatalog()).isEqualTo(context.effectiveEngineKind());
    assertThat(captured.getPathCount()).isZero();
    assertThat(captured.getName()).isEqualTo("information_schema");
  }

  @Test
  void resolveCatalog_prefersUserCatalogWhenNameMatchesEngineAlias() {
    ResourceId userCatalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("user-cat")
            .build();

    when(user.resolveCatalog("cid", context.effectiveEngineKind()))
        .thenReturn(Optional.of(userCatalogId));

    Optional<ResourceId> out = meta.resolveCatalog("cid", context.effectiveEngineKind());

    assertThat(out).contains(userCatalogId);
  }

  @Test
  void resolveCatalog_resolvesSystemCatalogFromEngineAlias() {
    String alias = context.effectiveEngineKind().toUpperCase();
    when(user.resolveCatalog("cid", alias)).thenReturn(Optional.empty());

    Optional<ResourceId> out = meta.resolveCatalog("cid", alias);

    assertThat(out)
        .contains(SystemNodeRegistry.systemCatalogContainerId(context.effectiveEngineKind()));
  }

  @Test
  void listNamespaceRefsByName_usesCacheByNameForUserNamespaces() {
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat").build();
    ResourceId userNamespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("user-ns")
            .build();
    ResourceId systemNamespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys-ns")
            .build();
    NamespaceNode systemNamespace =
        new NamespaceNode(
            systemNamespaceId,
            1,
            Instant.EPOCH,
            catalogId,
            List.of("information_schema"),
            "tables",
            GraphNodeOrigin.SYSTEM,
            Map.of(),
            Map.of());
    Set<String> names = Set.of("sales", "information_schema.tables");
    when(system.listNamespaces(catalogId, context)).thenReturn(List.of(systemNamespace));
    when(topologyCache.listNamespaceRefsByName(catalogId, names))
        .thenReturn(
            List.of(
                new TopologyGraph.NamespaceRef(
                    userNamespaceId, "sales", catalogId, List.of("sales"))));

    List<TopologyGraph.NamespaceRef> refs = meta.listNamespaceRefsByName(catalogId, names);

    assertThat(refs)
        .extracting(TopologyGraph.NamespaceRef::id)
        .containsExactly(systemNamespaceId, userNamespaceId);
    verify(topologyCache).listNamespaceRefsByName(catalogId, names);
    verify(topologyCache, never()).listNamespaceRefs(catalogId);
  }

  @Test
  void listRelationRefsByName_usesCacheByNameForUserRelations() {
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat").build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns")
            .build();
    ResourceId userRelationId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("orders")
            .build();
    Set<String> names = Set.of("orders");
    when(system.listRelationsInNamespace(catalogId, namespaceId, context)).thenReturn(List.of());
    when(topologyCache.listRelationRefsByName(catalogId, namespaceId, names))
        .thenReturn(
            List.of(
                new TopologyGraph.RelationRef(userRelationId, "orders", ResourceKind.RK_TABLE)));

    List<TopologyGraph.RelationRef> refs =
        meta.listRelationRefsByName(catalogId, namespaceId, names);

    assertThat(refs).extracting(TopologyGraph.RelationRef::id).containsExactly(userRelationId);
    verify(topologyCache).listRelationRefsByName(catalogId, namespaceId, names);
    verify(topologyCache, never()).listRelationRefs(catalogId, namespaceId);
  }

  @Test
  void engineAbsent_showsEmptySystemGraph() {
    EngineContextProvider engine = mock(EngineContextProvider.class);
    when(engine.isPresent()).thenReturn(false);

    MetaGraph metaNoEngine = new MetaGraph(user, schemaMapper, system, engine, topologyCache);

    NameRef ref = NameRef.newBuilder().setName("t").build();
    when(system.resolveTable(ref, EngineContext.empty())).thenReturn(Optional.empty());
    when(user.resolveTable("c", ref)).thenReturn(Optional.of(usrTable));

    Optional<ResourceId> out = metaNoEngine.resolveTable("c", ref);

    assertThat(out).contains(usrTable);
  }
}
