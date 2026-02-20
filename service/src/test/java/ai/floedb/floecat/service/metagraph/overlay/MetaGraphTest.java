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

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.metagraph.overlay.systemobjects.SystemGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.metagraph.resolver.FullyQualifiedResolver;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MetaGraphTest {

  UserGraph user;
  SystemGraph system;
  MetaGraph meta;
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

    meta = new MetaGraph(user, schemaMapper, system, engine);
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
    RelationNode u =
        new RelationNode() {
          @Override
          public ResourceId id() {
            return usrTable;
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
            return "usr";
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
            return GraphNodeOrigin.USER;
          }

          @Override
          public Map<EngineHintKey, EngineHint> engineHints() {
            return Map.of();
          }
        };

    when(system.listRelations(any(), same(context))).thenReturn(List.of(s));
    when(user.listRelations(any())).thenReturn(List.of(u));

    List<RelationNode> out = meta.listRelations(ResourceId.newBuilder().setId("cat").build());

    assertThat(out).containsExactly(s, u);
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
  void resolveTable_throwsWhenBothSystemAndUserMatch() {
    NameRef ref = NameRef.newBuilder().setName("t").build();
    when(system.resolveTable(ref, context)).thenReturn(Optional.of(sysTable));
    when(user.resolveTable("c", ref)).thenReturn(Optional.of(usrTable));

    assertThatThrownBy(() -> meta.resolveTable("c", ref)).isInstanceOf(RuntimeException.class);
  }

  private String firstErrorKey(StatusRuntimeException ex) {
    com.google.rpc.Status status = StatusProto.fromThrowable(ex);
    return status.getDetailsList().stream()
        .filter(detail -> detail.is(Error.class))
        .map(
            detail -> {
              try {
                return detail.unpack(Error.class).getMessageKey();
              } catch (Exception e) {
                throw new AssertionError("unable to unpack error detail", e);
              }
            })
        .findFirst()
        .orElse("");
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
    when(user.resolveTables(eq("cid"), anyList(), eq(2), eq(""))).thenReturn(userResult);

    CatalogOverlay.ResolveResult merged =
        meta.batchResolveTables("cid", List.of(systemRef, userRef), 2, "");

    assertThat(merged.relations()).hasSize(2);
    assertThat(merged.relations().get(0).resourceId()).isEqualTo(sysTable);
    assertThat(merged.relations().get(0).name().getCatalog()).isEqualTo(systemRef.getCatalog());
    assertThat(merged.relations().get(1).resourceId()).isEqualTo(userId);
    assertThat(merged.nextToken()).isEmpty();
  }

  @Test
  void resolveTables_list_throwsOnAmbiguousName() {
    NameRef ref = NameRef.newBuilder().setCatalog("examples").setName("collide").build();
    NameRef alias = NameRef.newBuilder().setCatalog("examples").setName("alias").build();

    when(system.resolveTable(any(NameRef.class), eq(context))).thenReturn(Optional.of(sysTable));
    when(system.tableName(sysTable, context)).thenReturn(Optional.of(alias));

    UserGraph.ResolveResult userResult =
        userResolveResult(1, "", new CatalogOverlay.QualifiedRelation(alias, usrTable));
    when(user.resolveTables(eq("cid"), anyList(), eq(1), eq(""))).thenReturn(userResult);

    assertThatThrownBy(() -> meta.batchResolveTables("cid", List.of(ref), 1, ""))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            ex ->
                assertThat(firstErrorKey((StatusRuntimeException) ex))
                    .contains("query.input.ambiguous"));
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

    TableNode systemNode =
        new TableNode() {
          @Override
          public ResourceId namespaceId() {
            return namespaceId;
          }

          @Override
          public String displayName() {
            return "sys_t";
          }

          @Override
          public ResourceId id() {
            return sysTable;
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
    when(user.resolveTables(eq("cid"), eq(prefix), eq(50), eq(""))).thenReturn(userResult);

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

    TableNode systemNode =
        new TableNode() {
          @Override
          public ResourceId namespaceId() {
            return namespaceId;
          }

          @Override
          public String displayName() {
            return "system_table";
          }

          @Override
          public ResourceId id() {
            return sysTable;
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

    when(system.listRelationsInNamespace(ResourceId.getDefaultInstance(), namespaceId, context))
        .thenReturn(List.of(systemNode));
    when(system.tableName(sysTable, context))
        .thenReturn(
            Optional.of(NameRef.newBuilder().setCatalog("engine").setName("system_table").build()));

    when(user.resolveTables(eq("cid"), eq(prefix), eq(50), eq("")))
        .thenReturn(
            new UserGraph.ResolveResult(
                new FullyQualifiedResolver.ResolveResult(List.of(), 0, "")));

    CatalogOverlay.ResolveResult merged = meta.listTablesByPrefix("cid", prefix, 50, "");

    assertThat(merged.relations()).hasSize(1);
    assertThat(merged.relations().get(0).resourceId()).isEqualTo(sysTable);
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
  void engineAbsent_showsEmptySystemGraph() {
    EngineContextProvider engine = mock(EngineContextProvider.class);
    when(engine.isPresent()).thenReturn(false);

    MetaGraph metaNoEngine = new MetaGraph(user, schemaMapper, system, engine);

    NameRef ref = NameRef.newBuilder().setName("t").build();
    when(system.resolveTable(ref, EngineContext.empty())).thenReturn(Optional.empty());
    when(user.resolveTable("c", ref)).thenReturn(Optional.of(usrTable));

    Optional<ResourceId> out = metaNoEngine.resolveTable("c", ref);

    assertThat(out).contains(usrTable);
  }
}
