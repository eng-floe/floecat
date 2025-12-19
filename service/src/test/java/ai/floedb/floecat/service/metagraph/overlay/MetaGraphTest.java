package ai.floedb.floecat.service.metagraph.overlay;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.metagraph.overlay.systemobjects.SystemGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.query.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetaGraphTest {

  UserGraph user;
  SystemGraph system;
  MetaGraph meta;
  LogicalSchemaMapper schemaMapper;

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
    when(engine.engineKind()).thenReturn("engine");
    when(engine.engineVersion()).thenReturn("1");
    when(engine.isPresent()).thenReturn(true);

    meta = new MetaGraph(user, schemaMapper, system, engine);
  }

  @AfterEach
  void cleanup() {
    // No cleanup needed
  }

  @Test
  void systemFirstResolution_table() {
    NameRef ref = NameRef.newBuilder().setName("t").build();
    when(system.resolveTable(ref, "engine", "1")).thenReturn(Optional.of(sysTable));
    when(user.tryResolveTable("c", ref)).thenReturn(Optional.empty());

    ResourceId out = meta.resolveTable("c", ref);

    assertThat(out).isEqualTo(sysTable);
  }

  @Test
  void userResolutionWhenSystemAbsent_table() {
    NameRef ref = NameRef.newBuilder().setName("t").build();
    when(system.resolveTable(ref, "engine", "1")).thenReturn(Optional.empty());
    when(user.tryResolveTable("c", ref)).thenReturn(Optional.of(usrTable));

    ResourceId out = meta.resolveTable("c", ref);

    assertThat(out).isEqualTo(usrTable);
  }

  @Test
  void unresolvedThrowsError_table() {
    NameRef ref = NameRef.newBuilder().setName("x").build();
    when(system.resolveTable(ref, "engine", "1")).thenReturn(Optional.empty());
    when(user.tryResolveTable("c", ref)).thenReturn(Optional.empty());

    assertThatThrownBy(() -> meta.resolveTable("c", ref)).isInstanceOf(RuntimeException.class);
  }

  @Test
  void listRelations_mergesSystemAndUser() {
    GraphNode s =
        new GraphNode() {
          @Override
          public ResourceId id() {
            return sysTable;
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
          public Map<EngineKey, EngineHint> engineHints() {
            return Map.of();
          }
        };
    GraphNode u =
        new GraphNode() {
          @Override
          public ResourceId id() {
            return usrTable;
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
          public Map<EngineKey, EngineHint> engineHints() {
            return Map.of();
          }
        };

    when(system.listRelations(any(), eq("engine"), eq("1"))).thenReturn(List.of(s));
    when(user.listRelations(any())).thenReturn(List.of(u));

    List<GraphNode> out = meta.listRelations(ResourceId.newBuilder().setId("cat").build());

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
    when(system.resolveTable(ref, "engine", "1")).thenReturn(Optional.of(sysTable));
    when(user.tryResolveTable("c", ref)).thenReturn(Optional.of(usrTable));

    assertThatThrownBy(() -> meta.resolveTable("c", ref)).isInstanceOf(RuntimeException.class);
  }

  @Test
  void engineAbsent_showsEmptySystemGraph() {
    EngineContextProvider engine = mock(EngineContextProvider.class);
    when(engine.isPresent()).thenReturn(false);

    MetaGraph metaNoEngine = new MetaGraph(user, schemaMapper, system, engine);

    NameRef ref = NameRef.newBuilder().setName("t").build();
    when(system.resolveTable(ref, null, null)).thenReturn(Optional.empty());
    when(user.tryResolveTable("c", ref)).thenReturn(Optional.of(usrTable));

    ResourceId out = metaNoEngine.resolveTable("c", ref);

    assertThat(out).isEqualTo(usrTable);
  }
}
