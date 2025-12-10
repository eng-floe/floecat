package ai.floedb.floecat.catalog.system_objects.spi;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectGraphView;
import ai.floedb.floecat.catalog.utils.TestGraphView;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SystemObjectScanContextTest {

  @Test
  void resolve_throws_whenMissing() {
    SystemObjectGraphView view = new TestGraphView();

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.newBuilder().build(),
            "spark",
            "3.5.0",
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance());

    assertThatThrownBy(() -> ctx.resolve(ResourceId.getDefaultInstance()))
        .isInstanceOf(Exception.class);
  }

  @Test
  void tryResolve_returnsEmptyWhenMissing() {
    SystemObjectGraphView view = new TestGraphView();

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.newBuilder().build(),
            "spark",
            "3.5.0",
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance());

    assertThat(ctx.tryResolve(ResourceId.getDefaultInstance())).isEmpty();
  }

  @Test
  void resolve_returnsNodeWhenPresent() {
    GraphNode node =
        new TableNode(
            ResourceId.getDefaultInstance(),
            0L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            "t",
            null, // format (not relevant for this test)
            "{}", // schemaJson
            Map.of(), // properties
            List.of(), // partitionKeys
            Map.of(), // fieldIdByPath
            Optional.empty(), // currentSnapshot
            Optional.empty(), // previousSnapshot
            Optional.empty(), // resolvedSnapshots
            Optional.empty(), // statsSummary
            List.of(), // dependentViews
            Map.of()); // engineHints

    SystemObjectGraphView view =
        new TestGraphView() {
          @Override
          public Optional<GraphNode> resolve(ResourceId id) {
            return Optional.of(node);
          }
        };

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            "spark",
            "3.5.0",
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance());

    assertThat(ctx.resolve(ResourceId.getDefaultInstance())).isSameAs(node);
  }

  @Test
  void listTables_delegates() {
    TableNode t =
        new TableNode(
            ResourceId.getDefaultInstance(),
            0L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            "t",
            null,
            "{}",
            Map.of(),
            List.of(),
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            List.of(),
            Map.of());

    SystemObjectGraphView view =
        new TestGraphView() {
          @Override
          public List<TableNode> listTables(ResourceId c) {
            return List.of(t);
          }
        };

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            "spark",
            "3.5.0",
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance());

    assertThat(ctx.listTables(ResourceId.getDefaultInstance())).containsExactly(t);
  }

  @Test
  void listViews_delegates() {
    ViewNode v =
        new ViewNode(
            ResourceId.getDefaultInstance(),
            0L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            "v",
            "SELECT 1", // sql
            "sql", // dialect
            List.of(), // outputColumns
            List.of(), // baseRelations
            List.of(), // creationSearchPath
            Map.of(), // properties
            Optional.empty(), // owner
            Map.of()); // engineHints

    SystemObjectGraphView view =
        new TestGraphView() {
          @Override
          public List<ViewNode> listViews(ResourceId c) {
            return List.of(v);
          }
        };

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            "spark",
            "3.5.0",
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance());

    assertThat(ctx.listViews(ResourceId.getDefaultInstance())).containsExactly(v);
  }

  @Test
  void listNamespaces_delegates() {
    NamespaceNode n =
        new NamespaceNode(
            ResourceId.getDefaultInstance(),
            0L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            List.of("public"), // pathSegments
            "n",
            Map.of(), // properties
            Optional.empty(), // relationIds
            Map.of()); // engineHints

    SystemObjectGraphView view =
        new TestGraphView() {
          @Override
          public List<NamespaceNode> listNamespaces(ResourceId c) {
            return List.of(n);
          }
        };

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            "spark",
            "3.5.0",
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance());

    assertThat(ctx.listNamespaces(ResourceId.getDefaultInstance())).containsExactly(n);
  }

  @Test
  void contextFields_areRetained() {
    NameRef name = NameRef.newBuilder().addPath("a").setName("b").build();
    ResourceId catalog = ResourceId.newBuilder().setId("cat").build();
    ResourceId ns = ResourceId.newBuilder().setId("ns").build();

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(new TestGraphView(), name, "spark", "3.5.0", catalog, ns);

    assertThat(ctx.name()).isEqualTo(name);
    assertThat(ctx.catalogId()).isEqualTo(catalog);
    assertThat(ctx.namespaceId()).isEqualTo(ns);
    assertThat(ctx.engineKind()).isEqualTo("spark");
    assertThat(ctx.engineVersion()).isEqualTo("3.5.0");
  }
}
