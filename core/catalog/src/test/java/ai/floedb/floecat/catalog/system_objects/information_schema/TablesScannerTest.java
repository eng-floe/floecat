package ai.floedb.floecat.catalog.system_objects.information_schema;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectGraphView;
import ai.floedb.floecat.catalog.system_objects.spi.*;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TablesScannerTest {

  @Test
  void schema_isCorrect() {
    assertThat(new TablesScanner().schema())
        .extracting(c -> c.getName())
        .containsExactly("table_schema", "table_name", "table_type");
  }

  @Test
  void scan_returnsOneRowPerTable() {
    // mock table node
    TableNode t =
        new TableNode(
            ResourceId.getDefaultInstance(),
            1L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            "mytable",
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

    NamespaceNode ns =
        new NamespaceNode(
            ResourceId.getDefaultInstance(),
            1L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            List.of("public"),
            "public",
            Map.of(),
            Optional.empty(),
            Map.of());

    SystemObjectGraphView view =
        new SystemObjectGraphView() {
          @Override
          public Optional<RelationNode> resolve(ResourceId id) {
            return Optional.of(ns);
          }

          @Override
          public List<TableNode> listTables(ResourceId c) {
            return List.of(t);
          }

          @Override
          public List<ViewNode> listViews(ResourceId c) {
            return List.of();
          }

          @Override
          public List<NamespaceNode> listNamespaces(ResourceId c) {
            return List.of();
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

    var rows = new TablesScanner().scan(ctx).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly("public", "mytable", "BASE TABLE");
  }
}
