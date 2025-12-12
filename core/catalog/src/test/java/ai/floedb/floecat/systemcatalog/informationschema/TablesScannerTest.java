package ai.floedb.floecat.systemcatalog.informationschema;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utils.TestGraphView;
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
            List.of(),
            "public",
            GraphNodeOrigin.USER,
            Map.of(),
            Optional.empty(),
            Map.of());

    TestGraphView view =
        new TestGraphView() {
          @Override
          public List<TableNode> listTables(ResourceId ns) {
            return List.of(t);
          }

          @Override
          public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
            return List.of(ns);
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

  @Test
  void scan_usesCanonicalSchemaPathForNestedNamespaces() {
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("cat")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("ns-nested")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    TableNode t =
        new TableNode(
            ResourceId.getDefaultInstance(),
            1L,
            Instant.EPOCH,
            catalogId,
            namespaceId,
            "nested_table",
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

    NamespaceNode nestedNs =
        new NamespaceNode(
            namespaceId,
            1L,
            Instant.EPOCH,
            catalogId,
            List.of("finance"),
            "sales",
            GraphNodeOrigin.USER,
            Map.of(),
            Optional.empty(),
            Map.of());

    TestGraphView view =
        new TestGraphView() {
          @Override
          public List<TableNode> listTables(ResourceId ns) {
            return List.of(t);
          }

          @Override
          public List<NamespaceNode> listNamespaces(ResourceId catalog) {
            return List.of(nestedNs);
          }
        };

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view, NameRef.getDefaultInstance(), "spark", "3.5.0", catalogId, namespaceId);

    var rows = new TablesScanner().scan(ctx).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly("finance.sales", "nested_table", "BASE TABLE");
  }
}
