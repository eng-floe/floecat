package ai.floedb.floecat.systemcatalog.informationschema;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utils.TestGraphView;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ColumnsScannerTest {

  @Test
  void schema_isCorrect() {
    assertThat(new ColumnsScanner().schema())
        .extracting(SchemaColumn::getName)
        .containsExactly(
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position");
  }

  @Test
  void scan_returnsOneRowPerColumn() {
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("cat")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("ns")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    TableNode table =
        new TableNode(
            ResourceId.getDefaultInstance(),
            1,
            Instant.EPOCH,
            catalogId,
            namespaceId,
            "orders",
            TableFormat.TF_DELTA,
            "",
            Map.of(),
            List.of(),
            Map.of("id", 1, "stats.sales", 2),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            List.of(),
            Map.of());

    NamespaceNode ns =
        new NamespaceNode(
            namespaceId,
            1,
            Instant.EPOCH,
            catalogId,
            List.of("finance"),
            "sales",
            GraphNodeOrigin.USER,
            Map.of(),
            Optional.empty(),
            Map.of());

    CatalogNode catalog =
        new CatalogNode(
            catalogId,
            1,
            Instant.EPOCH,
            "marketing",
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of());

    TestGraphView view =
        new TestGraphView() {
          @Override
          public List<TableNode> listTables(ResourceId nsId) {
            return List.of(table);
          }

          @Override
          public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
            return List.of(ns);
          }

          @Override
          public Optional<GraphNode> resolve(ResourceId id) {
            return Optional.of(catalog);
          }

          @Override
          public Map<String, String> tableColumnTypes(ResourceId tableId) {
            return Map.of("id", "long", "stats.sales", "double");
          }
        };

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view, NameRef.getDefaultInstance(), "spark", "3.5.0", catalogId, namespaceId);

    var rows = new ColumnsScanner().scan(ctx).map(r -> List.of(r.values())).toList();

    assertThat(rows)
        .containsExactly(
            List.of("marketing", "finance.sales", "orders", "id", "long", 1),
            List.of("marketing", "finance.sales", "orders", "sales", "double", 2));
  }
}
