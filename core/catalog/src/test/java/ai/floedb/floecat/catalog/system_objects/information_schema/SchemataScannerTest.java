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

class SchemataScannerTest {

  @Test
  void schema_isCorrect() {
    assertThat(new SchemataScanner().schema())
        .extracting(c -> c.getName())
        .containsExactly("catalog_name", "schema_name");
  }

  @Test
  void scan_returnsAllNamespacesInCatalog() {
    CatalogNode catalog =
        new CatalogNode(
            ResourceId.getDefaultInstance(),
            1,
            Instant.EPOCH,
            "main_catalog",
            Map.of(),
            null,
            null,
            null,
            Map.of());

    NamespaceNode ns1 =
        new NamespaceNode(
            ResourceId.getDefaultInstance(),
            1,
            Instant.EPOCH,
            catalog.id(),
            List.of("public"),
            "public",
            Map.of(),
            Optional.empty(),
            Map.of());

    NamespaceNode ns2 =
        new NamespaceNode(
            ResourceId.getDefaultInstance(),
            1,
            Instant.EPOCH,
            catalog.id(),
            List.of("sales"),
            "sales",
            Map.of(),
            Optional.empty(),
            Map.of());

    SystemObjectGraphView view =
        new SystemObjectGraphView() {
          @Override
          public Optional<RelationNode> resolve(ResourceId id) {
            return id.equals(catalog.id()) ? Optional.of(catalog) : Optional.empty();
          }

          @Override
          public List<TableNode> listTables(ResourceId c) {
            return List.of();
          }

          @Override
          public List<ViewNode> listViews(ResourceId c) {
            return List.of();
          }

          @Override
          public List<NamespaceNode> listNamespaces(ResourceId c) {
            return List.of(ns1, ns2);
          }
        };

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            "spark",
            "3.5.0",
            catalog.id(),
            ResourceId.getDefaultInstance());

    var rows = new SchemataScanner().scan(ctx).map(r -> List.of(r.values())).toList();

    assertThat(rows)
        .containsExactlyInAnyOrder(
            List.of("main_catalog", "public"), List.of("main_catalog", "sales"));
  }
}
