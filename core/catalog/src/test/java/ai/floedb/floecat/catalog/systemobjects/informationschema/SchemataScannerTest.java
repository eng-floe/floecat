package ai.floedb.floecat.catalog.systemobjects.informationschema;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.catalog.systemobjects.registry.SystemObjectGraphView;
import ai.floedb.floecat.catalog.systemobjects.spi.*;
import ai.floedb.floecat.catalog.utils.TestGraphView;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
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
            List.of(),
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
            List.of(),
            "sales",
            Map.of(),
            Optional.empty(),
            Map.of());

    SystemObjectGraphView view =
        new TestGraphView() {
          @Override
          public Optional<GraphNode> resolve(ResourceId id) {
            return id.equals(catalog.id()) ? Optional.of(catalog) : Optional.empty();
          }

          @Override
          public Optional<NamespaceNode> tryNamespace(ResourceId id) {
            if (id.equals(ns1.id())) return Optional.of(ns1);
            if (id.equals(ns2.id())) return Optional.of(ns2);
            return Optional.empty();
          }

          @Override
          public List<ResourceId> listCatalogs() {
            return List.of(catalog.id());
          }

          @Override
          public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
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

  @Test
  void scan_usesCanonicalSchemaPathForNestedNamespaces() {
    CatalogNode catalog =
        new CatalogNode(
            ResourceId.newBuilder()
                .setAccountId("account")
                .setId("cat")
                .setKind(ResourceKind.RK_CATALOG)
                .build(),
            1,
            Instant.EPOCH,
            "main_catalog",
            Map.of(),
            null,
            null,
            null,
            Map.of());

    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("ns-nested")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    NamespaceNode nested =
        new NamespaceNode(
            namespaceId,
            1,
            Instant.EPOCH,
            catalog.id(),
            List.of("finance"),
            "sales",
            Map.of(),
            Optional.empty(),
            Map.of());

    SystemObjectGraphView view =
        new TestGraphView() {
          @Override
          public Optional<GraphNode> resolve(ResourceId id) {
            if (id.equals(catalog.id())) return Optional.of(catalog);
            return Optional.empty();
          }

          @Override
          public List<ResourceId> listCatalogs() {
            return List.of(catalog.id());
          }

          @Override
          public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
            return List.of(nested);
          }
        };

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view, NameRef.getDefaultInstance(), "spark", "3.5.0", catalog.id(), namespaceId);

    var rows = new SchemataScanner().scan(ctx).map(r -> List.of(r.values())).toList();

    assertThat(rows).containsExactly(List.of("main_catalog", "finance.sales"));
  }
}
