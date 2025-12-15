package ai.floedb.floecat.systemcatalog.utilities;

import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utils.TestGraphView;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Test-only builder for {@link SystemObjectScanContext}.
 *
 * <p>This utility models the behavior of the catalog overlay and graph view used by system object
 * scanners (information_schema, pg_catalog, etc.).
 *
 * <p>Key semantics:
 *
 * <ul>
 *   <li>Supports both single-namespace and multi-namespace scenarios
 *   <li>In multi-namespace mode, tables are visible under all namespaces and scanners decide
 *       grouping (mirrors real system catalogs)
 *   <li>{@code resolve(...)} returns injected namespaces so scanners can derive canonical schema
 *       names
 *   <li>The context namespace ID is semantic (anchor only), not restrictive
 * </ul>
 *
 * <p>All scanner tests should use this builder instead of ad-hoc graph mocks to ensure consistent
 * overlay behavior.
 */
public final class TestScanContextBuilder {

  private ResourceId catalogId =
      ResourceId.newBuilder()
          .setAccountId("account")
          .setKind(ResourceKind.RK_CATALOG)
          .setId("cat")
          .build();

  private ResourceId namespaceId =
      ResourceId.newBuilder()
          .setAccountId("account")
          .setKind(ResourceKind.RK_NAMESPACE)
          .setId("ns")
          .build();

  private String catalogName = "catalog";
  private List<String> namespacePath = List.of("ns");
  private String namespaceDisplay = "ns";

  private List<TableNode> tables = List.of();
  private Map<String, String> columnTypes = Map.of();

  private String engineKind = "spark";
  private String engineVersion = "3.5.0";

  private List<NamespaceNode> namespaces = List.of();

  public TestScanContextBuilder withCatalog(String name) {
    this.catalogName = name;
    this.catalogId =
        ResourceId.newBuilder()
            .setAccountId(catalogId.getAccountId())
            .setKind(ResourceKind.RK_CATALOG)
            .setId(name)
            .build();
    return this;
  }

  public TestScanContextBuilder withNamespace(String dottedPath) {
    List<String> segments = List.of(dottedPath.split("\\."));
    if (segments.size() > 1) {
      this.namespacePath = segments.subList(0, segments.size() - 1);
      this.namespaceDisplay = segments.get(segments.size() - 1);
    } else {
      this.namespacePath = List.of();
      this.namespaceDisplay = segments.get(0);
    }
    this.namespaceId =
        ResourceId.newBuilder()
            .setAccountId(catalogId.getAccountId())
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(dottedPath)
            .build();
    return this;
  }

  public TestScanContextBuilder withTable(TableNode table) {
    this.tables =
        this.tables.isEmpty()
            ? List.of(table)
            : new ArrayList<>(this.tables) {
              {
                add(table);
              }
            };
    return this;
  }

  public TestScanContextBuilder withColumnTypes(Map<String, String> types) {
    this.columnTypes = types;
    return this;
  }

  public TableNode newTable(String name, Map<String, Integer> columns) {

    return new TableNode(
        ResourceId.newBuilder()
            .setAccountId(catalogId.getAccountId())
            .setKind(ResourceKind.RK_TABLE)
            .setId(namespaceId.getId() + "." + name)
            .build(),
        1,
        Instant.EPOCH,
        catalogId,
        namespaceId,
        name,
        TableFormat.TF_DELTA,
        "",
        Map.of(),
        List.of(),
        columns,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        Map.of());
  }

  public NamespaceNode newNamespace(String dottedPath) {
    List<String> segments = List.of(dottedPath.split("\\."));
    List<String> path = segments.size() > 1 ? segments.subList(0, segments.size() - 1) : List.of();
    String display = segments.get(segments.size() - 1);

    ResourceId nsId =
        ResourceId.newBuilder()
            .setAccountId(catalogId.getAccountId())
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(dottedPath)
            .build();

    return new NamespaceNode(
        nsId,
        1,
        Instant.EPOCH,
        catalogId,
        List.copyOf(path),
        display,
        GraphNodeOrigin.USER,
        Map.of(),
        Optional.empty(),
        Map.of());
  }

  public TestScanContextBuilder withNamespaces(List<NamespaceNode> namespaces) {
    this.namespaces = List.copyOf(namespaces);
    return this;
  }

  public SystemObjectScanContext build() {
    CatalogNode catalog =
        new CatalogNode(
            catalogId,
            1,
            Instant.EPOCH,
            catalogName,
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of());

    NamespaceNode namespace =
        new NamespaceNode(
            namespaceId,
            1,
            Instant.EPOCH,
            catalogId,
            namespacePath,
            namespaceDisplay,
            GraphNodeOrigin.USER,
            Map.of(),
            Optional.empty(),
            Map.of());

    TestGraphView view =
        new TestGraphView() {
          @Override
          public List<TableNode> listTables(ResourceId nsId) {
            // multi-namespace mode -> scanners decide grouping
            if (!namespaces.isEmpty()) {
              return tables;
            }

            // single-namespace mode -> strict matching
            if (!namespaceId.equals(nsId)) {
              return List.of();
            }
            return tables;
          }

          @Override
          public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
            if (!TestScanContextBuilder.this.catalogId.equals(catalogId)) {
              return List.of();
            }
            if (!namespaces.isEmpty()) {
              return namespaces;
            }
            return List.of(namespace);
          }

          @Override
          public Optional<GraphNode> resolve(ResourceId id) {
            if (id == null) {
              return Optional.empty();
            }

            // Catalog resolution
            if (id.equals(catalog.id())) {
              return Optional.of(catalog);
            }

            // Multi-namespace mode: resolve injected namespaces
            for (NamespaceNode ns : namespaces) {
              if (id.equals(ns.id())) {
                return Optional.of(ns);
              }
            }

            // Single-namespace mode fallback
            if (id.equals(namespace.id())) {
              return Optional.of(namespace);
            }

            return Optional.empty();
          }

          @Override
          public Map<String, String> tableColumnTypes(ResourceId tableId) {
            return columnTypes;
          }
        };

    ResourceId contextNamespaceId = !namespaces.isEmpty() ? namespaces.get(0).id() : namespaceId;

    return new SystemObjectScanContext(
        view,
        NameRef.getDefaultInstance(),
        engineKind,
        engineVersion,
        catalogId,
        contextNamespaceId);
  }

  public static TestScanContextBuilder builder() {
    return new TestScanContextBuilder();
  }
}
