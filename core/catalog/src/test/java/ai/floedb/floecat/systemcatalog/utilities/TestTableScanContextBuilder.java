package ai.floedb.floecat.systemcatalog.utilities;

import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.*;
import java.time.Instant;
import java.util.*;

/** Test builder for table- and namespace-based scanners (information_schema.*). */
public final class TestTableScanContextBuilder extends AbstractTestScanContextBuilder {

  private TestTableScanContextBuilder(ResourceId catalogId) {
    super(catalogId);
    CatalogNode catalog =
        new CatalogNode(
            catalogId,
            1,
            Instant.EPOCH,
            catalogId.getId(),
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of());
    overlay.addNode(catalog);
  }

  public static TestTableScanContextBuilder builder(String catalogName) {
    return new TestTableScanContextBuilder(
        ResourceId.newBuilder()
            .setAccountId("account")
            .setKind(ResourceKind.RK_CATALOG)
            .setId(catalogName)
            .build());
  }

  public NamespaceNode addNamespace(String dottedPath) {
    List<String> segments = List.of(dottedPath.split("\\."));
    List<String> path = segments.size() > 1 ? segments.subList(0, segments.size() - 1) : List.of();

    String display = segments.get(segments.size() - 1);

    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId(catalogId.getAccountId())
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(dottedPath)
            .build();

    NamespaceNode ns =
        new NamespaceNode(
            id,
            1,
            Instant.EPOCH,
            catalogId,
            path,
            display,
            GraphNodeOrigin.USER,
            Map.of(),
            Optional.empty(),
            Map.of());

    overlay.addNode(ns);
    return ns;
  }

  public UserTableNode addTable(
      NamespaceNode ns, String name, Map<String, Integer> fieldIds, Map<String, String> types) {

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(catalogId.getAccountId())
            .setKind(ResourceKind.RK_TABLE)
            .setId(ns.id().getId() + "." + name)
            .build();

    UserTableNode table =
        new UserTableNode(
            tableId,
            1,
            Instant.EPOCH,
            catalogId,
            ns.id(),
            name,
            TableFormat.TF_DELTA,
            "",
            Map.of(),
            List.of(),
            fieldIds,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            List.of(),
            Map.of());

    overlay.addRelation(ns.id(), table);
    overlay.setColumnTypes(tableId, types);
    return table;
  }
}
