package ai.floedb.floecat.systemcatalog.informationschema;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** information_schema.tables */
public final class TablesScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          SchemaColumn.newBuilder()
              .setName("table_catalog")
              .setLogicalType("VARCHAR")
              .setFieldId(0)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("table_schema")
              .setLogicalType("VARCHAR")
              .setFieldId(0)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("table_name")
              .setLogicalType("VARCHAR")
              .setFieldId(1)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("table_type")
              .setLogicalType("VARCHAR")
              .setFieldId(2)
              .setNullable(false)
              .build());

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    // Cache schema names by namespace id
    Map<ResourceId, String> schemaByNamespace =
        ctx.listNamespaces().stream()
            .collect(Collectors.toMap(NamespaceNode::id, TablesScanner::schemaName));

    // Cache catalog display names
    Map<ResourceId, String> catalogNames = new java.util.HashMap<>();

    return ctx.listNamespaces().stream()
        .flatMap(
            ns ->
                ctx.listRelations(ns.id()).stream()
                    .map(rel -> rowForRelation(ctx, ns, rel, schemaByNamespace, catalogNames)));
  }

  private SystemObjectRow rowForRelation(
      SystemObjectScanContext ctx,
      NamespaceNode namespace,
      GraphNode node,
      Map<ResourceId, String> schemaByNamespace,
      Map<ResourceId, String> catalogNames) {

    ResourceId catalogId = namespace.catalogId();

    String catalogName =
        catalogNames.computeIfAbsent(
            catalogId, id -> ((CatalogNode) ctx.resolve(id)).displayName());

    return new SystemObjectRow(
        new Object[] {
          catalogName,
          schemaByNamespace.getOrDefault(namespace.id(), ""),
          node.displayName(),
          relationKind(node)
        });
  }

  private static String relationKind(GraphNode node) {
    return switch (node.kind()) {
      case TABLE -> "BASE TABLE";
      case VIEW -> "VIEW";
      default -> "UNKNOWN";
    };
  }

  private static String schemaName(NamespaceNode namespace) {
    List<String> segments = new ArrayList<>(namespace.pathSegments());
    if (!namespace.displayName().isBlank()) {
      segments.add(namespace.displayName());
    }
    return String.join(".", segments);
  }
}
