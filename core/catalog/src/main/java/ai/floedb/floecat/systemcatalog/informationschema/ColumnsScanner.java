package ai.floedb.floecat.systemcatalog.informationschema;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** information_schema.columns */
public final class ColumnsScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          SchemaColumn.newBuilder()
              .setName("table_catalog")
              .setLogicalType("VARCHAR")
              .setFieldId(0)
              .setNullable(true)
              .build(),
          SchemaColumn.newBuilder()
              .setName("table_schema")
              .setLogicalType("VARCHAR")
              .setFieldId(1)
              .setNullable(true)
              .build(),
          SchemaColumn.newBuilder()
              .setName("table_name")
              .setLogicalType("VARCHAR")
              .setFieldId(2)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("column_name")
              .setLogicalType("VARCHAR")
              .setFieldId(3)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("data_type")
              .setLogicalType("VARCHAR")
              .setFieldId(4)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("ordinal_position")
              .setLogicalType("INT")
              .setFieldId(5)
              .setNullable(false)
              .build());

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    // Small per-scan caches (cheap, bounded)
    Map<ResourceId, String> catalogNames = new HashMap<>();

    return ctx.listNamespaces().stream()
        .flatMap(ns -> ctx.listRelations(ns.id()).stream())
        .flatMap(node -> scanRelation(ctx, node, catalogNames));
  }

  private Stream<SystemObjectRow> scanRelation(
      SystemObjectScanContext ctx, GraphNode node, Map<ResourceId, String> catalogNames) {

    if (node instanceof TableNode table) {
      return scanTable(ctx, table, catalogNames);
    }

    if (node instanceof ViewNode view) {
      return scanView(ctx, view, catalogNames);
    }

    return Stream.empty();
  }

  private Stream<SystemObjectRow> scanTable(
      SystemObjectScanContext ctx, TableNode table, Map<ResourceId, String> catalogNames) {

    NamespaceNode namespace = (NamespaceNode) ctx.resolve(table.namespaceId());
    ResourceId catalogId = namespace.catalogId();

    String catalogName =
        catalogNames.computeIfAbsent(
            catalogId, id -> ((CatalogNode) ctx.resolve(id)).displayName());

    String schemaName = schemaName(namespace);

    List<SchemaColumn> columns = ctx.graph().tableSchema(table.id());

    if (table instanceof UserTableNode ut) {
      return columns.stream()
          .sorted(Comparator.comparingInt(SchemaColumn::getFieldId))
          .map(
              col ->
                  new SystemObjectRow(
                      new Object[] {
                        catalogName,
                        schemaName,
                        table.displayName(),
                        col.getName(),
                        blankToNull(col.getLogicalType()),
                        col.getFieldId()
                      }));
    }

    // System tables (no field ids) – preserve declared order
    List<SystemObjectRow> rows = new ArrayList<>(columns.size());
    int ordinal = 1;
    for (SchemaColumn col : columns) {
      rows.add(
          new SystemObjectRow(
              new Object[] {
                catalogName,
                schemaName,
                table.displayName(),
                col.getName(),
                blankToNull(col.getLogicalType()),
                ordinal++
              }));
    }
    return rows.stream();
  }

  private Stream<SystemObjectRow> scanView(
      SystemObjectScanContext ctx, ViewNode view, Map<ResourceId, String> catalogNames) {

    NamespaceNode namespace = (NamespaceNode) ctx.resolve(view.namespaceId());
    ResourceId catalogId = namespace.catalogId();

    String catalogName =
        catalogNames.computeIfAbsent(
            catalogId, id -> ((CatalogNode) ctx.resolve(id)).displayName());

    String schemaName = schemaName(namespace);
    List<SchemaColumn> cols = view.outputColumns();

    List<SystemObjectRow> rows = new ArrayList<>(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      SchemaColumn col = cols.get(i);
      rows.add(
          new SystemObjectRow(
              new Object[] {
                catalogName,
                schemaName,
                view.displayName(),
                col.getName(),
                col.getLogicalType(),
                i + 1
              }));
    }
    return rows.stream();
  }

  private static String schemaName(NamespaceNode namespace) {
    List<String> segments = new ArrayList<>(namespace.pathSegments());
    if (!namespace.displayName().isBlank()) {
      segments.add(namespace.displayName());
    }
    return String.join(".", segments);
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }
}
