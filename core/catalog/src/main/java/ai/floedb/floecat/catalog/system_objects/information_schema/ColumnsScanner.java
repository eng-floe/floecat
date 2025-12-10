package ai.floedb.floecat.catalog.system_objects.information_schema;

import ai.floedb.floecat.catalog.system_objects.spi.*;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** information_schema.columns */
public final class ColumnsScanner implements SystemObjectScanner {

  public static final SchemaColumn[] SCHEMA =
      new SchemaColumn[] {
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
            .build()
      };

  @Override
  public SchemaColumn[] schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    // Cache namespace display names per catalog to avoid repeated lookups.
    Map<ResourceId, String> schemaByNamespace =
        ctx.listNamespaces(ctx.catalogId()).stream()
            .collect(Collectors.toMap(NamespaceNode::id, ColumnsScanner::schemaName));
    Map<ResourceId, String> catalogById = new HashMap<>();

    return ctx.listTables(ctx.catalogId()).stream()
        .flatMap(
            table -> {
              String catalogName =
                  catalogById.computeIfAbsent(
                      table.catalogId(), id -> ((CatalogNode) ctx.resolve(id)).displayName());
              String schemaName = schemaByNamespace.getOrDefault(table.namespaceId(), "");

              Map<String, String> columnTypes = ctx.columnTypes(table.id());

              return table.fieldIdByPath().entrySet().stream()
                  .sorted(Comparator.comparingInt(Map.Entry::getValue))
                  .map(
                      entry -> {
                        String dataType = columnTypes.getOrDefault(entry.getKey(), "");
                        return new SystemObjectRow(
                            new Object[] {
                              catalogName,
                              schemaName,
                              table.displayName(),
                              columnName(entry.getKey()),
                              dataType,
                              entry.getValue()
                            });
                      });
            });
  }

  private static String schemaName(NamespaceNode namespace) {
    List<String> segments = new ArrayList<>(namespace.pathSegments());
    if (!namespace.displayName().isBlank()) {
      segments.add(namespace.displayName());
    }
    return String.join(".", segments);
  }

  private static String columnName(String path) {
    if (path == null || path.isBlank()) {
      return "";
    }
    int idx = path.lastIndexOf('.');
    return idx < 0 ? path : path.substring(idx + 1);
  }
}
