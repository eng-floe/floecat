package ai.floedb.floecat.catalog.system_objects.information_schema;

import ai.floedb.floecat.catalog.system_objects.spi.*;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** information_schema.tables */
public final class TablesScanner implements SystemObjectScanner {

  private static final Object BASE_TABLE = "BASE TABLE";

  public static final SchemaColumn[] SCHEMA =
      new SchemaColumn[] {
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
            .build()
      };

  @Override
  public SchemaColumn[] schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    Map<ResourceId, String> schemaByNamespace =
        ctx.listNamespaces(ctx.catalogId()).stream()
            .collect(Collectors.toMap(NamespaceNode::id, TablesScanner::schemaName));

    return ctx.listTables(ctx.catalogId()).stream()
        .map(
            t ->
                new SystemObjectRow(
                    new Object[] {
                      schemaByNamespace.getOrDefault(t.namespaceId(), ""),
                      t.displayName(),
                      BASE_TABLE
                    }));
  }

  private static String schemaName(NamespaceNode namespace) {
    List<String> segments = new ArrayList<>(namespace.pathSegments());
    if (!namespace.displayName().isBlank()) {
      segments.add(namespace.displayName());
    }
    return String.join(".", segments);
  }
}
