package ai.floedb.floecat.systemcatalog.informationschema;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** information_schema.schemata */
public final class SchemataScanner implements SystemObjectScanner {

  public static final List<SchemaColumn> SCHEMA =
      List.of(
          SchemaColumn.newBuilder()
              .setName("catalog_name")
              .setLogicalType("VARCHAR")
              .setFieldId(0)
              .setNullable(false)
              .build(),
          SchemaColumn.newBuilder()
              .setName("schema_name")
              .setLogicalType("VARCHAR")
              .setFieldId(1)
              .setNullable(false)
              .build());

  @Override
  public List<SchemaColumn> schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    Map<ResourceId, String> catalogById = new HashMap<>();

    return ctx.listNamespaces().stream()
        .map(
            ns ->
                new SystemObjectRow(
                    new Object[] {
                      catalogById.computeIfAbsent(
                          ns.catalogId(), id -> ((CatalogNode) ctx.resolve(id)).displayName()),
                      schemaName(ns)
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
