package ai.floedb.floecat.catalog.system_objects.information_schema;

import ai.floedb.floecat.catalog.system_objects.spi.*;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
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

    return ctx.listTables(ctx.catalogId()).stream()
        .map(
            t ->
                new SystemObjectRow(
                    new Object[] {
                      ((NamespaceNode) ctx.resolve(t.namespaceId())).displayName(),
                      t.displayName(),
                      BASE_TABLE
                    }));
  }
}
