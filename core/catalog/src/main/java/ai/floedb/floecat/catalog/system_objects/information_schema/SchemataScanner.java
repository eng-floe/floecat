package ai.floedb.floecat.catalog.system_objects.information_schema;

import ai.floedb.floecat.catalog.system_objects.spi.*;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.stream.Stream;

/** information_schema.schemata */
public final class SchemataScanner implements SystemObjectScanner {

  public static final SchemaColumn[] SCHEMA =
      new SchemaColumn[] {
        SchemaColumn.newBuilder()
            .setName("schema_name")
            .setLogicalType("VARCHAR")
            .setFieldId(0)
            .setNullable(false)
            .build(),
        SchemaColumn.newBuilder()
            .setName("catalog_name")
            .setLogicalType("VARCHAR")
            .setFieldId(1)
            .setNullable(false)
            .build()
      };

  @Override
  public SchemaColumn[] schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {

    return ctx.listNamespaces(ctx.catalogId()).stream()
        .map(
            ns ->
                new SystemObjectRow(
                    new Object[] {
                      ns.displayName(), ((CatalogNode) ctx.resolve(ns.catalogId())).displayName()
                    }));
  }
}
