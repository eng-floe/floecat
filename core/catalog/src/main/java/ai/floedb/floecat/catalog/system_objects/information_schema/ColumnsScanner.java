package ai.floedb.floecat.catalog.system_objects.information_schema;

import ai.floedb.floecat.catalog.system_objects.spi.*;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.stream.Stream;

/** information_schema.columns */
public final class ColumnsScanner implements SystemObjectScanner {

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
            .setName("column_name")
            .setLogicalType("VARCHAR")
            .setFieldId(2)
            .setNullable(false)
            .build(),
        SchemaColumn.newBuilder()
            .setName("data_type")
            .setLogicalType("VARCHAR")
            .setFieldId(3)
            .setNullable(false)
            .build(),
        SchemaColumn.newBuilder()
            .setName("ordinal_position")
            .setLogicalType("INT")
            .setFieldId(4)
            .setNullable(false)
            .build()
      };

  @Override
  public SchemaColumn[] schema() {
    return SCHEMA;
  }

  @Override
  public Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    // TODO: Implement proper column enumeration from TableNode schemaJson / field map.
    return Stream.empty();
  }
}
