package ai.floedb.metacat.trino;

import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesResponse;
import io.trino.spi.connector.SchemaTableName;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

final class NameMapper {
  private NameMapper() {}

  static SchemaTableName toSchemaTableName(ResolveFQTablesResponse.Entry entry) {
    NameRef ref = entry.getName();
    String schema = schemaFrom(ref);
    String table = ref.getName();
    return new SchemaTableName(schema, table);
  }

  static String schemaFrom(NameRef ref) {
    // Schema should be derived from the path only; catalog is handled separately by Trino.
    List<String> parts = new ArrayList<>(ref.getPathList().stream().filter(p -> !p.isBlank()).toList());
    return parts.stream().collect(Collectors.joining("."));
  }

  static NameRef prefix(String catalog, String schema) {
    NameRef.Builder b = NameRef.newBuilder().setCatalog(catalog);
    if (schema != null && !schema.isBlank()) {
      b.addAllPath(List.of(schema.split("\\.")));
    }
    return b.build();
  }

  static NameRef nameRef(String catalog, String schema, String table) {
    NameRef.Builder b = prefix(catalog, schema).toBuilder();
    b.setName(table);
    return b.build();
  }

  static ResourceId tableId(ResolveFQTablesResponse.Entry entry) {
    return entry.getResourceId();
  }
}
