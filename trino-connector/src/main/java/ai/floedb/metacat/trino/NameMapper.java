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
    List<String> parts = new ArrayList<>();
    if (!ref.getCatalog().isBlank()) {
      parts.add(ref.getCatalog());
    }
    parts.addAll(ref.getPathList().stream().filter(p -> !p.isBlank()).toList());
    return parts.stream().collect(Collectors.joining("."));
  }

  static NameRef prefixFromSchema(String schema) {
    List<String> segs = (schema == null || schema.isBlank()) ? List.of() : List.of(schema.split("\\."));
    NameRef.Builder b = NameRef.newBuilder();
    if (!segs.isEmpty()) {
      b.setCatalog(segs.get(0));
      if (segs.size() > 1) {
        b.addAllPath(segs.subList(1, segs.size()));
      }
    }
    return b.build();
  }

  static NameRef nameRef(String schema, String table) {
    NameRef.Builder b = prefixFromSchema(schema).toBuilder();
    b.setName(table);
    return b.build();
  }

  static ResourceId tableId(ResolveFQTablesResponse.Entry entry) {
    return entry.getResourceId();
  }
}
