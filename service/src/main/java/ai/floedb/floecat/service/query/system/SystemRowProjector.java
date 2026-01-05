package ai.floedb.floecat.service.query.system;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import java.util.List;
import java.util.stream.Stream;

public final class SystemRowProjector {

  public static List<SystemObjectRow> project(
      List<SystemObjectRow> rows, List<SchemaColumn> schema, List<String> requiredColumns) {

    if (requiredColumns.isEmpty()) return rows;

    return project(rows.stream(), schema, requiredColumns).toList();
  }

  public static Stream<SystemObjectRow> project(
      Stream<SystemObjectRow> rows, List<SchemaColumn> schema, List<String> requiredColumns) {

    if (requiredColumns.isEmpty()) return rows;

    int[] indexes =
        requiredColumns.stream().mapToInt(c -> indexOf(schema, c)).filter(i -> i >= 0).toArray();

    return rows.map(r -> projectRow(r, indexes));
  }

  private static SystemObjectRow projectRow(SystemObjectRow row, int[] idxs) {

    Object[] src = row.values();
    Object[] dst = new Object[idxs.length];

    for (int i = 0; i < idxs.length; i++) {
      dst[i] = src[idxs[i]];
    }

    return new SystemObjectRow(dst);
  }

  private static int indexOf(List<SchemaColumn> schema, String name) {

    for (int i = 0; i < schema.size(); i++) {
      if (schema.get(i).getName().equalsIgnoreCase(name)) {
        return i;
      }
    }
    return -1;
  }

  private SystemRowProjector() {}
}
