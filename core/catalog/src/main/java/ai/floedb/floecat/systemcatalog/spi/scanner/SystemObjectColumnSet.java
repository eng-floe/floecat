package ai.floedb.floecat.systemcatalog.spi.scanner;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.List;

/** Immutable schema wrapper for system objects. */
public record SystemObjectColumnSet(List<SchemaColumn> columns) {

  public SystemObjectColumnSet {
    // defensive copy + immutability
    columns = List.copyOf(columns);
  }

  /** Returns an immutable list of columns. */
  @Override
  public List<SchemaColumn> columns() {
    return columns;
  }

  public int size() {
    return columns.size();
  }

  public SchemaColumn column(int i) {
    return columns.get(i);
  }
}
