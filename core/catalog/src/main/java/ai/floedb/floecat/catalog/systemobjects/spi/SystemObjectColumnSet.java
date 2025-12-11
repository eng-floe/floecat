package ai.floedb.floecat.catalog.systemobjects.spi;

import ai.floedb.floecat.query.rpc.SchemaColumn;

/** Immutable schema wrapper for system objects. */
public record SystemObjectColumnSet(SchemaColumn[] columns) {

  public SystemObjectColumnSet {
    columns = columns.clone(); // defensive copy
  }

  @Override
  public SchemaColumn[] columns() {
    return columns.clone();
  }

  public int size() {
    return columns.length;
  }

  public SchemaColumn column(int i) {
    return columns[i];
  }
}
