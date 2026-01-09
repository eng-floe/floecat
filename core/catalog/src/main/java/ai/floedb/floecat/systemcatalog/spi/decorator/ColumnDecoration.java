package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.types.LogicalType;

/** Mutable view over a column while decorators add engine metadata. */
public final class ColumnDecoration {

  private final ColumnInfo.Builder builder;
  private final SchemaColumn schemaColumn;
  private final LogicalType logicalType;
  private final int ordinal;
  private final RelationDecoration relation;

  public ColumnDecoration(
      ColumnInfo.Builder builder,
      SchemaColumn schemaColumn,
      LogicalType logicalType,
      int ordinal,
      RelationDecoration relation) {
    this.builder = builder;
    this.schemaColumn = schemaColumn;
    this.logicalType = logicalType;
    this.ordinal = ordinal;
    this.relation = relation;
  }

  public ColumnInfo.Builder builder() {
    return builder;
  }

  public SchemaColumn schemaColumn() {
    return schemaColumn;
  }

  public LogicalType logicalType() {
    return logicalType;
  }

  public int ordinal() {
    return ordinal;
  }

  public RelationDecoration relation() {
    return relation;
  }
}
