package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.query.rpc.SqlType;
import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
import java.util.Objects;

/** Mutable holder describing a type (SqlType) during bundle decoration. */
public final class TypeDecoration extends AbstractDecoration {

  private final SqlType.Builder builder;

  public TypeDecoration(SqlType.Builder builder, MetadataResolutionContext resolutionContext) {
    super(resolutionContext);
    this.builder = Objects.requireNonNull(builder, "builder");
  }

  public SqlType.Builder builder() {
    return builder;
  }
}
