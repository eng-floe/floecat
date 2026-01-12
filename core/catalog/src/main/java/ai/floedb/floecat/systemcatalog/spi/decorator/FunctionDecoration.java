package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.query.rpc.SqlFunction;
import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
import java.util.Objects;

/** Mutable holder describing a function (SqlFunction) during bundle decoration. */
public final class FunctionDecoration extends AbstractDecoration {

  private final SqlFunction.Builder builder;

  public FunctionDecoration(
      SqlFunction.Builder builder, MetadataResolutionContext resolutionContext) {
    super(resolutionContext);
    this.builder = Objects.requireNonNull(builder, "builder");
  }

  public SqlFunction.Builder builder() {
    return builder;
  }
}
