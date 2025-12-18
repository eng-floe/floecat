package ai.floedb.floecat.extensions.floedb;

import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import ai.floedb.floecat.systemcatalog.spi.types.TypeLookup;
import ai.floedb.floecat.types.LogicalType;
import java.util.Optional;

/** Maps Floecat logical types to FloeDb builtin type. */
public final class FloeTypeMapper implements EngineTypeMapper {

  @Override
  public Optional<TypeNode> resolve(LogicalType t, TypeLookup lookup) {
    return switch (t.kind()) {
      case INT32 -> lookup.findByName("pg_catalog", "int4");
      case STRING -> lookup.findByName("pg_catalog", "text");
      case BOOLEAN -> lookup.findByName("pg_catalog", "bool");
      case DECIMAL -> lookup.findByName("pg_catalog", "numeric");
      default -> Optional.empty();
    };
  }
}
