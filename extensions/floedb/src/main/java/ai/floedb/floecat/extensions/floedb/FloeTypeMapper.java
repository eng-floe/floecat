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
      case BOOLEAN -> lookup.findByName("pg_catalog", "bool");

      case INT16 -> lookup.findByName("pg_catalog", "int2");
      case INT32 -> lookup.findByName("pg_catalog", "int4");
      case INT64 -> lookup.findByName("pg_catalog", "int8");

      case FLOAT32 -> lookup.findByName("pg_catalog", "float4");
      case FLOAT64 -> lookup.findByName("pg_catalog", "float8");

      case STRING -> lookup.findByName("pg_catalog", "text");
      case BINARY -> lookup.findByName("pg_catalog", "bytea");
      case UUID -> lookup.findByName("pg_catalog", "uuid");

      case DATE -> lookup.findByName("pg_catalog", "date");
      case TIME -> lookup.findByName("pg_catalog", "time");
      case TIMESTAMP -> lookup.findByName("pg_catalog", "timestamp");

      case DECIMAL -> lookup.findByName("pg_catalog", "numeric");

      default -> Optional.empty();
    };
  }
}
