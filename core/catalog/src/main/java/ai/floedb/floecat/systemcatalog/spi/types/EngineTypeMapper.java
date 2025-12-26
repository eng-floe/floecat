package ai.floedb.floecat.systemcatalog.spi.types;

import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.types.LogicalType;
import java.util.Optional;

/** Maps logical types (INT32, STRING, DECIMAL(10,2), ...) to engine-specific builtin TypeNodes. */
public interface EngineTypeMapper {

  EngineTypeMapper EMPTY = (logicalType, lookup) -> Optional.empty();

  /**
   * Resolve a logical type to an engine TypeNode.
   *
   * <p>Resolution is performed against the current MetaGraph.
   */
  Optional<TypeNode> resolve(LogicalType logicalType, TypeLookup lookup);
}
