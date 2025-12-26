package ai.floedb.floecat.systemcatalog.spi.types;

import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.types.LogicalType;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Helper that resolves logical types to {@link TypeNode}s using an {@link EngineTypeMapper}. */
public final class TypeResolver {

  private final EngineTypeMapper mapper;
  private final TypeLookup lookup;
  private final Map<LogicalType, Optional<TypeNode>> cache = new HashMap<>();

  public TypeResolver(SystemObjectScanContext ctx, EngineTypeMapper mapper) {
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    Objects.requireNonNull(ctx, "ctx");
    this.lookup = new SystemTypeLookup(ctx.listTypes());
  }

  /** Resolves a logical type to an existing TypeNode, if the mapper is aware of it. */
  public Optional<TypeNode> resolve(LogicalType logicalType) {
    Objects.requireNonNull(logicalType, "logicalType");
    return cache.computeIfAbsent(logicalType, key -> mapper.resolve(key, lookup));
  }

  /** Resolves a logical type and throws if the mapper cannot produce a TypeNode. */
  public TypeNode resolveOrThrow(LogicalType logicalType) {
    return resolve(logicalType)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Failed to resolve logical type "
                        + logicalType
                        + " using mapper "
                        + mapper.getClass().getSimpleName()));
  }
}
