/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.systemcatalog.spi.types;

import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.types.LogicalType;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Helper that resolves logical types to {@link TypeNode}s using an {@link EngineTypeMapper}. */
public final class TypeResolver {
  private final EngineTypeMapper mapper;
  private final TypeLookup lookup;
  private final Map<LogicalType, Optional<TypeNode>> cache = new ConcurrentHashMap<>();

  /**
   * Returns a resolver memoized on the provided metadata context.
   *
   * <p>Resolver memoization is keyed by mapper instance to avoid mixing mapping semantics.
   */
  public static TypeResolver forContext(MetadataResolutionContext ctx, EngineTypeMapper mapper) {
    Objects.requireNonNull(ctx, "ctx");
    Objects.requireNonNull(mapper, "mapper");
    return ctx.memoized(new ResolverMemoKey(mapper), () -> new TypeResolver(ctx, mapper));
  }

  TypeResolver(MetadataResolutionContext ctx, EngineTypeMapper mapper) {
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    Objects.requireNonNull(ctx, "ctx");
    TypeLookup userLookup = new UserTypeLookup(ctx);
    TypeLookup systemLookup =
        (namespace, name) -> {
          if (namespace == null || namespace.isBlank() || name == null || name.isBlank()) {
            return Optional.empty();
          }
          return ctx.overlay().resolveSystemType(namespace, name);
        };
    this.lookup = new LayeredTypeLookup(userLookup, systemLookup);
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

  private record LayeredTypeLookup(TypeLookup user, TypeLookup system) implements TypeLookup {
    @Override
    public Optional<TypeNode> findByName(String namespace, String name) {
      return user.findByName(namespace, name).or(() -> system.findByName(namespace, name));
    }
  }

  private record ResolverMemoKey(EngineTypeMapper mapper) {}
}
