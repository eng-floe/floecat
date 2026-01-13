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
import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
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

  public TypeResolver(MetadataResolutionContext ctx, EngineTypeMapper mapper) {
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    Objects.requireNonNull(ctx, "ctx");
    this.lookup = new SystemTypeLookup(ctx.overlay().listTypes(ctx.catalogId()));
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
