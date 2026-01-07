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
