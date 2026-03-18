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

package ai.floedb.floecat.scanner.spi;

import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Shared provider that surfaces relation-scoped constraints to metadata consumers. */
public interface ConstraintProvider {

  default Optional<ConstraintSetView> constraints(ResourceId relationId, OptionalLong snapshotId) {
    return Optional.empty();
  }

  ConstraintProvider NONE = new ConstraintProvider() {};

  interface ConstraintSetView {
    /** Relation identity. */
    ResourceId relationId();

    /** Constraint set for this relation under the supplied caller context. */
    List<ConstraintDefinition> constraints();

    /** Optional provider-specific metadata. */
    default Map<String, String> properties() {
      return Map.of();
    }
  }
}
