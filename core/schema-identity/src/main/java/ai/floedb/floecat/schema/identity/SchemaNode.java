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

package ai.floedb.floecat.schema.identity;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Source identity facts for one schema node. This record deliberately carries no Floecat canonical
 * ID; assigning one is a later, stateful reconciliation step.
 */
public record SchemaNode(
    ColumnPath path,
    int ordinal,
    boolean leaf,
    OptionalInt nativeFieldId,
    Optional<ColumnPath> sourcePhysicalPath) {

  public SchemaNode {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(nativeFieldId, "nativeFieldId");
    Objects.requireNonNull(sourcePhysicalPath, "sourcePhysicalPath");
    if (path.isRoot()) {
      throw new IllegalArgumentException("A schema node cannot use the root path");
    }
    if (ordinal <= 0) {
      throw new IllegalArgumentException("Ordinal must be 1-based");
    }
  }

  public NodeKind kind() {
    return path.last().kind();
  }
}
