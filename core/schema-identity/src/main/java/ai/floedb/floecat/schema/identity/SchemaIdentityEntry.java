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
import java.util.OptionalInt;

/** Persisted identity facts for one node in the active schema version. */
public record SchemaIdentityEntry(ColumnPath path, OptionalInt nativeFieldId, long canonicalId) {
  public SchemaIdentityEntry {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(nativeFieldId, "nativeFieldId");
    if (path.isRoot()) {
      throw new IllegalArgumentException("An identity entry cannot use the root path");
    }
    if (canonicalId <= 0) {
      throw new IllegalArgumentException("Canonical column ID must be positive");
    }
  }
}
