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

/** One resolved source node paired with its authoritative Floecat ID. */
public record CanonicalSchemaNode(SchemaNode source, long canonicalId) {
  public CanonicalSchemaNode {
    Objects.requireNonNull(source, "source");
    if (canonicalId <= 0) {
      throw new IllegalArgumentException("Canonical column ID must be positive");
    }
  }
}
