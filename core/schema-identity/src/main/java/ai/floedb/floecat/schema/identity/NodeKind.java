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

/**
 * What a schema node is, structurally.
 *
 * <p>Collection interiors are first-class nodes rather than traversal artifacts: Delta can assign
 * them stable ids through {@code delta.columnMapping.nested.ids}, and an identity scheme for
 * unmapped tables has to be able to name them.
 */
public enum NodeKind {
  /** A named struct field. The only kind that carries a name. */
  FIELD,
  /** The implicit element of an array. */
  ARRAY_ELEMENT,
  /** The implicit key of a map. */
  MAP_KEY,
  /** The implicit value of a map. */
  MAP_VALUE;

  public boolean isCollectionInterior() {
    return this != FIELD;
  }
}
