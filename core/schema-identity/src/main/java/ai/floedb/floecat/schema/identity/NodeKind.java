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

/** A named schema field or one of the implicit nodes inside a collection. */
public enum NodeKind {
  FIELD(1, 0),
  ARRAY_ELEMENT(2, 1),
  MAP_KEY(3, 2),
  MAP_VALUE(4, 3);

  private final int stableCode;
  private final int collectionSuffixDigit;

  NodeKind(int stableCode, int collectionSuffixDigit) {
    this.stableCode = stableCode;
    this.collectionSuffixDigit = collectionSuffixDigit;
  }

  /** Stable persisted code that is independent of enum declaration order. */
  public int stableCode() {
    return stableCode;
  }

  /** Base-4 digit used when deriving the identity of a collection interior. */
  int collectionSuffixDigit() {
    if (this == FIELD) {
      throw new IllegalStateException("A field is not a collection interior");
    }
    return collectionSuffixDigit;
  }
}
