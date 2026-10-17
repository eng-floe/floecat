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

import java.util.Map;
import java.util.Objects;

/** Canonical IDs supplied by a newer state for source identities proven continuous to it. */
public record PreferredCanonicalIds(Map<ColumnPath, Long> byPath, Map<Integer, Long> byNativeId) {
  public static final PreferredCanonicalIds NONE = new PreferredCanonicalIds(Map.of(), Map.of());

  public PreferredCanonicalIds {
    byPath = Map.copyOf(Objects.requireNonNull(byPath, "byPath"));
    byNativeId = Map.copyOf(Objects.requireNonNull(byNativeId, "byNativeId"));
  }

  public static PreferredCanonicalIds byPath(Map<ColumnPath, Long> ids) {
    return new PreferredCanonicalIds(ids, Map.of());
  }

  public static PreferredCanonicalIds byNativeId(Map<Integer, Long> ids) {
    return new PreferredCanonicalIds(Map.of(), ids);
  }
}
