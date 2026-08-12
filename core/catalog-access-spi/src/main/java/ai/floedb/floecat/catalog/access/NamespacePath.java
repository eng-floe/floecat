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

package ai.floedb.floecat.catalog.access;

import java.util.List;
import java.util.Objects;

/** A case-preserving namespace path ordered from catalog root to namespace leaf. */
public record NamespacePath(List<String> segments) implements Comparable<NamespacePath> {
  private static final NamespacePath ROOT = new NamespacePath(List.of());

  public NamespacePath {
    segments =
        Objects.requireNonNull(segments, "segments").stream()
            .map(segment -> Objects.requireNonNull(segment, "segment").trim())
            .peek(
                segment -> {
                  if (segment.isEmpty()) {
                    throw new IllegalArgumentException("namespace segments must not be blank");
                  }
                })
            .toList();
  }

  public static NamespacePath root() {
    return ROOT;
  }

  public static NamespacePath of(String... segments) {
    return new NamespacePath(List.of(segments));
  }

  @Override
  public int compareTo(NamespacePath other) {
    int shared = Math.min(segments.size(), other.segments.size());
    for (int i = 0; i < shared; i++) {
      int compared = segments.get(i).compareTo(other.segments.get(i));
      if (compared != 0) {
        return compared;
      }
    }
    return Integer.compare(segments.size(), other.segments.size());
  }

  @Override
  public String toString() {
    return String.join(".", segments);
  }
}
