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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Projects structured {@link ColumnPath}s onto the lossy dotted keys of the string-based connector
 * contract, retiring every key that more than one distinct path renders to.
 *
 * <p>{@link ColumnPath#legacyDottedKey()} is not injective: a field literally named {@code "a.b"}
 * and the nested field {@code a} → {@code b} render identically. Any index that treats the
 * rendering as identity would silently attribute one column's values to another, so this class
 * drops such keys entirely rather than picking a winner. Dropping loses statistics for the
 * colliding columns, which only costs planning precision; keeping them would corrupt it.
 *
 * <p>Instances are mutable builders and are not thread-safe. Insertion order is preserved.
 *
 * @param <V> the value each path carries into the projected map
 */
public final class LegacyDottedKeyIndex<V> {

  private final Map<String, ColumnPath> pathsByKey = new LinkedHashMap<>();
  private final Map<String, V> valuesByKey = new LinkedHashMap<>();
  private final Set<String> ambiguousKeys = new LinkedHashSet<>();

  private LegacyDottedKeyIndex() {}

  public static <V> LegacyDottedKeyIndex<V> create() {
    return new LegacyDottedKeyIndex<>();
  }

  /**
   * Registers one path under its rendered key.
   *
   * <p>Re-adding an equal path is a no-op that keeps the first value. A different path claiming a
   * key retires that key for good: neither path, nor any later one rendering to it, appears in the
   * projected map.
   */
  public void add(ColumnPath path, V value) {
    Objects.requireNonNull(path, "path");
    String key = path.legacyDottedKey();
    ColumnPath existing = pathsByKey.putIfAbsent(key, path);
    if (existing == null) {
      valuesByKey.put(key, value);
      return;
    }
    if (existing.equals(path)) {
      return;
    }
    ambiguousKeys.add(key);
    valuesByKey.remove(key);
  }

  /** The values of every path whose rendered key names it unambiguously, in insertion order. */
  public Map<String, V> values() {
    return Collections.unmodifiableMap(new LinkedHashMap<>(valuesByKey));
  }

  /** The keys of {@link #values()}, in insertion order. */
  public Set<String> keys() {
    return Collections.unmodifiableSet(new LinkedHashSet<>(valuesByKey.keySet()));
  }
}
