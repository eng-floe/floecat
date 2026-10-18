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

package ai.floedb.floecat.delta.identity;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/** Lazy, bounded access to persisted identity state for one table. */
public interface PersistedIdentityLookup {
  static PersistedIdentityLookup forSnapshots(
      Map<Long, ColumnIdentityMap> maps, Optional<ColumnIdentityMap> tableMap, long highWaterMark) {
    return new MapLookup(new TreeMap<>(maps), tableMap, highWaterMark);
  }

  static PersistedIdentityLookup forTableMap(Optional<ColumnIdentityMap> tableMap) {
    return forSnapshots(
        Map.of(), tableMap, tableMap.map(ColumnIdentityMap::getHighWaterMark).orElse(0L));
  }

  record MapLookup(
      NavigableMap<Long, ColumnIdentityMap> maps,
      Optional<ColumnIdentityMap> tableMap,
      long highWaterMark)
      implements PersistedIdentityLookup {
    public MapLookup {
      maps = java.util.Collections.unmodifiableNavigableMap(new TreeMap<>(maps));
      java.util.Objects.requireNonNull(tableMap, "tableMap");
      if (highWaterMark < 0L) {
        throw new IllegalArgumentException("Identity high-water mark cannot be negative");
      }
    }

    public Optional<ColumnIdentityMap> exact(long version) {
      return Optional.ofNullable(maps.get(version));
    }

    public Optional<Map.Entry<Long, ColumnIdentityMap>> floor(long version) {
      return Optional.ofNullable(maps.floorEntry(version));
    }

    public Optional<Map.Entry<Long, ColumnIdentityMap>> ceiling(long version) {
      return Optional.ofNullable(maps.ceilingEntry(version));
    }
  }

  Optional<ColumnIdentityMap> exact(long version);

  Optional<Map.Entry<Long, ColumnIdentityMap>> floor(long version);

  Optional<Map.Entry<Long, ColumnIdentityMap>> ceiling(long version);

  Optional<ColumnIdentityMap> tableMap();

  long highWaterMark();
}
