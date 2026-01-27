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
 * distributed on the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.extensions.floedb.validation;

import ai.floedb.floecat.systemcatalog.engine.VersionIntervals;
import java.util.List;
import java.util.Map;

record Lookup(Map<String, List<TypeInfo>> typesByName, Map<Integer, List<TypeInfo>> typesByOid) {
  TypeInfo find(String canonical, VersionIntervals.VersionInterval interval) {
    List<TypeInfo> candidates = typesByName.get(canonical);
    if (candidates == null || candidates.isEmpty()) {
      return null;
    }
    if (interval == null) {
      return candidates.get(0);
    }
    for (TypeInfo info : candidates) {
      if (VersionIntervals.covers(List.of(info.interval()), interval)) {
        return info;
      }
    }
    return null;
  }

  TypeInfo find(String canonical) {
    return find(canonical, null);
  }

  List<TypeInfo> byOid(int oid) {
    return typesByOid.getOrDefault(oid, List.of());
  }

  List<TypeInfo> all() {
    return typesByOid.values().stream().flatMap(List::stream).toList();
  }
}
