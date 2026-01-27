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

package ai.floedb.floecat.extensions.floedb.validation;

import ai.floedb.floecat.systemcatalog.engine.VersionIntervals;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

record IntervalIndex<K>(Map<K, List<VersionIntervals.VersionInterval>> coverageByKey) {
  static <T extends Message, K> IntervalIndex<K> fromDecoded(
      List<? extends ValidationSupport.DecodedRule<T>> decodedRules,
      Function<? super ValidationSupport.DecodedRule<T>, K> keyFn) {
    if (decodedRules == null || decodedRules.isEmpty()) {
      return new IntervalIndex<>(Map.of());
    }

    Map<K, List<VersionIntervals.VersionInterval>> grouped = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<T> decoded : decodedRules) {
      K key = keyFn.apply(decoded);
      if (key == null) {
        continue;
      }
      grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(decoded.interval());
    }

    Map<K, List<VersionIntervals.VersionInterval>> coverage = new LinkedHashMap<>();
    grouped.forEach((key, intervals) -> coverage.put(key, VersionIntervals.union(intervals)));
    return new IntervalIndex<>(Map.copyOf(coverage));
  }

  boolean covers(K key, VersionIntervals.VersionInterval target) {
    if (target == null) {
      return true;
    }
    List<VersionIntervals.VersionInterval> coverage = coverageByKey.get(key);
    if (coverage == null || coverage.isEmpty()) {
      return false;
    }
    return VersionIntervals.covers(coverage, target);
  }

  List<VersionIntervals.VersionInterval> coverage(K key) {
    return coverageByKey.getOrDefault(key, List.of());
  }
}
