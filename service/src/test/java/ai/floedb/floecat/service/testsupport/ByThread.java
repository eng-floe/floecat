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

package ai.floedb.floecat.service.testsupport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

final class ByThread {
  private final ConcurrentMap<String, AtomicInteger> counts = new ConcurrentHashMap<>();

  void record() {
    record(1);
  }

  void record(int amount) {
    if (amount <= 0) {
      return;
    }
    counts
        .computeIfAbsent(Thread.currentThread().getName(), n -> new AtomicInteger())
        .addAndGet(amount);
  }

  void appendTo(StringBuilder out, String prefix) {
    if (counts.isEmpty()) {
      return;
    }
    counts.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            e ->
                out.append(prefix)
                    .append(e.getKey())
                    .append(' ')
                    .append(e.getValue())
                    .append('\n'));
  }

  void clear() {
    counts.clear();
  }
}
