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

package ai.floedb.floecat.connector.common.ndv;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class StaticOnceNdvProvider implements NdvProvider {
  private final Map<String, ColumnNdv> byName;
  private final AtomicBoolean done = new AtomicBoolean(false);

  public StaticOnceNdvProvider(Map<String, ColumnNdv> byName) {
    this.byName = byName;
  }

  @Override
  public void contributeNdv(String filePath, Map<String, ColumnNdv> sinks) {
    if (!done.compareAndSet(false, true)) {
      return;
    }

    if (byName == null || byName.isEmpty() || sinks == null || sinks.isEmpty()) {
      return;
    }

    for (var e : byName.entrySet()) {
      ColumnNdv sink = sinks.get(e.getKey());
      ColumnNdv from = e.getValue();
      if (sink == null || from == null) {
        continue;
      }

      if (sink.approx == null && from.approx != null) {
        sink.approx = from.approx;
      }

      if (from.sketches != null && !from.sketches.isEmpty()) {
        for (var sk : from.sketches) {
          if (sk == null || sk.data == null) {
            continue;
          }

          String t = sk.type == null ? "" : sk.type.toLowerCase(Locale.ROOT);

          if (t.contains("datasketches") && t.contains("theta")) {
            sink.mergeTheta(sk.data);
          } else {
            if (sink.sketches == null) {
              sink.sketches = new ArrayList<>();
            }

            sink.sketches.add(sk);
          }
        }
      }
    }
  }
}
