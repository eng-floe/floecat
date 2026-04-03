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

package ai.floedb.floecat.reconciler.jobs;

import java.util.Map;
import java.util.TreeMap;

public record ReconcileExecutionPolicy(
    ReconcileExecutionClass executionClass, String lane, Map<String, String> attributes) {
  private static final ReconcileExecutionPolicy DEFAULT_POLICY =
      new ReconcileExecutionPolicy(ReconcileExecutionClass.DEFAULT, "", Map.of());

  public ReconcileExecutionPolicy {
    executionClass = executionClass == null ? ReconcileExecutionClass.DEFAULT : executionClass;
    lane = lane == null ? "" : lane.trim();

    Map<String, String> normalized = new TreeMap<>();
    if (attributes != null) {
      for (var entry : attributes.entrySet()) {
        String key = entry.getKey();
        if (key == null || key.isBlank()) {
          continue;
        }
        normalized.put(key.trim(), entry.getValue() == null ? "" : entry.getValue().trim());
      }
    }
    attributes = Map.copyOf(normalized);
  }

  public static ReconcileExecutionPolicy defaults() {
    return DEFAULT_POLICY;
  }

  public static ReconcileExecutionPolicy of(
      ReconcileExecutionClass executionClass, String lane, Map<String, String> attributes) {
    if ((executionClass == null || executionClass == ReconcileExecutionClass.DEFAULT)
        && (lane == null || lane.isBlank())
        && (attributes == null || attributes.isEmpty())) {
      return DEFAULT_POLICY;
    }
    return new ReconcileExecutionPolicy(executionClass, lane, attributes);
  }

  public boolean isDefaultPolicy() {
    return executionClass == ReconcileExecutionClass.DEFAULT
        && lane.isBlank()
        && attributes.isEmpty();
  }
}
