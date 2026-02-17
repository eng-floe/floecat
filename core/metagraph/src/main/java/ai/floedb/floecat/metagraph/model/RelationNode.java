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

package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.LinkedHashMap;
import java.util.Map;

/** Common behavior for every relation-like node (tables, views) that can carry column hints. */
public interface RelationNode extends GraphNode {

  ResourceId namespaceId();

  /** Column-level engine hints keyed by stable `columnId`. */
  default Map<Long, Map<EngineHintKey, EngineHint>> columnHints() {
    return Map.of();
  }

  /** Normalizes the provided column hint map and returns an immutable result. */
  static Map<Long, Map<EngineHintKey, EngineHint>> normalizeColumnHints(
      Map<Long, Map<EngineHintKey, EngineHint>> hints) {
    if (hints == null || hints.isEmpty()) {
      return Map.of();
    }
    Map<Long, Map<EngineHintKey, EngineHint>> normalized = new LinkedHashMap<>();
    for (Map.Entry<Long, Map<EngineHintKey, EngineHint>> entry : hints.entrySet()) {
      Map<EngineHintKey, EngineHint> value =
          entry.getValue() == null ? Map.of() : Map.copyOf(entry.getValue());
      normalized.put(entry.getKey(), value);
    }
    return Map.copyOf(normalized);
  }
}
