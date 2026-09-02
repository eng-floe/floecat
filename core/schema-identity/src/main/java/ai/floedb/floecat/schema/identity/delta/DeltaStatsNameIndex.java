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

package ai.floedb.floecat.schema.identity.delta;

import ai.floedb.floecat.schema.identity.SchemaNode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Struct-descent trie over the name spellings a Delta statistics writer could have emitted.
 *
 * <p>A trie rather than a flat map of whole paths: when the mapping mode is unknown both spellings
 * of every segment are accepted, and enumerating whole paths would be exponential in nesting depth.
 *
 * <p>Delta collects no statistics inside a collection, so the trie stops at any array or map.
 */
final class DeltaStatsNameIndex {

  private final Map<String, DeltaStatsNameIndex> children = new LinkedHashMap<>();
  private SchemaNode node;

  DeltaStatsNameIndex child(String logicalName, String physicalName, ColumnMappingMode mode) {
    DeltaStatsNameIndex child = new DeltaStatsNameIndex();
    if (mode == null) {
      // Mode unknown: accept either spelling, physical winning a collision. Delta only mints a
      // distinct physical name when mapping is on, and that is when statistics are physical.
      children.putIfAbsent(logicalName, child);
      children.put(physicalName, child);
    } else if (mode.isEnabled()) {
      children.put(physicalName, child);
    } else {
      children.put(logicalName, child);
    }
    return child;
  }

  void setNode(SchemaNode node) {
    this.node = node;
  }

  Optional<SchemaNode> resolve(List<String> names) {
    DeltaStatsNameIndex current = this;
    for (String segment : names) {
      current = current.children.get(segment);
      if (current == null) {
        return Optional.empty();
      }
    }
    return Optional.ofNullable(current.node);
  }
}
