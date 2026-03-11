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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class MetadataListUtil {
  private MetadataListUtil() {}

  public static void upsertById(
      List<Map<String, Object>> entries, Map<String, Object> candidate, String idKey) {
    if (entries == null || candidate == null || candidate.isEmpty()) {
      return;
    }
    Object candidateId = candidate.get(idKey);
    if (candidateId == null) {
      entries.add(candidate);
      return;
    }
    for (int i = 0; i < entries.size(); i++) {
      Map<String, Object> existing = entries.get(i);
      if (existing == null) {
        continue;
      }
      if (Objects.equals(existing.get(idKey), candidateId)) {
        entries.set(i, candidate);
        return;
      }
    }
    entries.add(candidate);
  }

  public static List<Map<String, Object>> dedupeById(
      List<Map<String, Object>> entries, String idKey) {
    if (entries == null || entries.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    Set<Object> ids = new LinkedHashSet<>();
    for (int i = entries.size() - 1; i >= 0; i--) {
      Map<String, Object> entry = entries.get(i);
      if (entry == null || entry.isEmpty()) {
        continue;
      }
      Object id = entry.get(idKey);
      if (id != null && !ids.add(id)) {
        continue;
      }
      out.add(0, entry);
    }
    return out;
  }
}
