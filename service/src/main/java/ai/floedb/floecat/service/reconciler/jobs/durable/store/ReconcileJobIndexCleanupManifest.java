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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import java.util.LinkedHashSet;
import java.util.List;

public record ReconcileJobIndexCleanupManifest(
    List<String> indexPointerKeys, List<String> readyPointerKeys) {
  public static final ReconcileJobIndexCleanupManifest EMPTY =
      new ReconcileJobIndexCleanupManifest(List.of(), List.of());

  public ReconcileJobIndexCleanupManifest {
    indexPointerKeys = copyDistinct(indexPointerKeys);
    readyPointerKeys = copyDistinct(readyPointerKeys);
  }

  public boolean isEmpty() {
    return indexPointerKeys.isEmpty() && readyPointerKeys.isEmpty();
  }

  private static List<String> copyDistinct(List<String> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    LinkedHashSet<String> distinct = new LinkedHashSet<>();
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        distinct.add(value);
      }
    }
    return List.copyOf(distinct);
  }
}
