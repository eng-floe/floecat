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

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

final class OverlayPointerPager {
  private static final int COUNT_PAGE_SIZE = 500;

  private OverlayPointerPager() {}

  static int count(PointerStore pointerStore, List<GenerationScan> generations) {
    int count = 0;
    String afterTargetKey = "";
    while (true) {
      Page page = page(pointerStore, generations, COUNT_PAGE_SIZE, afterTargetKey);
      int pageSize = Math.min(page.candidates().size(), COUNT_PAGE_SIZE);
      count = Math.addExact(count, pageSize);
      if (!page.hasMore() || pageSize == 0) {
        return count;
      }
      afterTargetKey = page.candidates().get(pageSize - 1).targetKey();
    }
  }

  static Page page(
      PointerStore pointerStore,
      List<GenerationScan> generations,
      int limit,
      String afterTargetKey) {
    int effectiveLimit = Math.max(1, limit);
    int fetchLimit = effectiveLimit == Integer.MAX_VALUE ? effectiveLimit : effectiveLimit + 1;
    TreeMap<String, Pointer> pointersByTarget = new TreeMap<>();
    boolean hasMore = false;
    for (GenerationScan generation : generations) {
      String backendToken =
          afterTargetKey == null || afterTargetKey.isBlank()
              ? ""
              : pointerStore.pageTokenAfterKey(generation.generationPrefix() + afterTargetKey);
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              generation.listPrefix(), fetchLimit, backendToken, next);
      hasMore |= !next.isEmpty();
      for (Pointer pointer : pointers) {
        if (!pointer.getKey().startsWith(generation.generationPrefix())) {
          throw new IllegalStateException(
              "overlay pointer is outside its generation prefix: " + pointer.getKey());
        }
        String targetKey = pointer.getKey().substring(generation.generationPrefix().length());
        pointersByTarget.putIfAbsent(targetKey, pointer);
        if (pointersByTarget.size() > fetchLimit) {
          pointersByTarget.pollLastEntry();
          hasMore = true;
        }
      }
    }
    List<Candidate> candidates = new ArrayList<>(pointersByTarget.size());
    pointersByTarget.forEach(
        (targetKey, pointer) -> candidates.add(new Candidate(targetKey, pointer)));
    hasMore |= candidates.size() > effectiveLimit;
    return new Page(List.copyOf(candidates), hasMore);
  }

  record GenerationScan(String generationPrefix, String listPrefix) {}

  record Candidate(String targetKey, Pointer pointer) {}

  record Page(List<Candidate> candidates, boolean hasMore) {}
}
