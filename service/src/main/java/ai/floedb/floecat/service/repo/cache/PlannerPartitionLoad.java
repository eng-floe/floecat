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

package ai.floedb.floecat.service.repo.cache;

import ai.floedb.floecat.cache.CacheWeights;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/**
 * Builds one account's planner image from durable storage, within a byte budget.
 *
 * <p>Two phases, because the planner keys under a table are not a prefix: the table id sits above
 * {@code root/current} and {@code snapshots/current}, so scanning to reach them would mean reading
 * the whole table subtree -- every snapshot row and every stats generation -- to keep four rows.
 * The scan therefore covers the prefix families, and the per-table keys are fetched by id.
 *
 * <p>Returns an image rather than filling a partition: the caller holds the locks and owns the
 * registry, and a refusal must leave no partial state behind.
 */
final class PlannerPartitionLoad {
  private static final int PAGE_SIZE = 1_000;
  // Two keys per table, kept under the store's batch-get ceiling.
  private static final int PER_TABLE_BATCH_KEYS = 100;

  private final PointerStore durable;
  private final PlanningPointerIndex.Policy policy;

  PlannerPartitionLoad(PointerStore durable, PlanningPointerIndex.Policy policy) {
    this.durable = durable;
    this.policy = policy;
  }

  /**
   * The image, or a refusal. {@code entries} is empty on a refusal, so a caller that ignores the
   * flag still cannot publish a partial account.
   */
  record Result(NavigableMap<String, Pointer> entries, long bytes, boolean refusedForSize) {
    static Result refused() {
      return new Result(new TreeMap<>(), 0, true);
    }
  }

  /**
   * @param otherAccountBytes what every other resident account holds, read as the scan proceeds so
   *     a load completing alongside this one tightens the budget rather than being missed.
   */
  Result load(String partitionKey, LongSupplier otherAccountBytes) {
    Admission admission = new Admission(otherAccountBytes);
    List<String> tableIds = new ArrayList<>();
    String tablesById = Keys.tablePointerByIdPrefix(partitionKey);

    for (String prefix : PlanningPointerIndex.loadPrefixes(partitionKey)) {
      String token = "";
      do {
        StringBuilder next = new StringBuilder();
        for (Pointer pointer :
            durable.listPointersByPrefixConsistent(prefix, PAGE_SIZE, token, next)) {
          String key = pointer.getKey();
          if (!partitionKey.equals(PlanningPointerIndex.partitionFor(key))
              || !PlanningPointerIndex.isPlanningKey(key)) {
            continue;
          }
          if (!admission.admit(pointer)) {
            return Result.refused();
          }
          if (key.startsWith(tablesById)) {
            // Only a bare id. A blank one would make Keys reject the per-table key and fail every
            // retry identically, so one malformed row would disable the account for good.
            String segment = key.substring(tablesById.length());
            if (!segment.isBlank() && segment.indexOf('/') < 0) {
              tableIds.add(Keys.decodeSegment(segment));
            }
          }
        }
        String newToken = next.toString();
        if (!newToken.isBlank() && newToken.equals(token)) {
          throw new IllegalStateException("stagnant pointer index token");
        }
        token = newToken;
      } while (!token.isBlank());
    }

    return admitPerTableKeys(partitionKey, tableIds, admission)
        ? new Result(admission.loaded, admission.bytes, false)
        : Result.refused();
  }

  private boolean admitPerTableKeys(
      String partitionKey, List<String> tableIds, Admission admission) {
    List<String> batch = new ArrayList<>(PER_TABLE_BATCH_KEYS);
    for (int i = 0; i < tableIds.size(); i++) {
      batch.addAll(Keys.plannerTableKeys(partitionKey, tableIds.get(i)));
      boolean last = i == tableIds.size() - 1;
      if (batch.size() < PER_TABLE_BATCH_KEYS && !last) {
        continue;
      }
      for (Pointer pointer : durable.getBatchConsistent(batch).values()) {
        if (PlanningPointerIndex.isPlanningKey(pointer.getKey()) && !admission.admit(pointer)) {
          return false;
        }
      }
      batch.clear();
    }
    return true;
  }

  /**
   * The image as it fills, and the budget it fills against. Weighing on the way in means a refusal
   * costs the budget rather than the account.
   */
  private final class Admission {
    private final TreeMap<String, Pointer> loaded = new TreeMap<>();
    private final LongSupplier otherAccountBytes;
    private long bytes;

    Admission(LongSupplier otherAccountBytes) {
      this.otherAccountBytes = otherAccountBytes;
    }

    boolean admit(Pointer pointer) {
      bytes += CacheWeights.entry(pointer, 2L * pointer.getKey().length());
      // One account must fit its own cap, and must still fit beside the accounts already resident:
      // a per-account cap alone is unbounded in the number of accounts.
      if (bytes > policy.maxBytesPerAccount()
          || otherAccountBytes.getAsLong() + bytes > policy.maxBytesTotal()) {
        return false;
      }
      loaded.put(pointer.getKey(), pointer);
      return true;
    }
  }
}
