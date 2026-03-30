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

package ai.floedb.floecat.service.it.stats;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import com.google.protobuf.Timestamp;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Alternative
@Priority(1)
@ApplicationScoped
public class TestOverrideStatsStore implements StatsStore {
  private static final AtomicInteger PUT_COUNT = new AtomicInteger();
  private static final AtomicInteger GET_COUNT = new AtomicInteger();
  private static final AtomicInteger LIST_COUNT = new AtomicInteger();

  private final ConcurrentHashMap<StoreKey, TargetStatsRecord> records = new ConcurrentHashMap<>();

  public static void resetCounters() {
    PUT_COUNT.set(0);
    GET_COUNT.set(0);
    LIST_COUNT.set(0);
  }

  public static int putCount() {
    return PUT_COUNT.get();
  }

  public static int getCount() {
    return GET_COUNT.get();
  }

  @Override
  public void putTargetStats(TargetStatsRecord value) {
    PUT_COUNT.incrementAndGet();
    records.put(StoreKey.of(value.getTableId(), value.getSnapshotId(), value.getTarget()), value);
  }

  @Override
  public Optional<TargetStatsRecord> getTargetStats(
      ResourceId tableId, long snapshotId, StatsTarget target) {
    GET_COUNT.incrementAndGet();
    return Optional.ofNullable(records.get(StoreKey.of(tableId, snapshotId, target)));
  }

  @Override
  public boolean deleteTargetStats(ResourceId tableId, long snapshotId, StatsTarget target) {
    return records.remove(StoreKey.of(tableId, snapshotId, target)) != null;
  }

  @Override
  public StatsStorePage listTargetStats(
      ResourceId tableId,
      long snapshotId,
      Optional<StatsTargetType> targetType,
      int limit,
      String pageToken) {
    LIST_COUNT.incrementAndGet();
    int offset = decodeOffset(pageToken);
    int pageSize = Math.max(1, limit);

    List<TargetStatsRecord> filtered =
        records.values().stream()
            .filter(r -> r.getTableId().equals(tableId) && r.getSnapshotId() == snapshotId)
            .filter(r -> targetType.map(kind -> matches(kind, r.getTarget())).orElse(true))
            .sorted(Comparator.comparing(r -> StatsTargetIdentity.storageId(r.getTarget())))
            .toList();

    if (offset >= filtered.size()) {
      return new StatsStorePage(List.of(), "");
    }

    int end = Math.min(offset + pageSize, filtered.size());
    List<TargetStatsRecord> page = new ArrayList<>(filtered.subList(offset, end));
    String next = end < filtered.size() ? Integer.toString(end) : "";
    return new StatsStorePage(page, next);
  }

  @Override
  public int countTargetStats(
      ResourceId tableId, long snapshotId, Optional<StatsTargetType> targetType) {
    return Math.toIntExact(
        records.values().stream()
            .filter(r -> r.getTableId().equals(tableId) && r.getSnapshotId() == snapshotId)
            .filter(r -> targetType.map(kind -> matches(kind, r.getTarget())).orElse(true))
            .count());
  }

  @Override
  public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
    records.keySet().removeIf(k -> k.tableId().equals(tableId) && k.snapshotId() == snapshotId);
    return true;
  }

  @Override
  public MutationMeta metaForTargetStats(
      ResourceId tableId, long snapshotId, StatsTarget target, Timestamp nowTs) {
    return MutationMeta.newBuilder().setUpdatedAt(nowTs).setPointerVersion(1L).build();
  }

  private static int decodeOffset(String pageToken) {
    if (pageToken == null || pageToken.isBlank()) {
      return 0;
    }
    try {
      return Math.max(0, Integer.parseInt(pageToken));
    } catch (NumberFormatException ignored) {
      return 0;
    }
  }

  private static boolean matches(StatsTargetType kind, StatsTarget target) {
    return switch (kind) {
      case TABLE -> target.hasTable();
      case COLUMN -> target.hasColumn();
      case EXPRESSION -> target.hasExpression();
      case FILE -> target.hasFile();
    };
  }

  private record StoreKey(ResourceId tableId, long snapshotId, String storageId) {
    private static StoreKey of(ResourceId tableId, long snapshotId, StatsTarget target) {
      return new StoreKey(tableId, snapshotId, StatsTargetIdentity.storageId(target));
    }
  }
}
