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

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.TargetStatsKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class StatsRepository implements StatsStore {

  private final GenericResourceRepository<TargetStatsRecord, TargetStatsKey> targetStatsRepo;

  @Inject
  public StatsRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.targetStatsRepo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.TARGET_STATS,
            TargetStatsRecord::parseFrom,
            TargetStatsRecord::toByteArray,
            "application/x-protobuf");
  }

  private static String targetStatsPrefix(ResourceId tableId, long snapshotId) {
    return Keys.snapshotTargetStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
  }

  private static String targetStatsPrefixWithStorageIdPrefix(
      ResourceId tableId, long snapshotId, String targetStorageIdPrefix) {
    return Keys.snapshotTargetColumnStatsPrefix(
        tableId.getAccountId(), tableId.getId(), snapshotId, targetStorageIdPrefix);
  }

  private static TargetStatsKey targetStatsLookupKey(
      ResourceId tableId, long snapshotId, StatsTarget target) {
    return new TargetStatsKey(
        tableId.getAccountId(),
        tableId.getId(),
        snapshotId,
        StatsTargetIdentity.storageId(target),
        "");
  }

  @Override
  public void putTargetStats(TargetStatsRecord value) {
    targetStatsRepo.create(value);
  }

  @Override
  public Optional<TargetStatsRecord> getTargetStats(
      ResourceId tableId, long snapshotId, StatsTarget target) {
    return targetStatsRepo.getByKey(targetStatsLookupKey(tableId, snapshotId, target));
  }

  @Override
  public boolean deleteTargetStats(ResourceId tableId, long snapshotId, StatsTarget target) {
    return targetStatsRepo.delete(targetStatsLookupKey(tableId, snapshotId, target));
  }

  private List<TargetStatsRecord> listTargetStats(
      ResourceId tableId, long snapshotId, int limit, String token, StringBuilder nextOut) {
    String prefix = targetStatsPrefix(tableId, snapshotId);
    return targetStatsRepo.listByPrefix(prefix, limit, token, nextOut);
  }

  private int countTargetStats(ResourceId tableId, long snapshotId) {
    String prefix = targetStatsPrefix(tableId, snapshotId);
    return targetStatsRepo.countByPrefix(prefix);
  }

  private List<TargetStatsRecord> listTargetStatsByStoragePrefix(
      ResourceId tableId,
      long snapshotId,
      String targetStorageIdPrefix,
      int limit,
      String token,
      StringBuilder nextOut) {
    String prefix =
        targetStatsPrefixWithStorageIdPrefix(tableId, snapshotId, targetStorageIdPrefix);
    return targetStatsRepo.listByPrefix(prefix, limit, token, nextOut);
  }

  private int countTargetStatsByStoragePrefix(
      ResourceId tableId, long snapshotId, String targetStorageIdPrefix) {
    String prefix =
        targetStatsPrefixWithStorageIdPrefix(tableId, snapshotId, targetStorageIdPrefix);
    return targetStatsRepo.countByPrefix(prefix);
  }

  @Override
  public StatsStorePage listTargetStats(
      ResourceId tableId,
      long snapshotId,
      Optional<StatsTargetType> targetType,
      int limit,
      String pageToken) {
    StringBuilder next = new StringBuilder();
    List<TargetStatsRecord> rows =
        targetType
            .map(
                type ->
                    listTargetStatsByStoragePrefix(
                        tableId,
                        snapshotId,
                        storagePrefixFor(type),
                        Math.max(1, limit),
                        pageToken,
                        next))
            .orElseGet(
                () -> listTargetStats(tableId, snapshotId, Math.max(1, limit), pageToken, next));
    return new StatsStorePage(rows, next.toString());
  }

  @Override
  public int countTargetStats(
      ResourceId tableId, long snapshotId, Optional<StatsTargetType> targetType) {
    return targetType
        .map(type -> countTargetStatsByStoragePrefix(tableId, snapshotId, storagePrefixFor(type)))
        .orElseGet(() -> countTargetStats(tableId, snapshotId));
  }

  @Override
  public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
    String colPrefix = targetStatsPrefix(tableId, snapshotId);
    String token = "";
    StringBuilder next = new StringBuilder();

    do {
      List<TargetStatsRecord> page = targetStatsRepo.listByPrefix(colPrefix, 200, token, next);
      for (TargetStatsRecord record : page) {
        TargetStatsKey key = Schemas.TARGET_STATS.keyFromValue.apply(record);
        targetStatsRepo.delete(key);
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return true;
  }

  @Override
  public MutationMeta metaForTargetStats(
      ResourceId tableId, long snapshotId, StatsTarget target, Timestamp nowTs) {
    return targetStatsRepo.metaFor(targetStatsLookupKey(tableId, snapshotId, target), nowTs);
  }

  private static String storagePrefixFor(StatsTargetType type) {
    return switch (type) {
      case TABLE -> StatsTargetIdentity.tableStorageIdPrefix();
      case COLUMN -> StatsTargetIdentity.columnStorageIdPrefix();
      case EXPRESSION -> StatsTargetIdentity.expressionStorageIdPrefix();
      case FILE -> StatsTargetIdentity.fileStorageIdPrefix();
    };
  }
}
