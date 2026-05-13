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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.SnapshotKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class SnapshotRepository {

  private final GenericResourceRepository<Snapshot, SnapshotKey> repo;
  private final TableRepository tableRepo;

  @Inject
  public SnapshotRepository(
      PointerStore pointerStore, BlobStore blobStore, TableRepository tableRepo) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.SNAPSHOT,
            Snapshot::parseFrom,
            Snapshot::toByteArray,
            "application/x-protobuf");
    this.tableRepo = tableRepo;
  }

  public void create(Snapshot snapshot) {
    repo.create(snapshot);
  }

  public boolean update(Snapshot snapshot, long expectedPointerVersion) {
    return repo.update(snapshot, expectedPointerVersion);
  }

  public boolean delete(ResourceId tableId, long snapshotId) {
    return repo.delete(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  public boolean deleteWithPrecondition(
      ResourceId tableId, long snapshotId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId),
        expectedPointerVersion);
  }

  public Optional<Snapshot> getById(ResourceId tableId, long snapshotId) {
    return repo.getByKey(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  public static String metadataLocation(Snapshot snapshot) {
    if (snapshot == null || !snapshot.hasMetadataLocation()) {
      return null;
    }
    String value = snapshot.getMetadataLocation().trim();
    return value.isBlank() ? null : value;
  }

  public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
    if (tableId == null) {
      return Optional.empty();
    }
    Optional<Table> table = tableRepo.getById(tableId);
    if (table.isEmpty()) {
      return Optional.empty();
    }
    String currentSnapshotId = table.get().getPropertiesMap().get("current-snapshot-id");
    if (currentSnapshotId == null || currentSnapshotId.isBlank()) {
      return Optional.empty();
    }
    long snapshotId;
    try {
      snapshotId = Long.parseLong(currentSnapshotId);
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
    if (snapshotId < 0) {
      return Optional.empty();
    }
    return getById(tableId, snapshotId);
  }

  public Optional<Snapshot> getAsOf(ResourceId tableId, Timestamp asOf) {
    String prefix = Keys.snapshotPointerByTimePrefix(tableId.getAccountId(), tableId.getId());
    long asOfMs = Timestamps.toMillis(asOf);

    String token = "";
    StringBuilder next = new StringBuilder();
    do {
      List<Snapshot> batch = repo.listByPrefix(prefix, 200, token, next);
      for (Snapshot snapshot : batch) {
        long createdMs = Timestamps.toMillis(snapshot.getUpstreamCreatedAt());
        if (createdMs <= asOfMs) {
          return Optional.of(snapshot);
        }
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return Optional.empty();
  }

  public List<Snapshot> list(
      ResourceId tableId, int limit, String pageToken, StringBuilder nextOut) {
    String prefix = Keys.snapshotPointerByIdPrefix(tableId.getAccountId(), tableId.getId());
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public List<Snapshot> listByTime(
      ResourceId tableId, int limit, String pageToken, StringBuilder nextOut) {
    String prefix = Keys.snapshotPointerByTimePrefix(tableId.getAccountId(), tableId.getId());
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(ResourceId tableId) {
    String prefix = Keys.snapshotPointerByIdPrefix(tableId.getAccountId(), tableId.getId());
    return repo.countByPrefix(prefix);
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId) {
    return repo.metaFor(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    return repo.metaFor(
        new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableId, long snapshotId) {
    return repo.metaForSafe(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }
}
