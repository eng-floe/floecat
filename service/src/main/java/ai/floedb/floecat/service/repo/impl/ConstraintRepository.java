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

import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.SnapshotConstraintsKey;
import ai.floedb.floecat.service.repo.util.ConstraintNormalizer;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class ConstraintRepository {

  private final GenericResourceRepository<SnapshotConstraints, SnapshotConstraintsKey> repo;

  @Inject
  public ConstraintRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.SNAPSHOT_CONSTRAINTS,
            SnapshotConstraints::parseFrom,
            SnapshotConstraints::toByteArray,
            "application/x-protobuf");
  }

  /**
   * Put constraints for a snapshot. Returns true when storage changed, false for exact-idempotent
   * replays.
   */
  public boolean putSnapshotConstraints(
      ResourceId tableId, long snapshotId, SnapshotConstraints value) {
    SnapshotConstraints normalized = normalizeForKey(tableId, snapshotId, value);
    SnapshotConstraintsKey key = key(tableId, snapshotId);
    Optional<SnapshotConstraints> current = repo.getByKey(key);
    if (current.isEmpty()) {
      repo.create(normalized);
      return true;
    }
    if (current.get().equals(normalized)) {
      return false;
    }
    MutationMeta meta = repo.metaFor(key);
    return repo.update(normalized, meta.getPointerVersion());
  }

  /**
   * Updates an existing constraints bundle using optimistic pointer-version CAS.
   *
   * <p>Returns false when the expected pointer version does not match current storage state.
   *
   * @throws BaseResourceRepository.NotFoundException when the target bundle is missing.
   */
  public boolean updateSnapshotConstraints(
      ResourceId tableId, long snapshotId, SnapshotConstraints value, long expectedPointerVersion) {
    SnapshotConstraints normalized = normalizeForKey(tableId, snapshotId, value);
    return repo.update(normalized, expectedPointerVersion);
  }

  /**
   * Creates a constraints bundle only when no bundle exists for the snapshot.
   *
   * <p>Returns false when a bundle already exists.
   */
  public boolean createSnapshotConstraintsIfAbsent(
      ResourceId tableId, long snapshotId, SnapshotConstraints value) {
    SnapshotConstraints normalized = normalizeForKey(tableId, snapshotId, value);
    return repo.createIfAbsent(normalized);
  }

  public Optional<SnapshotConstraints> getSnapshotConstraints(ResourceId tableId, long snapshotId) {
    return repo.getByKey(key(tableId, snapshotId));
  }

  public boolean deleteSnapshotConstraints(ResourceId tableId, long snapshotId) {
    return repo.delete(key(tableId, snapshotId));
  }

  public List<SnapshotConstraints> listSnapshotConstraints(
      ResourceId tableId, int limit, String token, StringBuilder nextOut) {
    return repo.listByPrefix(prefixFor(tableId), limit, token, nextOut);
  }

  public int countSnapshotConstraints(ResourceId tableId) {
    return repo.countByPrefix(prefixFor(tableId));
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId) {
    return repo.metaFor(key(tableId, snapshotId));
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    return repo.metaFor(key(tableId, snapshotId), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableId, long snapshotId) {
    return repo.metaForSafe(key(tableId, snapshotId));
  }

  /** Canonical lookup key — sha256 is empty because canonical pointer lookups don't use it. */
  private static SnapshotConstraintsKey key(ResourceId tableId, long snapshotId) {
    return new SnapshotConstraintsKey(tableId.getAccountId(), tableId.getId(), snapshotId, "");
  }

  private static String prefixFor(ResourceId tableId) {
    return Keys.snapshotConstraintsPointerPrefix(tableId.getAccountId(), tableId.getId());
  }

  private static SnapshotConstraints normalizeForKey(
      ResourceId tableId, long snapshotId, SnapshotConstraints value) {
    SnapshotConstraints withIdentity =
        value.toBuilder().setTableId(tableId).setSnapshotId(snapshotId).build();
    return ConstraintNormalizer.normalize(withIdentity);
  }
}
