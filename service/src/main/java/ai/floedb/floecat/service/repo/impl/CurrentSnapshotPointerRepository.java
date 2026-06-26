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

import ai.floedb.floecat.catalog.rpc.CurrentSnapshotPointer;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.CurrentSnapshotPointerKey;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class CurrentSnapshotPointerRepository {
  private final GenericResourceRepository<CurrentSnapshotPointer, CurrentSnapshotPointerKey> repo;

  @Inject
  public CurrentSnapshotPointerRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.CURRENT_SNAPSHOT_POINTER,
            CurrentSnapshotPointer::parseFrom,
            CurrentSnapshotPointer::toByteArray,
            "application/x-protobuf");
  }

  public Optional<CurrentSnapshotPointer> get(ResourceId tableId) {
    return repo.getByKey(key(tableId));
  }

  public boolean createIfAbsent(CurrentSnapshotPointer pointer) {
    return repo.createIfAbsent(pointer);
  }

  public boolean update(CurrentSnapshotPointer pointer, long expectedPointerVersion) {
    return repo.update(pointer, expectedPointerVersion);
  }

  public boolean delete(ResourceId tableId) {
    return repo.delete(key(tableId));
  }

  public boolean deleteWithPrecondition(ResourceId tableId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(key(tableId), expectedPointerVersion);
  }

  public MutationMeta metaFor(ResourceId tableId) {
    return repo.metaFor(key(tableId));
  }

  private static CurrentSnapshotPointerKey key(ResourceId tableId) {
    return new CurrentSnapshotPointerKey(tableId.getAccountId(), tableId.getId());
  }
}
