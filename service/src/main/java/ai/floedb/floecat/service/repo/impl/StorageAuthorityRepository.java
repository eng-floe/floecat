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

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.StorageAuthorityKey;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.CredentialCleanupState;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@ApplicationScoped
public class StorageAuthorityRepository {

  private final GenericResourceRepository<StorageAuthority, StorageAuthorityKey> repo;
  private final PointerStore pointerStore;

  @Inject
  public StorageAuthorityRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.pointerStore = pointerStore;
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.STORAGE_AUTHORITY,
            StorageAuthority::parseFrom,
            StorageAuthority::toByteArray,
            "application/x-protobuf");
  }

  public void create(StorageAuthority authority) {
    repo.create(authority);
  }

  public void create(StorageAuthority authority, BatchGuard guard) {
    repo.create(authority, guard);
  }

  public boolean update(StorageAuthority authority, long expectedPointerVersion) {
    return repo.update(authority, expectedPointerVersion);
  }

  public boolean update(
      StorageAuthority authority, long expectedPointerVersion, BatchGuard accountLiveGuard) {
    return repo.update(authority, expectedPointerVersion, accountLiveGuard);
  }

  public boolean delete(ResourceId authorityId) {
    return repo.delete(new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()));
  }

  public boolean delete(ResourceId authorityId, BatchGuard guard) {
    return repo.delete(
        new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()), guard);
  }

  public boolean deleteWithPrecondition(ResourceId authorityId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()),
        expectedPointerVersion);
  }

  public boolean deleteWithPrecondition(
      ResourceId authorityId, long expectedPointerVersion, BatchGuard guard) {
    return repo.deleteWithPrecondition(
        new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()),
        expectedPointerVersion,
        guard);
  }

  public Optional<StorageAuthority> getById(ResourceId authorityId) {
    return repo.getByKey(new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()));
  }

  public Optional<StorageAuthority> getByName(String accountId, String displayName) {
    return repo.get(Keys.storageAuthorityPointerByName(accountId, displayName));
  }

  public List<StorageAuthority> list(
      String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(
        Keys.storageAuthorityPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public int count(String accountId) {
    return repo.countByPrefix(Keys.storageAuthorityPointerByNamePrefix(accountId));
  }

  public MutationMeta metaFor(ResourceId authorityId) {
    return repo.metaFor(new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()));
  }

  public MutationMeta metaFor(ResourceId authorityId, Timestamp nowTs) {
    return repo.metaFor(
        new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId authorityId) {
    return repo.metaForSafe(
        new StorageAuthorityKey(authorityId.getAccountId(), authorityId.getId()));
  }

  /** Durable handle for deleting an authority's external credentials after its pointer is gone. */
  public record CredentialCleanup(ResourceId authorityId, String pointerKey, long pointerVersion) {}

  public CredentialCleanup prepareCredentialCleanup(ResourceId authorityId) {
    String key =
        Keys.storageAuthorityCredentialCleanupPointer(
            authorityId.getAccountId(), authorityId.getId());
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var existing = pointerStore.get(key).orElse(null);
      if (existing != null) {
        if (CredentialCleanupState.isWriting(existing)) {
          throw new BaseResourceRepository.AbortRetryableException(
              "storage authority credential write still in flight for: " + authorityId.getId());
        }
        return new CredentialCleanup(authorityId, key, existing.getVersion());
      }
      var marker = PointerReferences.opaqueMarkerPointer(key, authorityId.getId(), 1L);
      if (pointerStore.compareAndSetBatch(List.of(new PointerStore.CasUpsert(key, 0L, marker)))) {
        return new CredentialCleanup(authorityId, key, 1L);
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "storage authority credential cleanup staging contended for: " + key);
  }

  public CredentialCleanupState.Write beginCredentialWrite(
      ResourceId authorityId, long expectedPointerVersion, BatchGuard accountLive) {
    String key =
        Keys.storageAuthorityCredentialCleanupPointer(
            authorityId.getAccountId(), authorityId.getId());
    String authorityPointer =
        Keys.storageAuthorityPointerById(authorityId.getAccountId(), authorityId.getId());
    var resourceState =
        CredentialCleanupState.pointerVersionGuard(
            pointerStore,
            authorityPointer,
            expectedPointerVersion,
            "storage authority " + authorityId.getId());
    return CredentialCleanupState.begin(
        pointerStore, key, authorityId.getId(), BatchGuard.all(accountLive, resourceState));
  }

  public BatchGuard credentialWriteCommitGuard(CredentialCleanupState.Write write) {
    return CredentialCleanupState.commitGuard(pointerStore, write);
  }

  public void abortCredentialWrite(CredentialCleanupState.Write write) {
    CredentialCleanupState.abort(pointerStore, write);
  }

  public void abortCredentialCreate(CredentialCleanupState.Write write) {
    CredentialCleanupState.abortCreate(pointerStore, write);
  }

  public boolean credentialWriteCommitted(CredentialCleanupState.Write write) {
    return CredentialCleanupState.committed(pointerStore, write);
  }

  public BatchGuard credentialCleanupReadyGuard(ResourceId authorityId) {
    String key =
        Keys.storageAuthorityCredentialCleanupPointer(
            authorityId.getAccountId(), authorityId.getId());
    return CredentialCleanupState.readyGuard(
        pointerStore, key, authorityId.getId(), "storage authority " + authorityId.getId());
  }

  /** Returns a durable cleanup left by an earlier delete attempt, without creating a new one. */
  public Optional<CredentialCleanup> pendingCredentialCleanup(ResourceId authorityId) {
    String key =
        Keys.storageAuthorityCredentialCleanupPointer(
            authorityId.getAccountId(), authorityId.getId());
    return pointerStore
        .get(key)
        .map(pointer -> new CredentialCleanup(authorityId, key, pointer.getVersion()));
  }

  /**
   * Claims a task only while the authority pointer is absent and the caller's guard still holds.
   */
  public Optional<CredentialCleanup> claimCredentialCleanup(
      CredentialCleanup cleanup, BatchGuard guard) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var current = pointerStore.get(cleanup.pointerKey()).orElse(null);
      if (current == null) {
        return Optional.empty();
      }
      if (CredentialCleanupState.isWriting(current)) {
        throw new BaseResourceRepository.AbortRetryableException(
            "storage authority credential write still in flight for: "
                + cleanup.authorityId().getId());
      }
      String authorityPointer =
          Keys.storageAuthorityPointerById(
              cleanup.authorityId().getAccountId(), cleanup.authorityId().getId());
      var claimed =
          PointerReferences.opaqueMarkerPointer(
              cleanup.pointerKey(), cleanup.authorityId().getId(), current.getVersion() + 1L);
      var ops = new java.util.ArrayList<PointerStore.CasOp>();
      ops.add(new PointerStore.CasUpsert(cleanup.pointerKey(), current.getVersion(), claimed));
      ops.add(new PointerStore.CasCheckAbsent(authorityPointer));
      ops.addAll(guard.ops());
      if (pointerStore.compareAndSetBatch(ops)) {
        return Optional.of(
            new CredentialCleanup(
                cleanup.authorityId(), cleanup.pointerKey(), claimed.getVersion()));
      }
      if (guard.reevaluate() == BatchGuard.Outcome.BROKEN) {
        throw new BaseResourceRepository.BatchGuardFailedException(
            "storage authority credential cleanup lost the race against " + guard.describe());
      }
      if (pointerStore.get(authorityPointer).isPresent()) {
        return Optional.empty();
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "storage authority credential cleanup claim contended for: " + cleanup.pointerKey());
  }

  public void completeCredentialCleanup(CredentialCleanup cleanup) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var current = pointerStore.get(cleanup.pointerKey()).orElse(null);
      if (current == null) {
        return;
      }
      if (pointerStore.compareAndSetBatch(
          List.of(new PointerStore.CasDelete(cleanup.pointerKey(), current.getVersion())))) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "storage authority credential cleanup completion contended for: " + cleanup.pointerKey());
  }

  public void forEachCredentialCleanup(String accountId, Consumer<CredentialCleanup> action) {
    repo.forEachRefByPrefixConsistent(
        Keys.storageAuthorityCredentialCleanupPrefix(accountId),
        pointer -> {
          String authorityId = Keys.extractLastSegment(pointer.getKey());
          action.accept(
              new CredentialCleanup(
                  ResourceId.newBuilder()
                      .setAccountId(accountId)
                      .setId(authorityId)
                      .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
                      .build(),
                  pointer.getKey(),
                  pointer.getVersion()));
        });
  }

  public void forEachId(String accountId, Consumer<ResourceId> action) {
    repo.forEachRefByPrefixConsistent(
        Keys.storageAuthorityPointerByIdPrefix(accountId),
        pointer ->
            action.accept(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(Keys.extractLastSegment(pointer.getKey()))
                    .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
                    .build()));
  }

  public int deleteResidualRows(String accountId, BatchGuard accountGone) {
    return deleteResidualRows(
        accountId, accountGone, new BaseResourceRepository.GuardedDeleteProgress());
  }

  public int deleteResidualRows(
      String accountId,
      BatchGuard accountGone,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    return repo.deleteByPrefix(
            Keys.storageAuthorityPointerByIdPrefix(accountId), accountGone, deleteProgress)
        + repo.deleteByPrefix(
            Keys.storageAuthorityPointerByNamePrefix(accountId), accountGone, deleteProgress);
  }
}
