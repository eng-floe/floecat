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
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository.CorruptionException;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.GuardedBlobPrefixSweeper;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public final class IdempotencyRepositoryImpl implements IdempotencyRepository {
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;
  @Inject MarkerStore markerStore;

  private static final int CAS_MAX = 10;

  @Inject
  public IdempotencyRepositoryImpl(PointerStore ptr, BlobStore blobs) {
    this.ptr = ptr;
    this.blobs = blobs;
  }

  public IdempotencyRepositoryImpl() {}

  @Override
  public Optional<IdempotencyRecord> get(String key) {
    var p = ptr.get(key);
    if (p.isEmpty()) {
      return Optional.empty();
    }

    try {
      var bytes = blobs.get(p.get().getBlobUri());
      if (bytes == null) {
        throw new StorageAbortRetryableException(
            "idempotency blob not yet visible: " + p.get().getBlobUri());
      }
      return Optional.of(IdempotencyRecord.parseFrom(bytes));
    } catch (StorageNotFoundException nf) {
      throw new StorageAbortRetryableException(
          "idempotency blob not yet visible: " + p.get().getBlobUri());
    } catch (Exception e) {
      throw new CorruptionException(
          "failed to parse idempotency record: " + p.get().getBlobUri(), e);
    }
  }

  @Override
  public PendingClaim createPending(
      String accountId,
      String key,
      String opName,
      String requestHash,
      Timestamp createdAt,
      Timestamp expiresAt) {
    var rec =
        IdempotencyRecord.newBuilder()
            .setOpName(opName)
            .setRequestHash(requestHash)
            .setStatus(IdempotencyRecord.Status.PENDING)
            .setCreatedAt(createdAt)
            .setExpiresAt(expiresAt)
            .build();

    BatchGuard accountGuard = accountLiveGuard(accountId, false);
    String uri = Keys.idempotencyBlobUri(accountId, key, "pending-" + UUID.randomUUID());
    blobs.put(uri, rec.toByteArray(), "application/x-protobuf");

    String claimMarkerKey = key + "/claim/" + UUID.randomUUID();
    var pendingPointer =
        PointerReferences.asBlobPointer(
                Pointer.newBuilder().setKey(key).setExpiresAt(expiresAt).setVersion(1L), uri)
            .build();
    var claimMarker =
        PointerReferences.opaqueMarkerPointer(claimMarkerKey, "idempotency-pending-claim", 1L)
            .toBuilder()
            .setExpiresAt(expiresAt)
            .build();

    for (int i = 0; i < CAS_MAX; i++) {
      var ops = new java.util.ArrayList<PointerStore.CasOp>();
      ops.add(new PointerStore.CasUpsert(key, 0L, pendingPointer));
      ops.add(new PointerStore.CasUpsert(claimMarkerKey, 0L, claimMarker));
      ops.addAll(accountGuard.ops());
      if (ptr.compareAndSetBatch(ops)) {
        return new PendingClaim(true, 1L, claimMarkerKey, 1L);
      }

      if (accountGuard.reevaluate() == BatchGuard.Outcome.BROKEN) {
        blobs.delete(uri);
        throw new BaseResourceRepository.BatchGuardFailedException(
            "idempotency pending publication lost the race against " + accountGuard.describe());
      }
      if (ptr.get(key).isPresent()) {
        blobs.delete(uri);
        return new PendingClaim(false, 0L, "", 0L);
      }
    }

    blobs.delete(uri);
    throw new StorageAbortRetryableException("idempotency pointer not yet visible: key=" + key);
  }

  @Override
  public void finalizeSuccess(
      String accountId,
      String key,
      PendingClaim pendingClaim,
      String opName,
      String requestHash,
      ResourceId resourceId,
      MutationMeta meta,
      byte[] payloadBytes,
      Timestamp createdAt,
      Timestamp expiresAt) {
    var rec =
        IdempotencyRecord.newBuilder()
            .setOpName(opName)
            .setRequestHash(requestHash)
            .setStatus(IdempotencyRecord.Status.SUCCEEDED)
            .setResourceId(resourceId)
            .setMeta(meta)
            .setPayload(ByteString.copyFrom(payloadBytes))
            .setCreatedAt(createdAt)
            .setExpiresAt(expiresAt)
            .build();

    if (!pendingClaim.created()) {
      throw new IllegalArgumentException("cannot finalize an idempotency claim that was not won");
    }
    BatchGuard accountGuard = accountLiveGuard(accountId, true);
    Pointer previous = ptr.get(key).orElse(null);
    requireOwnedPending(
        key, pendingClaim, previous, ptr.get(pendingClaim.claimMarkerKey()).orElse(null));
    String uri = Keys.idempotencyBlobUri(accountId, key, "success-" + UUID.randomUUID());
    blobs.put(uri, rec.toByteArray(), "application/x-protobuf");

    boolean updated = false;
    for (int i = 0; i < CAS_MAX; i++) {
      var current = ptr.get(key).orElse(null);
      var claimMarker = ptr.get(pendingClaim.claimMarkerKey()).orElse(null);
      if (!ownsPending(pendingClaim, current, claimMarker)) {
        blobs.delete(uri);
        throw new BaseResourceRepository.BatchGuardFailedAfterWriteException(
            "idempotency pending claim disappeared before success publication: " + key);
      }
      var next =
          PointerReferences.asBlobPointer(
                  Pointer.newBuilder()
                      .setKey(key)
                      .setExpiresAt(expiresAt)
                      .setVersion(pendingClaim.pointerVersion() + 1L),
                  uri)
              .build();
      var ops = new java.util.ArrayList<PointerStore.CasOp>();
      ops.add(new PointerStore.CasUpsert(key, pendingClaim.pointerVersion(), next));
      ops.add(
          new PointerStore.CasDelete(
              pendingClaim.claimMarkerKey(), pendingClaim.claimMarkerVersion()));
      ops.addAll(accountGuard.ops());
      if (ptr.compareAndSetBatch(ops)) {
        previous = current;
        updated = true;
        break;
      }
      if (accountGuard.reevaluate() == BatchGuard.Outcome.BROKEN) {
        blobs.delete(uri);
        throw new BaseResourceRepository.BatchGuardFailedAfterWriteException(
            "idempotency success publication lost the race against " + accountGuard.describe());
      }
    }
    if (!updated) {
      blobs.delete(uri);
      throw new StorageAbortRetryableException("idempotency pointer not yet visible: key=" + key);
    }
    if (previous != null && !previous.getBlobUri().equals(uri)) {
      blobs.delete(previous.getBlobUri());
    }
  }

  @Override
  public boolean delete(String key) {
    var p = ptr.get(key);
    if (p.isEmpty()) {
      return true;
    }

    Pointer pointer = p.orElseThrow();
    boolean ok = ptr.compareAndDelete(key, pointer.getVersion());
    if (!ok) {
      return false;
    }
    blobs.delete(pointer.getBlobUri());
    return true;
  }

  @Override
  public boolean deletePending(String key, PendingClaim pendingClaim) {
    Pointer pointer = ptr.get(key).orElse(null);
    Pointer claimMarker = ptr.get(pendingClaim.claimMarkerKey()).orElse(null);
    if (!ownsPending(pendingClaim, pointer, claimMarker)) {
      return pointer == null && claimMarker == null;
    }
    boolean deleted =
        ptr.compareAndSetBatch(
            java.util.List.of(
                new PointerStore.CasDelete(key, pendingClaim.pointerVersion()),
                new PointerStore.CasDelete(
                    pendingClaim.claimMarkerKey(), pendingClaim.claimMarkerVersion())));
    if (deleted) {
      blobs.delete(pointer.getBlobUri());
    }
    return deleted;
  }

  @Override
  public CleanupResult deleteAccountResources(
      String accountId,
      BatchGuard accountGone,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    String prefix = Keys.idempotencyPrefixAccount(accountId);
    int pointersDeleted = 0;
    var seenTokens = new HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      for (var row : ptr.listPointersByPrefix(prefix, 1_000, token, next, true)) {
        if (BaseResourceRepository.deletePointerWithGuard(
            ptr, row, accountGone, deleteProgress.hasPriorWrite())) {
          pointersDeleted++;
          deleteProgress.recordWrite();
        }
      }
      token = next.toString();
      if (token.isBlank()) {
        break;
      }
      if (!seenTokens.add(token)) {
        throw new IllegalStateException(
            "idempotency pointer scan did not advance; repeated page token: " + token);
      }
    }
    int blobsDeleted = GuardedBlobPrefixSweeper.delete(blobs, prefix, accountGone, deleteProgress);
    return new CleanupResult(pointersDeleted, blobsDeleted);
  }

  private BatchGuard accountLiveGuard(String accountId, boolean afterCreatorWrite) {
    // Directly constructed repositories in storage tests have no lifecycle store. Platform-level
    // CreateAccount calls deliberately use a synthetic scope: there is no account pointer to pin,
    // and that scope is never removed by DeleteAccount.
    if (markerStore == null || IdempotencyGuard.PLATFORM_SCOPE.equals(accountId)) {
      return BatchGuard.NONE;
    }
    return markerStore
        .accountLiveGuard(accountId)
        .orElseThrow(
            () ->
                afterCreatorWrite
                    ? new BaseResourceRepository.BatchGuardFailedAfterWriteException(
                        "account disappeared before idempotency success publication: " + accountId)
                    : new BaseResourceRepository.BatchGuardFailedException(
                        "account disappeared before idempotency pending publication: "
                            + accountId));
  }

  private static void requireOwnedPending(
      String key, PendingClaim pendingClaim, Pointer current, Pointer claimMarker) {
    if (!ownsPending(pendingClaim, current, claimMarker)) {
      throw new BaseResourceRepository.BatchGuardFailedAfterWriteException(
          "idempotency pending claim disappeared before success publication: " + key);
    }
  }

  private static boolean ownsPending(
      PendingClaim pendingClaim, Pointer current, Pointer claimMarker) {
    return current != null
        && current.getVersion() == pendingClaim.pointerVersion()
        && claimMarker != null
        && claimMarker.getVersion() == pendingClaim.claimMarkerVersion();
  }
}
