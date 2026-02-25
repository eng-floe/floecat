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
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository.CorruptionException;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public final class IdempotencyRepositoryImpl implements IdempotencyRepository {
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

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
  public boolean createPending(
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

    String uri = Keys.idempotencyBlobUri(accountId, key, "pending-" + UUID.randomUUID());
    blobs.put(uri, rec.toByteArray(), "application/x-protobuf");

    var pendingPointer =
        Pointer.newBuilder()
            .setKey(key)
            .setBlobUri(uri)
            .setExpiresAt(expiresAt)
            .setVersion(1L)
            .build();

    for (int i = 0; i < CAS_MAX; i++) {
      if (ptr.compareAndSet(key, 0L, pendingPointer)) {
        return true;
      }

      if (ptr.get(key).isPresent()) {
        blobs.delete(uri);
        return false;
      }
    }

    blobs.delete(uri);
    throw new StorageAbortRetryableException("idempotency pointer not yet visible: key=" + key);
  }

  @Override
  public void finalizeSuccess(
      String accountId,
      String key,
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

    String uri = Keys.idempotencyBlobUri(accountId, key, "success-" + UUID.randomUUID());
    blobs.put(uri, rec.toByteArray(), "application/x-protobuf");

    Pointer previous = null;
    boolean updated = false;
    for (int i = 0; i < CAS_MAX; i++) {
      var current = ptr.get(key).orElse(null);
      long expected = current != null ? current.getVersion() : 0L;
      var next =
          Pointer.newBuilder()
              .setKey(key)
              .setExpiresAt(expiresAt)
              .setBlobUri(uri)
              .setVersion(expected + 1)
              .build();
      if (ptr.compareAndSet(key, expected, next)) {
        previous = current;
        updated = true;
        break;
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

    var uri = p.get().getBlobUri();
    var ok = ptr.delete(key);
    blobs.delete(uri);
    blobs.deletePrefix(Keys.idempotencyBlobPrefixForPointerKey(key));

    return ok;
  }
}
