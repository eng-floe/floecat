package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.IdempotencyRepository;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository.CorruptionException;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
import ai.floedb.metacat.storage.errors.StorageAbortRetryableException;
import ai.floedb.metacat.storage.errors.StorageNotFoundException;
import ai.floedb.metacat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

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
      String tenantId,
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

    String uri = Keys.idempotencyBlobUri(tenantId, key);

    blobs.put(uri, rec.toByteArray(), "application/x-protobuf");

    for (int i = 0; i < CAS_MAX; i++) {
      long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
      var next =
          Pointer.newBuilder()
              .setKey(key)
              .setBlobUri(uri)
              .setExpiresAt(expiresAt)
              .setVersion(expected + 1)
              .build();
      if (ptr.compareAndSet(key, expected, next)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void finalizeSuccess(
      String tenantId,
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

    String uri = Keys.idempotencyBlobUri(tenantId, key);
    blobs.put(uri, rec.toByteArray(), "application/x-protobuf");

    for (int i = 0; i < CAS_MAX; i++) {
      long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
      var next =
          Pointer.newBuilder()
              .setKey(key)
              .setExpiresAt(expiresAt)
              .setBlobUri(uri)
              .setVersion(expected + 1)
              .build();
      if (ptr.compareAndSet(key, expected, next)) {
        break;
      }
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

    return ok;
  }
}
