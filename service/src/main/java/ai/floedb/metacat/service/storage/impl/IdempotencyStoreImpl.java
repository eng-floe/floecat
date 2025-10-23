package ai.floedb.metacat.service.storage.impl;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.PointerStore;
import ai.floedb.metacat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public final class IdempotencyStoreImpl implements IdempotencyStore {
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  @Override
  public Optional<IdempotencyRecord> get(String key) {
    var p = ptr.get(key);
    if (p.isEmpty()) {
      return Optional.empty();
    }

    var hdr = blobs.head(p.get().getBlobUri());
    if (hdr.isEmpty()) {
      return Optional.empty();
    }

    try {
      var bytes = blobs.get(p.get().getBlobUri());
      return Optional.of(IdempotencyRecord.parseFrom(bytes));
    } catch (Exception e) {
      throw new IllegalStateException(
          "failed to parse idempotency record: " + p.get().getBlobUri(), e);
    }
  }

  @Override
  public boolean createPending(
      String key, String opName, String requestHash, Timestamp createdAt, Timestamp expiresAt) {
    var rec =
        IdempotencyRecord.newBuilder()
            .setOpName(opName)
            .setRequestHash(requestHash)
            .setStatus(IdempotencyRecord.Status.PENDING)
            .setCreatedAt(createdAt)
            .setExpiresAt(expiresAt)
            .build();

    String uri = Keys.memUriFor(key, "record.pb");

    blobs.put(uri, rec.toByteArray(), "application/x-protobuf");

    for (int i = 0; i < BaseRepository.CAS_MAX; i++) {
      long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
      var next = Pointer.newBuilder().setKey(key).setBlobUri(uri).setVersion(expected + 1).build();
      if (ptr.compareAndSet(key, expected, next)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void finalizeSuccess(
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

    String uri = Keys.memUriFor(key, "record.pb");
    blobs.put(uri, rec.toByteArray(), "application/x-protobuf");

    for (int i = 0; i < BaseRepository.CAS_MAX; i++) {
      long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
      var next = Pointer.newBuilder().setKey(key).setBlobUri(uri).setVersion(expected + 1).build();
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
