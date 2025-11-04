package ai.floedb.metacat.service.common;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.IdempotencyRepository;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.storage.errors.StorageAbortRetryableException;
import ai.floedb.metacat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public final class IdempotencyGuard {
  public record CreateResult<T>(T resource, ResourceId resourceId) {}

  public static <T> T runOnce(
      String tenantId,
      String opName,
      String idempotencyKey,
      byte[] requestBytes,
      Supplier<CreateResult<T>> creator,
      Function<T, MutationMeta> metaExtractor,
      Function<T, byte[]> serializer,
      Function<byte[], T> parser,
      IdempotencyRepository store,
      long ttlSeconds,
      Timestamp now,
      Supplier<String> corrId) {
    if (idempotencyKey == null || idempotencyKey.isBlank()) {
      var result = creator.get();
      return result.resource();
    }

    String key = Keys.idempotencyKey(tenantId, opName, idempotencyKey);
    String requestHash = sha256B64(requestBytes);

    var existing = store.get(key);
    if (existing.isPresent()) {
      var rec = existing.get();
      if (!rec.getRequestHash().equals(requestHash)) {
        throw GrpcErrors.conflict(
            corrId.get(), "idempotency_mismatch", Map.of("op", opName, "key", idempotencyKey));
      }
      if (rec.getStatus() == IdempotencyRecord.Status.SUCCEEDED) {
        return parser.apply(rec.getPayload().toByteArray());
      }
    }

    long ttlMillis = Math.max(1, ttlSeconds) * 1000L;
    Timestamp expiresAt =
        Timestamps.add(
            now,
            Duration.newBuilder()
                .setSeconds(ttlMillis / 1000)
                .setNanos((int) ((ttlMillis % 1000) * 1_000_000))
                .build());

    boolean iCreated = store.createPending(tenantId, key, opName, requestHash, now, expiresAt);
    if (!iCreated) {
      var againOpt = store.get(key);
      if (againOpt.isEmpty()) {
        throw new StorageAbortRetryableException("idempotency record not yet visible: key=" + key);
      }
      var again = againOpt.get();
      if (!again.getRequestHash().equals(requestHash)) {
        throw GrpcErrors.conflict(
            corrId.get(), "idempotency_mismatch", Map.of("op", opName, "key", idempotencyKey));
      }
      if (again.getStatus() == IdempotencyRecord.Status.SUCCEEDED) {
        return parser.apply(again.getPayload().toByteArray());
      }
      throw new StorageAbortRetryableException("idempotency record pending: key=" + key);
    }

    try {
      var created = creator.get();
      var meta = metaExtractor.apply(created.resource());
      var payload = serializer.apply(created.resource());
      store.finalizeSuccess(
          tenantId, key, opName, requestHash, created.resourceId(), meta, payload, now, expiresAt);
      return created.resource();
    } catch (Throwable t) {
      if (iCreated) {
        store.delete(key);
      }
      throw t;
    }
  }

  public static <T> T runOnce(
      String tenantId,
      String opName,
      String idempotencyKey,
      byte[] requestBytes,
      Supplier<CreateResult<T>> creator,
      Function<T, MutationMeta> metaExtractor,
      Function<T, byte[]> serializer,
      Function<byte[], T> parser,
      IdempotencyRepository store,
      long ttlSeconds,
      Timestamp now,
      Supplier<String> corrId,
      Function<IdempotencyRecord, Boolean> canReplay) {

    if (idempotencyKey == null || idempotencyKey.isBlank()) {
      var result = creator.get();
      return result.resource();
    }

    String key = Keys.idempotencyKey(tenantId, opName, idempotencyKey);
    String requestHash = sha256B64(requestBytes);

    var existing = store.get(key);
    if (existing.isPresent()) {
      var rec = existing.get();
      if (!rec.getRequestHash().equals(requestHash)) {
        throw GrpcErrors.conflict(
            corrId.get(), "idempotency_mismatch", Map.of("op", opName, "key", idempotencyKey));
      }
      if (rec.getStatus() == IdempotencyRecord.Status.SUCCEEDED) {
        boolean replayOk = (canReplay == null) || Boolean.TRUE.equals(canReplay.apply(rec));
        if (replayOk) {
          return parser.apply(rec.getPayload().toByteArray());
        }

        store.delete(key);
      } else {
        // ignore
      }
    }

    long ttlMillis = Math.max(1, ttlSeconds) * 1000L;
    Timestamp expiresAt =
        Timestamps.add(
            now,
            Duration.newBuilder()
                .setSeconds(ttlMillis / 1000)
                .setNanos((int) ((ttlMillis % 1000) * 1_000_000))
                .build());

    boolean iCreated = store.createPending(tenantId, key, opName, requestHash, now, expiresAt);
    if (!iCreated) {
      var againOpt = store.get(key);
      if (againOpt.isEmpty()) {
        throw new StorageAbortRetryableException("idempotency record not yet visible: key=" + key);
      }
      var again = againOpt.get();
      if (!again.getRequestHash().equals(requestHash)) {
        throw GrpcErrors.conflict(
            corrId.get(), "idempotency_mismatch", Map.of("op", opName, "key", idempotencyKey));
      }
      if (again.getStatus() == IdempotencyRecord.Status.SUCCEEDED) {
        boolean replayOk = (canReplay == null) || Boolean.TRUE.equals(canReplay.apply(again));
        if (replayOk) {
          return parser.apply(again.getPayload().toByteArray());
        }

        store.delete(key);
      } else {
        throw new StorageAbortRetryableException("idempotency record pending: key=" + key);
      }
    }

    try {
      var created = creator.get();
      var meta = metaExtractor.apply(created.resource());
      var payload = serializer.apply(created.resource());
      store.finalizeSuccess(
          tenantId, key, opName, requestHash, created.resourceId(), meta, payload, now, expiresAt);
      return created.resource();
    } catch (Throwable t) {
      store.delete(key);
      throw t;
    }
  }

  private static String sha256B64(byte[] data) {
    try {
      var md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(data);
      return Base64.getEncoder().encodeToString(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
