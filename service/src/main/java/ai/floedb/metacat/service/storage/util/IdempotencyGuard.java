package ai.floedb.metacat.service.storage.util;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
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
      IdempotencyStore store,
      long ttlSeconds,
      Timestamp now,
      Supplier<String> corrId) {
    if (idempotencyKey == null || idempotencyKey.isBlank()) {
      var result = creator.get();
      return result.resource();
    }

    String key = Keys.idemKey(tenantId, opName, idempotencyKey);
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

    if (!store.createPending(key, opName, requestHash, now, expiresAt)) {
      var again = store.get(key).orElseThrow();
      if (!again.getRequestHash().equals(requestHash)) {
        throw GrpcErrors.conflict(
            corrId.get(), "idempotency_mismatch", Map.of("op", opName, "key", idempotencyKey));
      }
      if (again.getStatus() == IdempotencyRecord.Status.SUCCEEDED) {
        return parser.apply(again.getPayload().toByteArray());
      }
      throw GrpcErrors.preconditionFailed(
          corrId.get(), "idempotency_pending", Map.of("op", opName, "key", idempotencyKey));
    }

    try {
      var created = creator.get();
      var meta = metaExtractor.apply(created.resource());
      var payload = serializer.apply(created.resource());
      store.finalizeSuccess(
          key, opName, requestHash, created.resourceId(), meta, payload, now, expiresAt);
      return created.resource();
    } catch (Throwable t) {
      store.delete(key);
      throw t;
    }
  }

  private static String sha256B64(byte[] data) {
    try {
      var md = java.security.MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(data);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
