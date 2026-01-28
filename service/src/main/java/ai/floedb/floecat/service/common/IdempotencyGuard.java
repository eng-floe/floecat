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

package ai.floedb.floecat.service.common;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

public final class IdempotencyGuard {
  public record CreateResult<T>(T resource, ResourceId resourceId) {}

  public record Result<T>(T resource, MutationMeta meta) {}

  private static final Logger LOG = Logger.getLogger(IdempotencyGuard.class);

  public static <T> Result<T> runOnce(
      String accountId,
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
      var created = creator.get();
      var meta = metaExtractor.apply(created.resource());
      return new Result<>(created.resource(), meta);
    }

    final String key = Keys.idempotencyKey(accountId, opName, idempotencyKey);
    final String requestHash = sha256B64(requestBytes);

    var existingOpt = store.get(key);
    if (existingOpt.isPresent()) {
      var rec = existingOpt.get();

      if (!requestHash.equals(rec.getRequestHash())) {
        throw GrpcErrors.conflict(
            corrId.get(), IDEMPOTENCY_MISMATCH, Map.of("op", opName, "key", idempotencyKey));
      }

      switch (rec.getStatus()) {
        case SUCCEEDED -> {
          if (!rec.hasMeta()) {
            throw new BaseResourceRepository.CorruptionException(
                "idempotency meta missing for succeeded record: key=" + key, null);
          }
          var resource = parser.apply(rec.getPayload().toByteArray());
          return new Result<>(resource, rec.getMeta());
        }
        case PENDING ->
            throw new StorageAbortRetryableException("idempotency record pending: key=" + key);
        default ->
            throw new StorageAbortRetryableException("idempotency state transient: key=" + key);
      }
    }

    final long ttlMillis = Math.max(1, ttlSeconds) * 1000L;
    final Timestamp expiresAt =
        Timestamps.add(
            now,
            Duration.newBuilder()
                .setSeconds(ttlMillis / 1000)
                .setNanos((int) ((ttlMillis % 1000) * 1_000_000))
                .build());

    final boolean createdPending =
        store.createPending(accountId, key, opName, requestHash, now, expiresAt);
    if (!createdPending) {
      var againOpt = store.get(key);
      if (againOpt.isEmpty()) {
        throw new StorageAbortRetryableException("idempotency record not yet visible: key=" + key);
      }
      var again = againOpt.get();

      if (!requestHash.equals(again.getRequestHash())) {
        throw GrpcErrors.conflict(
            corrId.get(), IDEMPOTENCY_MISMATCH, Map.of("op", opName, "key", idempotencyKey));
      }

      switch (again.getStatus()) {
        case SUCCEEDED -> {
          if (!again.hasMeta()) {
            throw new BaseResourceRepository.CorruptionException(
                "idempotency meta missing for succeeded record: key=" + key, null);
          }
          var resource = parser.apply(again.getPayload().toByteArray());
          return new Result<>(resource, again.getMeta());
        }
        case PENDING ->
            throw new StorageAbortRetryableException("idempotency record pending: key=" + key);
        default ->
            throw new StorageAbortRetryableException("idempotency state transient: key=" + key);
      }
    }

    try {
      var created = creator.get();
      var meta = metaExtractor.apply(created.resource());
      var payload = serializer.apply(created.resource());

      store.finalizeSuccess(
          accountId, key, opName, requestHash, created.resourceId(), meta, payload, now, expiresAt);

      return new Result<>(created.resource(), meta);
    } catch (Throwable t) {
      boolean retryable =
          (t instanceof BaseResourceRepository.AbortRetryableException)
              || (t instanceof StorageAbortRetryableException);
      if (!retryable) {
        try {
          store.delete(key);
        } catch (Throwable deleteError) {
          LOG.warnf(deleteError, "idempotency.delete_failed key=%s corr=%s", key, corrId.get());
        }
      }
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
