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
import ai.floedb.floecat.storage.errors.StorageTransactionConflictException;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

public final class IdempotencyGuard {
  public record CreateResult<T>(T resource, ResourceId resourceId) {}

  public record CommittedCreate<T>(T resource, ResourceId resourceId, MutationMeta meta) {}

  public interface SuccessCommitter<T> {
    List<PointerStore.CasOp> prepareSuccessOps(CommittedCreate<T> committed);

    default void discardPreparedSuccessOps(List<PointerStore.CasOp> prepared) {}
  }

  public record Result<T>(T resource, MutationMeta meta) {}

  private static final Logger LOG = Logger.getLogger(IdempotencyGuard.class);

  /**
   * Idempotent resource creation with a durable identity reservation. A pending record contains the
   * resource id before the create transaction runs. The creator must publish the immutable success
   * receipt in the same pointer transaction as the resource; retries never reconstruct a receipt
   * from mutable resource state. A retryable failure deliberately retains the reservation: a later
   * attempt adopts its stable resource id and reruns the recoverable creator, rather than releasing
   * and allocating a different identity.
   */
  public static <T> Result<T> runOnceReserved(
      String accountId,
      String opName,
      String idempotencyKey,
      byte[] requestBytes,
      Supplier<ResourceId> resourceIdAllocator,
      BiFunction<ResourceId, SuccessCommitter<T>, CommittedCreate<T>> creator,
      Function<T, byte[]> serializer,
      Function<byte[], T> parser,
      IdempotencyRepository store,
      long ttlSeconds,
      Timestamp now,
      Supplier<String> corrId) {
    if (idempotencyKey == null || idempotencyKey.isBlank()) {
      var created = creator.apply(resourceIdAllocator.get(), ignored -> List.of());
      return new Result<>(created.resource(), created.meta());
    }

    String key = Keys.idempotencyKey(accountId, opName, idempotencyKey);
    String requestHash = sha256B64(requestBytes);
    ResourceId reservedId;
    Timestamp reservationCreatedAt;
    Timestamp reservationExpiresAt;
    boolean reservationCreatedByCaller = false;
    Optional<ai.floedb.floecat.storage.rpc.IdempotencyRecord> existing = store.get(key);
    if (existing.isPresent()) {
      var replay = existing.get();
      requireMatchingRequest(replay.getRequestHash(), requestHash, opName, idempotencyKey, corrId);
      if (replay.getStatus() == ai.floedb.floecat.storage.rpc.IdempotencyRecord.Status.SUCCEEDED) {
        if (!replay.hasMeta()) {
          throw new BaseResourceRepository.CorruptionException(
              "idempotency meta missing for succeeded record: key=" + key, null);
        }
        return new Result<>(parser.apply(replay.getPayload().toByteArray()), replay.getMeta());
      }
      if (replay.getStatus() != ai.floedb.floecat.storage.rpc.IdempotencyRecord.Status.PENDING
          || !replay.hasResourceId()
          || replay.getResourceId().getId().isEmpty()) {
        throw new StorageAbortRetryableException("idempotency record pending: key=" + key);
      }
      reservedId = replay.getResourceId();
      reservationCreatedAt = replay.getCreatedAt();
      reservationExpiresAt = replay.getExpiresAt();
    } else {
      reservedId = resourceIdAllocator.get();
      Timestamp expiresAt = expiresAt(now, ttlSeconds);
      if (!store.createPending(accountId, key, opName, requestHash, reservedId, now, expiresAt)) {
        var winner = store.get(key);
        if (winner.isEmpty()) {
          throw new StorageAbortRetryableException(
              "idempotency record not yet visible: key=" + key);
        }
        var replay = winner.get();
        requireMatchingRequest(
            replay.getRequestHash(), requestHash, opName, idempotencyKey, corrId);
        if (replay.getStatus()
            == ai.floedb.floecat.storage.rpc.IdempotencyRecord.Status.SUCCEEDED) {
          if (!replay.hasMeta()) {
            throw new BaseResourceRepository.CorruptionException(
                "idempotency meta missing for succeeded record: key=" + key, null);
          }
          return new Result<>(parser.apply(replay.getPayload().toByteArray()), replay.getMeta());
        }
        if (!replay.hasResourceId() || replay.getResourceId().getId().isEmpty()) {
          throw new StorageAbortRetryableException("idempotency record pending: key=" + key);
        }
        reservedId = replay.getResourceId();
        reservationCreatedAt = replay.getCreatedAt();
        reservationExpiresAt = replay.getExpiresAt();
      } else {
        reservationCreatedByCaller = true;
        reservationCreatedAt = now;
        reservationExpiresAt = expiresAt;
      }
    }

    boolean committed = false;
    try {
      Timestamp commitCreatedAt = reservationCreatedAt;
      Timestamp commitExpiresAt = reservationExpiresAt;
      SuccessCommitter<T> committer =
          successCommitter(
              store,
              accountId,
              key,
              opName,
              requestHash,
              commitCreatedAt,
              commitExpiresAt,
              reservedId,
              serializer);
      CommittedCreate<T> created = creator.apply(reservedId, committer);
      committed = true;
      var completed = store.get(key);
      if (completed.isPresent()
          && completed.get().getStatus()
              == ai.floedb.floecat.storage.rpc.IdempotencyRecord.Status.SUCCEEDED) {
        var exact = completed.get();
        requireMatchingRequest(exact.getRequestHash(), requestHash, opName, idempotencyKey, corrId);
        return new Result<>(parser.apply(exact.getPayload().toByteArray()), exact.getMeta());
      }
      throw new StorageAbortRetryableException(
          "resource create returned without an atomic idempotency receipt: key=" + key);
    } catch (Throwable failure) {
      boolean retryable =
          failure instanceof BaseResourceRepository.AbortRetryableException
              || failure instanceof StorageAbortRetryableException;
      if (!committed && !retryable) {
        var completed = store.get(key);
        if (completed.isPresent()
            && completed.get().getStatus()
                == ai.floedb.floecat.storage.rpc.IdempotencyRecord.Status.SUCCEEDED) {
          var exact = completed.get();
          requireMatchingRequest(
              exact.getRequestHash(), requestHash, opName, idempotencyKey, corrId);
          if (!exact.hasMeta()) {
            throw new BaseResourceRepository.CorruptionException(
                "idempotency meta missing for succeeded record: key=" + key, null);
          }
          return new Result<>(parser.apply(exact.getPayload().toByteArray()), exact.getMeta());
        }
        if (reservationCreatedByCaller) {
          deletePendingIfOwned(
              store, key, opName, requestHash, reservationCreatedAt, reservationExpiresAt, corrId);
        }
      }
      throw failure;
    }
  }

  /**
   * Idempotent mutation of a known resource. The callback must publish the prepared success receipt
   * in the same pointer transaction as its business mutation.
   */
  public static <T> Result<T> runOnceCommitted(
      String accountId,
      String opName,
      String idempotencyKey,
      byte[] requestBytes,
      ResourceId resourceId,
      Function<SuccessCommitter<T>, CommittedCreate<T>> mutation,
      Function<T, byte[]> serializer,
      Function<byte[], T> parser,
      IdempotencyRepository store,
      long ttlSeconds,
      Timestamp now,
      Supplier<String> corrId) {
    if (idempotencyKey == null || idempotencyKey.isBlank()) {
      throw new IllegalArgumentException("runOnceCommitted requires an idempotency key");
    }

    String key = Keys.idempotencyKey(accountId, opName, idempotencyKey);
    String requestHash = sha256B64(requestBytes);
    Optional<ai.floedb.floecat.storage.rpc.IdempotencyRecord> existing = store.get(key);
    if (existing.isPresent()) {
      var replay = existing.get();
      requireMatchingRequest(replay.getRequestHash(), requestHash, opName, idempotencyKey, corrId);
      if (replay.getStatus() == ai.floedb.floecat.storage.rpc.IdempotencyRecord.Status.SUCCEEDED) {
        if (!replay.hasMeta()) {
          throw new BaseResourceRepository.CorruptionException(
              "idempotency meta missing for succeeded record: key=" + key, null);
        }
        return new Result<>(parser.apply(replay.getPayload().toByteArray()), replay.getMeta());
      }
      throw new IdempotencyInProgressException("idempotency record pending: key=" + key);
    }

    Timestamp expiresAt = expiresAt(now, ttlSeconds);
    if (!store.createPending(accountId, key, opName, requestHash, resourceId, now, expiresAt)) {
      var winner = store.get(key);
      if (winner.isEmpty()) {
        throw new StorageAbortRetryableException("idempotency record not yet visible: key=" + key);
      }
      var replay = winner.get();
      requireMatchingRequest(replay.getRequestHash(), requestHash, opName, idempotencyKey, corrId);
      if (replay.getStatus() == ai.floedb.floecat.storage.rpc.IdempotencyRecord.Status.SUCCEEDED) {
        if (!replay.hasMeta()) {
          throw new BaseResourceRepository.CorruptionException(
              "idempotency meta missing for succeeded record: key=" + key, null);
        }
        return new Result<>(parser.apply(replay.getPayload().toByteArray()), replay.getMeta());
      }
      throw new IdempotencyInProgressException("idempotency record pending: key=" + key);
    }

    try {
      SuccessCommitter<T> committer =
          successCommitter(
              store, accountId, key, opName, requestHash, now, expiresAt, resourceId, serializer);
      CommittedCreate<T> committed = mutation.apply(committer);
      if (!resourceId.equals(committed.resourceId())) {
        throw new IllegalArgumentException("committed resource identity changed");
      }
      var completed = store.get(key);
      if (completed.isPresent()
          && completed.get().getStatus()
              == ai.floedb.floecat.storage.rpc.IdempotencyRecord.Status.SUCCEEDED) {
        var exact = completed.get();
        requireMatchingRequest(exact.getRequestHash(), requestHash, opName, idempotencyKey, corrId);
        return new Result<>(parser.apply(exact.getPayload().toByteArray()), exact.getMeta());
      }
      deletePendingIfOwned(store, key, opName, requestHash, now, expiresAt, corrId);
      throw new StorageAbortRetryableException(
          "atomic mutation returned without an idempotency receipt: key=" + key);
    } catch (Throwable failure) {
      var completed = store.get(key);
      if (completed.isPresent()
          && completed.get().getStatus()
              == ai.floedb.floecat.storage.rpc.IdempotencyRecord.Status.SUCCEEDED) {
        var exact = completed.get();
        requireMatchingRequest(exact.getRequestHash(), requestHash, opName, idempotencyKey, corrId);
        if (!exact.hasMeta()) {
          throw new BaseResourceRepository.CorruptionException(
              "idempotency meta missing for succeeded record: key=" + key, null);
        }
        return new Result<>(parser.apply(exact.getPayload().toByteArray()), exact.getMeta());
      }
      // Repository aborts are safe to release only because this helper's callback contract permits
      // one pointer transaction containing all business mutations and the receipt. They therefore
      // cannot represent a pre-receipt partial commit.
      boolean knownAbort =
          failure instanceof BaseResourceRepository.AbortRetryableException
              || failure instanceof StorageTransactionConflictException;
      boolean retryable = failure instanceof StorageAbortRetryableException || knownAbort;
      if (!retryable || knownAbort) {
        deletePendingIfOwned(store, key, opName, requestHash, now, expiresAt, corrId);
      }
      throw failure;
    }
  }

  private static void requireMatchingRequest(
      String storedHash,
      String requestHash,
      String opName,
      String idempotencyKey,
      Supplier<String> corrId) {
    if (!requestHash.equals(storedHash)) {
      throw GrpcErrors.conflict(
          corrId.get(), IDEMPOTENCY_MISMATCH, Map.of("op", opName, "key", idempotencyKey));
    }
  }

  private static Timestamp expiresAt(Timestamp now, long ttlSeconds) {
    long ttlMillis = Math.max(1, ttlSeconds) * 1000L;
    return Timestamps.add(
        now,
        Duration.newBuilder()
            .setSeconds(ttlMillis / 1000)
            .setNanos((int) ((ttlMillis % 1000) * 1_000_000))
            .build());
  }

  /**
   * Runs an operation whose callback has no durable side effect. If the receipt transaction is
   * definitively cancelled, the caller's pending claim is released so an outer retry can rerun it.
   */
  public static <T> Result<T> runOnceReceiptOnly(
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
    return executeClaimedEffect(
        accountId,
        opName,
        idempotencyKey,
        requestBytes,
        creator,
        metaExtractor,
        serializer,
        parser,
        store,
        ttlSeconds,
        now,
        corrId);
  }

  /**
   * Runs an effect whose durable writes have natural deduplication and therefore converge when the
   * whole callback is repeated. This is intentionally separate from pointer-atomic mutations.
   */
  public static <T> Result<T> runOnceConvergentEffects(
      String accountId,
      String opName,
      String idempotencyKey,
      byte[] requestBytes,
      Supplier<CreateResult<T>> effect,
      Function<T, MutationMeta> metaExtractor,
      Function<T, byte[]> serializer,
      Function<byte[], T> parser,
      IdempotencyRepository store,
      long ttlSeconds,
      Timestamp now,
      Supplier<String> corrId) {
    return executeClaimedEffect(
        accountId,
        opName,
        idempotencyKey,
        requestBytes,
        effect,
        metaExtractor,
        serializer,
        parser,
        store,
        ttlSeconds,
        now,
        corrId);
  }

  private static <T> Result<T> executeClaimedEffect(
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
    final boolean logTiming = shouldLogTiming(opName);
    final long totalStartNanos = logTiming ? System.nanoTime() : 0L;
    long getNanos = 0L;
    long createPendingNanos = 0L;
    long creatorNanos = 0L;
    long finalizeSuccessNanos = 0L;
    long getAfterCreatePendingNanos = 0L;
    String outcome = "unknown";
    String failurePhase = "creator";

    long getStartNanos = logTiming ? System.nanoTime() : 0L;
    var existingOpt = store.get(key);
    if (logTiming) {
      getNanos = System.nanoTime() - getStartNanos;
    }
    if (existingOpt.isPresent()) {
      var rec = existingOpt.get();

      if (!requestHash.equals(rec.getRequestHash())) {
        logTiming(
            logTiming,
            opName,
            key,
            "mismatch_existing",
            totalStartNanos,
            getNanos,
            createPendingNanos,
            getAfterCreatePendingNanos,
            creatorNanos,
            finalizeSuccessNanos);
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
          logTiming(
              logTiming,
              opName,
              key,
              "hit_succeeded",
              totalStartNanos,
              getNanos,
              createPendingNanos,
              getAfterCreatePendingNanos,
              creatorNanos,
              finalizeSuccessNanos);
          return new Result<>(resource, rec.getMeta());
        }
        case PENDING -> {
          logTiming(
              logTiming,
              opName,
              key,
              "hit_pending",
              totalStartNanos,
              getNanos,
              createPendingNanos,
              getAfterCreatePendingNanos,
              creatorNanos,
              finalizeSuccessNanos);
          throw new IdempotencyInProgressException("idempotency record pending: key=" + key);
        }
        default -> {
          logTiming(
              logTiming,
              opName,
              key,
              "hit_transient",
              totalStartNanos,
              getNanos,
              createPendingNanos,
              getAfterCreatePendingNanos,
              creatorNanos,
              finalizeSuccessNanos);
          throw new StorageAbortRetryableException("idempotency state transient: key=" + key);
        }
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

    long createPendingStartNanos = logTiming ? System.nanoTime() : 0L;
    final boolean createdPending =
        store.createPending(accountId, key, opName, requestHash, now, expiresAt);
    if (logTiming) {
      createPendingNanos = System.nanoTime() - createPendingStartNanos;
    }
    if (!createdPending) {
      long getAfterPendingStartNanos = logTiming ? System.nanoTime() : 0L;
      var againOpt = store.get(key);
      if (logTiming) {
        getAfterCreatePendingNanos = System.nanoTime() - getAfterPendingStartNanos;
      }
      if (againOpt.isEmpty()) {
        logTiming(
            logTiming,
            opName,
            key,
            "not_visible_after_pending",
            totalStartNanos,
            getNanos,
            createPendingNanos,
            getAfterCreatePendingNanos,
            creatorNanos,
            finalizeSuccessNanos);
        throw new StorageAbortRetryableException("idempotency record not yet visible: key=" + key);
      }
      var again = againOpt.get();

      if (!requestHash.equals(again.getRequestHash())) {
        logTiming(
            logTiming,
            opName,
            key,
            "mismatch_after_pending",
            totalStartNanos,
            getNanos,
            createPendingNanos,
            getAfterCreatePendingNanos,
            creatorNanos,
            finalizeSuccessNanos);
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
          logTiming(
              logTiming,
              opName,
              key,
              "lost_race_succeeded",
              totalStartNanos,
              getNanos,
              createPendingNanos,
              getAfterCreatePendingNanos,
              creatorNanos,
              finalizeSuccessNanos);
          return new Result<>(resource, again.getMeta());
        }
        case PENDING -> {
          logTiming(
              logTiming,
              opName,
              key,
              "lost_race_pending",
              totalStartNanos,
              getNanos,
              createPendingNanos,
              getAfterCreatePendingNanos,
              creatorNanos,
              finalizeSuccessNanos);
          throw new IdempotencyInProgressException("idempotency record pending: key=" + key);
        }
        default -> {
          logTiming(
              logTiming,
              opName,
              key,
              "lost_race_transient",
              totalStartNanos,
              getNanos,
              createPendingNanos,
              getAfterCreatePendingNanos,
              creatorNanos,
              finalizeSuccessNanos);
          throw new StorageAbortRetryableException("idempotency state transient: key=" + key);
        }
      }
    }

    try {
      long creatorStartNanos = logTiming ? System.nanoTime() : 0L;
      var created = creator.get();
      if (logTiming) {
        creatorNanos = System.nanoTime() - creatorStartNanos;
      }
      failurePhase = "meta_extractor";
      var meta = metaExtractor.apply(created.resource());
      failurePhase = "serializer";
      var payload = serializer.apply(created.resource());

      failurePhase = "finalize_success";
      long finalizeStartNanos = logTiming ? System.nanoTime() : 0L;
      store.finalizeSuccess(
          accountId, key, opName, requestHash, created.resourceId(), meta, payload, now, expiresAt);
      if (logTiming) {
        finalizeSuccessNanos = System.nanoTime() - finalizeStartNanos;
      }
      outcome = "created";
      logTiming(
          logTiming,
          opName,
          key,
          outcome,
          totalStartNanos,
          getNanos,
          createPendingNanos,
          getAfterCreatePendingNanos,
          creatorNanos,
          finalizeSuccessNanos);

      return new Result<>(created.resource(), meta);
    } catch (Throwable t) {
      logTiming(
          logTiming,
          opName,
          key,
          "failed_" + t.getClass().getSimpleName(),
          totalStartNanos,
          getNanos,
          createPendingNanos,
          getAfterCreatePendingNanos,
          creatorNanos,
          finalizeSuccessNanos);
      LOG.warnf(
          t,
          "idempotency.repeatable_effect_failed op=%s key=%s phase=%s corr=%s",
          opName,
          key,
          failurePhase,
          corrId.get());
      // Both callers of this helper explicitly promise that the callback has no durable effect or
      // is safe to repeat. Releasing an owned PENDING claim is therefore safe for every failure,
      // including acknowledgement-uncertain receipt writes; a receipt that actually committed is
      // SUCCEEDED and cannot be removed by deletePendingIfOwned.
      deletePendingIfOwned(store, key, opName, requestHash, now, expiresAt, corrId);
      throw t;
    }
  }

  private static <T> SuccessCommitter<T> successCommitter(
      IdempotencyRepository store,
      String accountId,
      String key,
      String opName,
      String requestHash,
      Timestamp createdAt,
      Timestamp expiresAt,
      ResourceId expectedResourceId,
      Function<T, byte[]> serializer) {
    return new SuccessCommitter<>() {
      @Override
      public List<PointerStore.CasOp> prepareSuccessOps(CommittedCreate<T> committed) {
        if (!expectedResourceId.equals(committed.resourceId())) {
          throw new IllegalArgumentException("committed resource identity changed");
        }
        return List.of(
            store.prepareSuccess(
                accountId,
                key,
                opName,
                requestHash,
                committed.resourceId(),
                committed.meta(),
                serializer.apply(committed.resource()),
                createdAt,
                expiresAt));
      }

      @Override
      public void discardPreparedSuccessOps(List<PointerStore.CasOp> prepared) {
        if (prepared != null) {
          prepared.forEach(store::discardPreparedSuccess);
        }
      }
    };
  }

  private static void deletePendingIfOwned(
      IdempotencyRepository store,
      String key,
      String opName,
      String requestHash,
      Timestamp createdAt,
      Timestamp expiresAt,
      Supplier<String> corrId) {
    try {
      store.deletePendingIfOwned(key, opName, requestHash, createdAt, expiresAt);
    } catch (Throwable deleteError) {
      LOG.warnf(deleteError, "idempotency.delete_failed key=%s corr=%s", key, corrId.get());
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

  private static boolean shouldLogTiming(String opName) {
    return "CommitLeasedFileGroupResult".equals(opName)
        || "SubmitLeasedSnapshotFinalizeResult".equals(opName);
  }

  private static void logTiming(
      boolean enabled,
      String opName,
      String key,
      String outcome,
      long totalStartNanos,
      long getNanos,
      long createPendingNanos,
      long getAfterCreatePendingNanos,
      long creatorNanos,
      long finalizeSuccessNanos) {
    if (!enabled) {
      return;
    }
    long totalNanos = System.nanoTime() - totalStartNanos;
    long accountedNanos =
        getNanos
            + createPendingNanos
            + getAfterCreatePendingNanos
            + creatorNanos
            + finalizeSuccessNanos;
    long otherNanos = Math.max(0L, totalNanos - accountedNanos);
    LOG.infof(
        "idempotency_guard_timing op=%s outcome=%s key=%s totalMs=%.3f getMs=%.3f "
            + "createPendingMs=%.3f getAfterPendingMs=%.3f creatorMs=%.3f finalizeSuccessMs=%.3f "
            + "otherMs=%.3f",
        opName,
        outcome,
        key,
        totalNanos / 1_000_000.0,
        getNanos / 1_000_000.0,
        createPendingNanos / 1_000_000.0,
        getAfterCreatePendingNanos / 1_000_000.0,
        creatorNanos / 1_000_000.0,
        finalizeSuccessNanos / 1_000_000.0,
        otherNanos / 1_000_000.0);
  }
}
