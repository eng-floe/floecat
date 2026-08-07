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

  /** Stable idempotency scope for platform calls that do not execute as an account. */
  public static final String PLATFORM_SCOPE = "platform";

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
    final boolean logTiming = shouldLogTiming(opName);
    final long totalStartNanos = logTiming ? System.nanoTime() : 0L;
    long getNanos = 0L;
    long createPendingNanos = 0L;
    long creatorNanos = 0L;
    long finalizeSuccessNanos = 0L;
    long getAfterCreatePendingNanos = 0L;
    String outcome = "unknown";

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
          throw new StorageAbortRetryableException("idempotency record pending: key=" + key);
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
    final var pendingClaim =
        store.createPending(accountId, key, opName, requestHash, now, expiresAt);
    final boolean createdPending = pendingClaim.created();
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
          throw new StorageAbortRetryableException("idempotency record pending: key=" + key);
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
      var meta = metaExtractor.apply(created.resource());
      var payload = serializer.apply(created.resource());

      long finalizeStartNanos = logTiming ? System.nanoTime() : 0L;
      store.finalizeSuccess(
          accountId,
          key,
          pendingClaim,
          opName,
          requestHash,
          created.resourceId(),
          meta,
          payload,
          now,
          expiresAt);
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
      // The PENDING record is kept for a retryable failure because the creator may have committed
      // before failing, and deleting it would let a retry write the resource twice. A broken batch
      // guard is the one retryable failure where releasing the key is safe anyway, and the record
      // must go — otherwise the retry the guard's own contract promises can never happen. Every
      // later attempt with this key reads PENDING and aborts instead of re-running the creator, so
      // the key stays unusable until its TTL expires and the resource is never created. A child
      // fence breaks on any change to its namespace's pointer, a rename or a description edit
      // included, so this is an ordinary outcome, not a rare one.
      //
      // What makes it safe is narrower than "nothing was written". The guarded batch itself
      // certainly wrote nothing unless the exception explicitly says an earlier guarded step
      // committed (BatchGuardFailedAfterWriteException). But a creator may have committed writes
      // on the way to that batch — CreateNamespace with parents runs ensurePathChainExists first,
      // which creates intermediate ancestors and bumps the catalog marker before the guarded leaf
      // create that breaks. The real invariant is that every write reachable from the guarded leaf
      // is idempotent on replay: those ancestors are keyed by path, so a retry resolves each level
      // through refByPath and skips it, and the leaf gets a fresh id because none was committed.
      //
      // So a new multi-write creator may only rely on this release if its pre-writes are
      // replay-idempotent too. One that is not — anything keyed by a value it generates per
      // attempt,
      // or any non-idempotent side effect — would have that work duplicated by the retry this
      // release enables, and must keep its record instead.
      boolean provablyUncommitted =
          t instanceof BaseResourceRepository.BatchGuardFailedException
              && !(t instanceof BaseResourceRepository.BatchGuardFailedAfterWriteException);
      boolean retryable =
          !provablyUncommitted
              && ((t instanceof BaseResourceRepository.AbortRetryableException)
                  || (t instanceof StorageAbortRetryableException));
      if (!retryable) {
        try {
          store.deletePending(key, pendingClaim);
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
