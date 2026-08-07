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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.Timestamp;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A create that publishes into a namespace carries a child fence, and that fence breaks on any
 * change to the namespace's pointer — a rename, a reparent, even a description edit — not only on
 * its deletion. The guard's contract is that a break is retryable: re-resolve the namespace and try
 * again.
 *
 * <p>Under an idempotency key that retry has to actually happen. The record is left PENDING for a
 * retryable failure because the creator may have committed before failing, but a broken guard is
 * raised either before the batch exists or after the store refused the whole batch, so nothing was
 * written. Keeping the record there makes every later attempt with that key read PENDING and abort
 * without re-running the creator, so the key is dead until its TTL and the resource is never
 * created.
 */
class IdempotencyGuardGuardBreakTest {

  private static final Timestamp NOW = Timestamp.newBuilder().setSeconds(1_000L).build();

  @Test
  void aBrokenChildFenceClearsThePendingRecordSoTheRetryCanRun() {
    var store = new FakeIdempotencyStore();
    var attempts = new AtomicInteger();

    // First attempt: the namespace pointer moved under the create, so the fence breaks.
    assertThrows(
        BaseResourceRepository.BatchGuardFailedException.class,
        () ->
            runOnce(
                store,
                () -> {
                  attempts.incrementAndGet();
                  throw new BaseResourceRepository.BatchGuardFailedException(
                      "create lost the race against deletion of namespace ns");
                }));

    assertTrue(store.isEmpty(), "the pending record must not outlive a batch that never committed");

    // The client retries with the same key, and this time it goes through.
    var result =
        runOnce(
            store,
            () -> {
              attempts.incrementAndGet();
              return new IdempotencyGuard.CreateResult<>("table", id());
            });

    assertEquals("table", result.resource());
    assertEquals(
        2, attempts.get(), "the retry has to re-run the creator, not read a stale PENDING");
  }

  /** Every other retryable failure still keeps the record: the creator may have committed. */
  @Test
  void anOrdinaryRetryableFailureStillKeepsThePendingRecord() {
    var store = new FakeIdempotencyStore();

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () ->
            runOnce(
                store,
                () -> {
                  throw new BaseResourceRepository.AbortRetryableException("conflict, retry");
                }));

    assertTrue(store.hasPending(), "an uncertain outcome must not hand the key back out");
  }

  @Test
  void aGuardBreakAfterADurableWriteKeepsThePendingRecord() {
    var store = new FakeIdempotencyStore();

    assertThrows(
        BaseResourceRepository.BatchGuardFailedAfterWriteException.class,
        () ->
            runOnce(
                store,
                () -> {
                  throw new BaseResourceRepository.BatchGuardFailedAfterWriteException(
                      "prefix sweep committed before its guard broke");
                }));

    assertTrue(store.hasPending(), "a partially committed operation must not be replayed");
  }

  private static IdempotencyGuard.Result<String> runOnce(
      FakeIdempotencyStore store,
      java.util.function.Supplier<IdempotencyGuard.CreateResult<String>> creator) {
    return IdempotencyGuard.runOnce(
        "acct",
        "CreateTable",
        "key-1",
        "request".getBytes(StandardCharsets.UTF_8),
        creator,
        resource -> MutationMeta.newBuilder().setPointerVersion(1L).build(),
        resource -> resource.getBytes(StandardCharsets.UTF_8),
        bytes -> new String(bytes, StandardCharsets.UTF_8),
        store,
        900L,
        NOW,
        () -> "corr");
  }

  private static ResourceId id() {
    return ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();
  }

  private static final class FakeIdempotencyStore implements IdempotencyRepository {
    private final Map<String, IdempotencyRecord> records = new HashMap<>();

    @Override
    public Optional<IdempotencyRecord> get(String key) {
      return Optional.ofNullable(records.get(key));
    }

    @Override
    public PendingClaim createPending(
        String accountId,
        String key,
        String opName,
        String requestHash,
        Timestamp createdAt,
        Timestamp expiresAt) {
      if (records.containsKey(key)) {
        return new PendingClaim(false, 0L, "", 0L);
      }
      records.put(
          key,
          IdempotencyRecord.newBuilder()
              .setOpName(opName)
              .setRequestHash(requestHash)
              .setStatus(IdempotencyRecord.Status.PENDING)
              .build());
      return new PendingClaim(true, 1L, key + "/claim", 1L);
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
      records.put(
          key,
          IdempotencyRecord.newBuilder()
              .setOpName(opName)
              .setRequestHash(requestHash)
              .setStatus(IdempotencyRecord.Status.SUCCEEDED)
              .setResourceId(resourceId)
              .setMeta(meta)
              .setPayload(com.google.protobuf.ByteString.copyFrom(payloadBytes))
              .build());
    }

    @Override
    public boolean delete(String key) {
      return records.remove(key) != null;
    }

    @Override
    public boolean deletePending(String key, PendingClaim pendingClaim) {
      return delete(key);
    }

    @Override
    public CleanupResult deleteAccountResources(
        String accountId,
        BatchGuard accountGone,
        BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
      int deleted = records.size();
      records.clear();
      return new CleanupResult(deleted, 0);
    }

    boolean isEmpty() {
      return records.isEmpty();
    }

    boolean hasPending() {
      return records.values().stream()
          .anyMatch(r -> r.getStatus() == IdempotencyRecord.Status.PENDING);
    }
  }
}
