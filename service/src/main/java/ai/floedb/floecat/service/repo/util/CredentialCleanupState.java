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

package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.UUID;

/** Durable ownership state shared by credential writers and teardown workers. */
public final class CredentialCleanupState {
  private static final String WRITING_PREFIX = "credential-write:";

  private CredentialCleanupState() {}

  public record Write(String pointerKey, String readyPayload, String token, long pointerVersion) {}

  /**
   * Acquires the cleanup row before an external secret is touched. A cleanup worker refuses a row
   * in this state, so account deletion cannot report success while this writer can still publish or
   * compensate the secret.
   *
   * <p>WRITING ownership deliberately has no timeout. The external credential stores expose no
   * fencing token or conditional write, so elapsed time cannot prove a stalled writer has stopped.
   * Only the owning writer may commit or compensate this row.
   */
  public static Write begin(
      PointerStore pointerStore, String pointerKey, String readyPayload, BatchGuard eligibility) {
    String token = writingToken();
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      Pointer current = pointerStore.get(pointerKey).orElse(null);
      if (isWriting(current)) {
        throw new BaseResourceRepository.AbortRetryableException(
            "credential write already owns cleanup row: " + pointerKey);
      }
      long expected = current == null ? 0L : current.getVersion();
      Pointer writing = PointerReferences.opaqueMarkerPointer(pointerKey, token, expected + 1L);
      var ops = new java.util.ArrayList<PointerStore.CasOp>();
      ops.add(new PointerStore.CasUpsert(pointerKey, expected, writing));
      ops.addAll(eligibility.ops());
      if (pointerStore.compareAndSetBatch(ops)) {
        return new Write(pointerKey, readyPayload, token, writing.getVersion());
      }
      if (eligibility.reevaluate() == BatchGuard.Outcome.BROKEN) {
        throw new BaseResourceRepository.BatchGuardFailedException(
            "credential write lost eligibility for " + eligibility.describe());
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "credential write staging contended for: " + pointerKey);
  }

  /** Exact resource-pointer state that authorizes an external credential write. */
  public static BatchGuard pointerVersionGuard(
      PointerStore pointerStore, String pointerKey, long expectedVersion, String subject) {
    return new BatchGuard() {
      @Override
      public List<PointerStore.CasOp> ops() {
        return expectedVersion == 0L
            ? List.of(new PointerStore.CasCheckAbsent(pointerKey))
            : List.of(new PointerStore.CasCheck(pointerKey, expectedVersion));
      }

      @Override
      public Outcome reevaluate() {
        long actual = pointerStore.get(pointerKey).map(Pointer::getVersion).orElse(0L);
        return actual == expectedVersion ? Outcome.HOLDS : Outcome.BROKEN;
      }

      @Override
      public String describe() {
        return subject;
      }
    };
  }

  /** Moves an uncommitted writer back to an ordinary, claimable cleanup task. */
  public static void abort(PointerStore pointerStore, Write write) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      Pointer current = pointerStore.get(write.pointerKey()).orElse(null);
      if (current == null) {
        return;
      }
      if (current != null && !isWriting(current)) {
        return;
      }
      if (current != null && !write.token().equals(current.getBlobUri())) {
        throw new BaseResourceRepository.AbortRetryableException(
            "credential write ownership changed for: " + write.pointerKey());
      }
      long expected = current.getVersion();
      Pointer ready =
          PointerReferences.opaqueMarkerPointer(
              write.pointerKey(), write.readyPayload(), expected + 1L);
      if (pointerStore.compareAndSetBatch(
          List.of(new PointerStore.CasUpsert(write.pointerKey(), expected, ready)))) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "credential write release contended for: " + write.pointerKey());
  }

  /** Removes a failed create's row after its never-published secret was successfully deleted. */
  public static void abortCreate(PointerStore pointerStore, Write write) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      Pointer current = pointerStore.get(write.pointerKey()).orElse(null);
      if (current == null || !write.token().equals(current.getBlobUri())) {
        return;
      }
      if (pointerStore.compareAndSetBatch(
          List.of(new PointerStore.CasDelete(write.pointerKey(), current.getVersion())))) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "credential create release contended for: " + write.pointerKey());
  }

  /** Transitions WRITING to cleanup-ready in the same batch that publishes the resource pointer. */
  public static BatchGuard commitGuard(PointerStore pointerStore, Write write) {
    return new BatchGuard() {
      private boolean committed;

      @Override
      public List<PointerStore.CasOp> ops() {
        if (committed) {
          return List.of(
              new PointerStore.CasCheck(write.pointerKey(), write.pointerVersion() + 1L));
        }
        Pointer ready =
            PointerReferences.opaqueMarkerPointer(
                write.pointerKey(), write.readyPayload(), write.pointerVersion() + 1L);
        return List.of(
            new PointerStore.CasUpsert(write.pointerKey(), write.pointerVersion(), ready));
      }

      @Override
      public Outcome reevaluate() {
        Pointer current = pointerStore.get(write.pointerKey()).orElse(null);
        if (current != null
            && current.getVersion() == write.pointerVersion()
            && write.token().equals(current.getBlobUri())) {
          return Outcome.HOLDS;
        }
        if (current != null
            && current.getVersion() == write.pointerVersion() + 1L
            && write.readyPayload().equals(current.getBlobUri())) {
          committed = true;
          return Outcome.HOLDS;
        }
        return Outcome.BROKEN;
      }

      @Override
      public String describe() {
        return "credential write " + write.pointerKey();
      }
    };
  }

  /** Pins a cleanup-ready row into the resource-delete batch. */
  public static BatchGuard readyGuard(
      PointerStore pointerStore, String pointerKey, String readyPayload, String subject) {
    Pointer captured = pointerStore.get(pointerKey).orElse(null);
    if (captured == null || isWriting(captured)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "credential write still in flight for: " + pointerKey);
    }
    return new BatchGuard() {
      @Override
      public List<PointerStore.CasOp> ops() {
        return List.of(new PointerStore.CasCheck(pointerKey, captured.getVersion()));
      }

      @Override
      public Outcome reevaluate() {
        Pointer current = pointerStore.get(pointerKey).orElse(null);
        return current != null
                && current.getVersion() == captured.getVersion()
                && !isWriting(current)
            ? Outcome.HOLDS
            : Outcome.BROKEN;
      }

      @Override
      public String describe() {
        return subject + " credential cleanup readiness";
      }
    };
  }

  public static boolean isWriting(Pointer pointer) {
    return pointer != null && pointer.getBlobUri().startsWith(WRITING_PREFIX);
  }

  private static String writingToken() {
    return WRITING_PREFIX + UUID.randomUUID();
  }

  /** True when the resource mutation and the WRITING-to-ready transition committed together. */
  public static boolean committed(PointerStore pointerStore, Write write) {
    Pointer current = pointerStore.get(write.pointerKey()).orElse(null);
    return current != null
        && current.getVersion() == write.pointerVersion() + 1L
        && write.readyPayload().equals(current.getBlobUri());
  }
}
