/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.Optional;

/** Shared durable fence protocol for a resource whose row remains until cleanup completes. */
public final class DurableDeletionFence {
  private DurableDeletionFence() {}

  /** A durable fence protecting one unchanged resource version. */
  public record Fence(long version, boolean newlyInstalled) {}

  /**
   * Installs {@code fenceKey} beside the expected resource version, or resumes an existing fence.
   *
   * <p>The resource row is deliberately left in place. Descendant writers assert that the fence is
   * absent, cleanup can therefore span transactions and process restarts, and the final delete
   * removes the row and fence atomically.
   */
  public static Optional<Fence> acquire(
      PointerStore pointerStore,
      String accountId,
      String canonicalKey,
      String fenceKey,
      long expectedPointerVersion) {
    if (expectedPointerVersion <= 0L) return Optional.empty();
    Pointer existing = pointerStore.get(fenceKey).orElse(null);
    if (existing != null) {
      boolean unchanged =
          pointerStore
              .get(canonicalKey)
              .map(pointer -> pointer.getVersion() == expectedPointerVersion)
              .orElse(false);
      return unchanged ? Optional.of(new Fence(existing.getVersion(), false)) : Optional.empty();
    }
    Pointer fence = PointerReferences.opaqueMarkerPointer(fenceKey, fenceKey, 1L);
    return pointerStore.compareAndSetBatch(
            List.of(
                new PointerStore.CasCheck(canonicalKey, expectedPointerVersion),
                AccountDeletionFence.checkForAccountWrite(accountId, fenceKey),
                new PointerStore.CasUpsert(fenceKey, 0L, fence)))
        ? Optional.of(new Fence(fence.getVersion(), true))
        : Optional.empty();
  }
}
