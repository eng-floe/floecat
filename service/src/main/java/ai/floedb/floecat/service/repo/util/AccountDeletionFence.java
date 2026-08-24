/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;

/** Account-deletion exclusion for repositories that publish raw pointer transactions. */
public final class AccountDeletionFence {
  private AccountDeletionFence() {}

  public static void requireAbsent(PointerStore pointerStore, String accountId) {
    if (pointerStore.get(Keys.accountDeletionMarker(accountId)).isPresent()) {
      throw new BaseResourceRepository.AccountDeletionInProgressException(accountId);
    }
  }

  public static boolean compareAndSet(
      PointerStore pointerStore, String accountId, String key, long expectedVersion, Pointer next) {
    return compareAndSetBatch(
        pointerStore, accountId, List.of(new PointerStore.CasUpsert(key, expectedVersion, next)));
  }

  public static boolean compareAndSetBatch(
      PointerStore pointerStore, String accountId, List<? extends PointerStore.CasOp> operations) {
    String fenceKey = Keys.accountDeletionMarker(accountId);
    List<PointerStore.CasOp> fenced = new ArrayList<>(operations.size() + 1);
    fenced.add(new PointerStore.CasCheckAbsent(fenceKey));
    fenced.addAll(operations);
    boolean committed = pointerStore.compareAndSetBatch(fenced);
    if (!committed && pointerStore.get(fenceKey).isPresent()) {
      throw new BaseResourceRepository.AccountDeletionInProgressException(accountId);
    }
    return committed;
  }
}
