/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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

  /** Returns exact fence checks for every account touched by a pointer upsert. */
  public static List<PointerStore.CasCheckAbsent> checksForAccountWrites(
      List<? extends PointerStore.CasOp> operations) {
    Set<String> fenceKeys = new LinkedHashSet<>();
    Set<String> existingChecks = new LinkedHashSet<>();
    if (operations != null) {
      for (PointerStore.CasOp operation : operations) {
        if (operation instanceof PointerStore.CasUpsert upsert) {
          collectFenceKey(upsert.key(), fenceKeys);
          if (PointerReferences.isPointerKeyPointer(upsert.next())) {
            collectFenceKey(upsert.next().getBlobUri(), fenceKeys);
          }
        } else if (operation instanceof PointerStore.UnconditionalUpsert upsert) {
          collectFenceKey(upsert.key(), fenceKeys);
          if (PointerReferences.isPointerKeyPointer(upsert.next())) {
            collectFenceKey(upsert.next().getBlobUri(), fenceKeys);
          }
        } else if (operation instanceof PointerStore.CasCheckAbsent check) {
          existingChecks.add(check.key());
        }
      }
    }
    fenceKeys.removeAll(existingChecks);
    return fenceKeys.stream().map(PointerStore.CasCheckAbsent::new).toList();
  }

  public static List<PointerStore.CasOp> withChecksForAccountWrites(
      List<? extends PointerStore.CasOp> operations) {
    List<PointerStore.CasOp> fenced = new ArrayList<>();
    fenced.addAll(checksForAccountWrites(operations));
    if (operations != null) {
      fenced.addAll(operations);
    }
    return List.copyOf(fenced);
  }

  private static void collectFenceKey(String pointerKey, Set<String> fenceKeys) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return;
    }
    String normalized = pointerKey.startsWith("/") ? pointerKey : "/" + pointerKey;
    String prefix = Keys.accountRootPrefix();
    if (!normalized.startsWith(prefix)) {
      return;
    }
    int segmentEnd = normalized.indexOf('/', prefix.length());
    if (segmentEnd < 0) {
      return;
    }
    String accountSegment = normalized.substring(prefix.length(), segmentEnd);
    if (accountSegment.isBlank() || Keys.isReservedAccountDirectorySegment(accountSegment)) {
      return;
    }
    fenceKeys.add(Keys.accountDeletionMarkerForEncodedSegment(accountSegment));
  }
}
