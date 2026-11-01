/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.gc;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.account.impl.AccountCredentialCleanup;
import ai.floedb.floecat.service.cache.HintCache;
import ai.floedb.floecat.service.cache.ObjectCache;
import ai.floedb.floecat.service.reconciler.jobs.DurableReconcileJobStore;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.logging.Logger;

/** Drains durable account-purge work after the delete RPC has fenced and removed an account. */
@ApplicationScoped
public class AccountPurge {
  private static final Logger LOG = Logger.getLogger(AccountPurge.class);
  private static final int DIAGNOSTIC_KEY_LIMIT = 10;

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject Instance<DurableReconcileJobStore> durableReconcileJobStore;
  @Inject ObjectCache objects;
  @Inject HintCache hints;
  @Inject BlobCacheAccess blobs;
  @Inject OwnedAccounts accounts;
  @Inject AccountCredentialCleanup credentialCleanup;

  public record Result(int scanned, int purged) {}

  public Result drain(long deadlineMs, int pageSize) {
    int scanned = 0;
    int purged = 0;
    String token = "";
    String prefix = Keys.accountPurgePrefix();
    while (System.currentTimeMillis() < deadlineMs) {
      var next = new StringBuilder();
      var entries = pointerStore.listPointersByPrefix(prefix, Math.max(1, pageSize), token, next);
      for (Pointer entry : entries) {
        if (System.currentTimeMillis() >= deadlineMs) break;
        scanned++;
        String accountId = parseAccountId(entry.getKey());
        if (accountId == null) {
          LOG.errorf("invalid account purge key=%s", entry.getKey());
          if (pointerStore.compareAndDelete(entry.getKey(), entry.getVersion())) purged++;
          continue;
        }
        if (!accounts.owns(accountId)) continue;
        if (pointerStore.get(Keys.accountDeletionMarker(accountId)).isEmpty()) {
          LOG.warnf("account purge has no deletion marker account_id=%s", accountId);
          continue;
        }
        ResourceId id =
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(accountId)
                .setKind(ResourceKind.RK_ACCOUNT)
                .build();
        if (accounts.getById(id).isPresent()) continue;
        try {
          purge(accountId);
          if (pointerStore.compareAndDelete(entry.getKey(), entry.getVersion())) purged++;
        } catch (RuntimeException failure) {
          LOG.errorf(failure, "account purge failed account_id=%s", accountId);
        }
      }
      token = next.toString();
      if (token.isEmpty()) break;
    }
    return new Result(scanned, purged);
  }

  private void purge(String accountId) {
    LOG.infof("account_purge_start account_id=%s", accountId);
    credentialCleanup.revoke(accountId);
    int reconcileJobsDeleted = 0;
    if (durableReconcileJobStore.isResolvable()) {
      reconcileJobsDeleted = durableReconcileJobStore.get().cleanupAccount(accountId);
    } else {
      LOG.warnf("account_purge_reconcile_cleanup_unavailable account_id=%s", accountId);
    }

    String accountPrefix = Keys.accountRootPrefix(accountId);
    String deletionFence = Keys.accountDeletionMarker(accountId);
    int pointersDeleted = pointerStore.deleteByPrefixExcluding(accountPrefix, deletionFence);
    assertAccountPointerSweepComplete(accountPrefix, deletionFence);
    pointerStore.deleteByPrefix(Keys.relationPurgeAccountPrefix(accountId));
    int blobsDeleted = blobStore.deletePrefix(accountPrefix);

    // Evict last so local reads cannot repopulate deleted content from a live durable pointer.
    objects.evictAccount(accountId);
    hints.evictAccount(accountId);
    blobs.evictAccount(accountId);
    LOG.infof(
        "account_purge_complete account_id=%s account_pointer_deletes=%d reconcile_jobs=%d residual_account_blob_deletes=%d",
        accountId, pointersDeleted, reconcileJobsDeleted, blobsDeleted);
  }

  void assertAccountPointerSweepComplete(String accountPrefix, String deletionFence) {
    Pointer remainingFence = pointerStore.get(deletionFence).orElse(null);
    int remaining = pointerStore.countByPrefix(accountPrefix);
    if (remainingFence == null || remaining != 1) {
      int unexpectedCount = remaining - (remainingFence == null ? 0 : 1);
      var next = new StringBuilder();
      List<String> unexpectedKeys =
          pointerStore
              .listPointersByPrefix(accountPrefix, DIAGNOSTIC_KEY_LIMIT + 1, "", next)
              .stream()
              .map(Pointer::getKey)
              .filter(key -> !key.equals(deletionFence))
              .limit(DIAGNOSTIC_KEY_LIMIT)
              .toList();
      throw new BaseResourceRepository.AbortRetryableException(
          "account pointer sweep left "
              + remaining
              + " rows under "
              + accountPrefix
              + "; deletion_fence_present="
              + (remainingFence != null)
              + " unexpected_pointer_count="
              + unexpectedCount
              + " unexpected_pointer_keys="
              + unexpectedKeys
              + " unexpected_pointer_keys_truncated="
              + (unexpectedCount > unexpectedKeys.size()));
    }
  }

  private static String parseAccountId(String key) {
    String prefix = Keys.accountPurgePrefix();
    if (key == null || !key.startsWith(prefix)) return null;
    String encoded = key.substring(prefix.length());
    if (encoded.isBlank() || encoded.indexOf('/') >= 0) return null;
    try {
      String accountId = Keys.decodeSegment(encoded);
      return accountId.isBlank() ? null : accountId;
    } catch (IllegalArgumentException failure) {
      return null;
    }
  }
}
