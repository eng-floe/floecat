/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.account.impl.AccountCredentialCleanup;
import ai.floedb.floecat.service.cache.HintCache;
import ai.floedb.floecat.service.cache.ObjectCache;
import ai.floedb.floecat.service.reconciler.jobs.DurableReconcileJobStore;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccountPurgeTest {
  private AccountPurge purge;
  private FailingPointerStore pointers;
  private InMemoryBlobStore blobStore;
  private DurableReconcileJobStore jobs;

  @BeforeEach
  void setUp() {
    purge = new AccountPurge();
    pointers = new FailingPointerStore();
    blobStore = new InMemoryBlobStore();
    jobs = mock(DurableReconcileJobStore.class);
    Instance<DurableReconcileJobStore> jobsInstance = mock(Instance.class);
    when(jobsInstance.isResolvable()).thenReturn(true);
    when(jobsInstance.get()).thenReturn(jobs);
    purge.pointerStore = pointers;
    purge.blobStore = blobStore;
    purge.durableReconcileJobStore = jobsInstance;
    purge.objects = ObjectCache.forTesting();
    purge.hints = mock(HintCache.class);
    purge.blobs = mock(BlobCacheAccess.class);
    purge.accounts = mock(OwnedAccounts.class);
    purge.credentialCleanup = mock(AccountCredentialCleanup.class);
  }

  @Test
  void purgesAccountAndAcknowledgesQueueEntry() {
    installFencedPurge("acct");
    String pointer = Keys.tablePointerById("acct", "table");
    String blob = Keys.tableBlobPrefix("acct", "table") + "orphan.pb";
    put(pointer, "broken");
    blobStore.put(blob, new byte[] {1}, "application/octet-stream");
    when(purge.accounts.owns("acct")).thenReturn(true);
    when(jobs.cleanupAccount("acct")).thenReturn(3);

    AccountPurge.Result result = purge.drain(System.currentTimeMillis() + 30_000L, 100);

    assertEquals(new AccountPurge.Result(1, 1), result);
    assertFalse(pointers.get(pointer).isPresent());
    assertFalse(pointers.get(Keys.accountPurgePointer("acct")).isPresent());
    assertTrue(pointers.get(Keys.accountDeletionMarker("acct")).isPresent());
    assertTrue(blobStore.get(blob) == null);
    verify(jobs).cleanupAccount("acct");
    verify(purge.credentialCleanup).revoke("acct");
    verify(purge.hints).evictAccount("acct");
    verify(purge.blobs).evictAccount("acct");
  }

  @Test
  void skipsAccountOwnedByAnotherReplica() {
    installFencedPurge("acct");
    when(purge.accounts.owns("acct")).thenReturn(false);

    assertEquals(
        new AccountPurge.Result(1, 0), purge.drain(System.currentTimeMillis() + 30_000L, 100));
    assertTrue(pointers.get(Keys.accountPurgePointer("acct")).isPresent());
  }

  @Test
  void skipsUnfencedAccount() {
    installQueue("acct");
    when(purge.accounts.owns("acct")).thenReturn(true);

    assertEquals(
        new AccountPurge.Result(1, 0), purge.drain(System.currentTimeMillis() + 30_000L, 100));
    assertTrue(pointers.get(Keys.accountPurgePointer("acct")).isPresent());
  }

  @Test
  void skipsFencedAccountUntilItsRowIsDeleted() {
    installFencedPurge("acct");
    when(purge.accounts.owns("acct")).thenReturn(true);
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("acct")
            .setKind(ResourceKind.RK_ACCOUNT)
            .build();
    when(purge.accounts.getById(id))
        .thenReturn(java.util.Optional.of(Account.newBuilder().setResourceId(id).build()));

    assertEquals(
        new AccountPurge.Result(1, 0), purge.drain(System.currentTimeMillis() + 30_000L, 100));
    assertTrue(pointers.get(Keys.accountPurgePointer("acct")).isPresent());
  }

  @Test
  void failedPurgeRemainsQueuedAndNextDrainFinishes() {
    installFencedPurge("acct");
    put(Keys.tablePointerById("acct", "table"), "broken");
    when(purge.accounts.owns("acct")).thenReturn(true);
    pointers.failNextSweep = true;

    assertEquals(
        new AccountPurge.Result(1, 0), purge.drain(System.currentTimeMillis() + 30_000L, 100));
    assertTrue(pointers.get(Keys.accountPurgePointer("acct")).isPresent());

    assertEquals(
        new AccountPurge.Result(1, 1), purge.drain(System.currentTimeMillis() + 30_000L, 100));
    assertFalse(pointers.get(Keys.accountPurgePointer("acct")).isPresent());
  }

  @Test
  void credentialRevocationFailureLeavesRowsAndPurgeEntryForRetry() {
    installFencedPurge("acct");
    String pointer = Keys.tablePointerById("acct", "table");
    put(pointer, "live-until-credentials-are-revoked");
    when(purge.accounts.owns("acct")).thenReturn(true);
    doThrow(new IllegalStateException("secret backend unavailable"))
        .when(purge.credentialCleanup)
        .revoke("acct");

    assertEquals(
        new AccountPurge.Result(1, 0), purge.drain(System.currentTimeMillis() + 30_000L, 100));

    assertTrue(pointers.get(pointer).isPresent());
    assertTrue(pointers.get(Keys.accountPurgePointer("acct")).isPresent());
  }

  @Test
  void dropsMalformedQueueEntry() {
    String malformed = Keys.accountPurgePrefix() + "bad/extra";
    put(malformed, "bad");

    assertEquals(
        new AccountPurge.Result(1, 1), purge.drain(System.currentTimeMillis() + 30_000L, 100));
    assertFalse(pointers.get(malformed).isPresent());
  }

  @Test
  void incompletePointerSweepReportsBoundedUnexpectedKeys() {
    String accountPrefix = Keys.accountRootPrefix("acct");
    String deletionFence = Keys.accountDeletionMarker("acct");
    put(deletionFence, "deletion-fence");
    for (char suffix = 'a'; suffix <= 'l'; suffix++) {
      put(accountPrefix + "residual-" + suffix, "residual-" + suffix);
    }

    BaseResourceRepository.AbortRetryableException failure =
        assertThrows(
            BaseResourceRepository.AbortRetryableException.class,
            () -> purge.assertAccountPointerSweepComplete(accountPrefix, deletionFence));

    assertTrue(failure.getMessage().contains("unexpected_pointer_count=12"));
    assertTrue(failure.getMessage().contains("unexpected_pointer_keys_truncated=true"));
  }

  private void installFencedPurge(String accountId) {
    installQueue(accountId);
    put(Keys.accountDeletionMarker(accountId), "deleting");
  }

  private void installQueue(String accountId) {
    put(Keys.accountPurgePointer(accountId), accountId);
  }

  private void put(String key, String payload) {
    pointers.compareAndSet(key, 0L, PointerReferences.opaqueMarkerPointer(key, payload, 1L));
  }

  private static final class FailingPointerStore extends InMemoryPointerStore {
    private boolean failNextSweep;

    @Override
    public synchronized int deleteByPrefixExcluding(String prefix, String excludedKey) {
      if (failNextSweep) {
        failNextSweep = false;
        throw new BaseResourceRepository.AbortRetryableException("injected sweep failure");
      }
      return super.deleteByPrefixExcluding(prefix, excludedKey);
    }
  }
}
