/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.account.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AccountAwsExternalIdProviderTest {
  @Test
  void racingFirstUsePersistsAndReturnsOneExternalId() throws Exception {
    var repository = mock(AccountRepository.class);
    var id =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("account")
            .setKind(ResourceKind.RK_ACCOUNT)
            .build();
    var value = new AtomicReference<>(Account.newBuilder().setResourceId(id).build());
    var version = new AtomicLong(1L);
    when(repository.getById(any())).thenAnswer(ignored -> Optional.of(value.get()));
    var firstReads = new AtomicInteger();
    var bothReadMissing = new CountDownLatch(2);
    var releaseReads = new CountDownLatch(1);
    when(repository.getByIdWithMetaForMutation(any()))
        .thenAnswer(
            ignored -> {
              Account snapshot = value.get();
              long snapshotVersion = version.get();
              if (snapshot.getAwsExternalId().isBlank() && firstReads.incrementAndGet() <= 2) {
                bothReadMissing.countDown();
                releaseReads.await();
              }
              return Optional.of(
                  new ResourceWithMeta<>(
                      snapshot,
                      MutationMeta.newBuilder().setPointerVersion(snapshotVersion).build()));
            });
    when(repository.update(any(), anyLong()))
        .thenAnswer(
            invocation -> {
              Account proposed = invocation.getArgument(0);
              long expected = invocation.getArgument(1);
              synchronized (value) {
                if (version.get() != expected) return false;
                value.set(proposed);
                version.incrementAndGet();
                return true;
              }
            });
    var issued = new AtomicInteger();
    var provider = new AccountAwsExternalIdProvider();
    provider.accounts = repository;
    provider.externalIds = () -> "floecat-external-" + issued.incrementAndGet();

    var firstResult = new AtomicReference<String>();
    var secondResult = new AtomicReference<String>();
    var first = Thread.ofVirtual().start(() -> firstResult.set(provider.getOrCreate("account")));
    var second = Thread.ofVirtual().start(() -> secondResult.set(provider.getOrCreate("account")));
    bothReadMissing.await();
    releaseReads.countDown();
    first.join();
    second.join();

    assertEquals(value.get().getAwsExternalId(), firstResult.get());
    assertEquals(firstResult.get(), secondResult.get());
    assertEquals(firstResult.get(), provider.getOrCreate("account"));
    assertEquals(1L, version.get() - 1L);
    assertEquals(2, issued.get());
  }

  @Test
  void missingAccountFailsClosed() {
    var repository = mock(AccountRepository.class);
    when(repository.getById(any())).thenReturn(Optional.empty());
    when(repository.getByIdWithMetaForMutation(any())).thenReturn(Optional.empty());
    var provider = new AccountAwsExternalIdProvider();
    provider.accounts = repository;

    assertThrows(
        AccountAwsExternalIdProvider.AccountMissingException.class,
        () -> provider.getOrCreate("missing"));
  }

  @Test
  void existingExternalIdUsesTheCachedRepositoryReadOnly() {
    var repository = mock(AccountRepository.class);
    when(repository.getById(any()))
        .thenReturn(Optional.of(Account.newBuilder().setAwsExternalId("floecat-existing").build()));
    var provider = new AccountAwsExternalIdProvider();
    provider.accounts = repository;

    assertEquals("floecat-existing", provider.getOrCreate("account"));

    verify(repository, never()).getByIdWithMetaForMutation(any());
    verify(repository, never()).update(any(), anyLong());
  }

  @Test
  void deletionFenceDuringInitializationFailsAsAMissingAccount() {
    var repository = mock(AccountRepository.class);
    var account = Account.newBuilder().build();
    when(repository.getById(any())).thenReturn(Optional.of(account));
    when(repository.getByIdWithMetaForMutation(any()))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    account, MutationMeta.newBuilder().setPointerVersion(1L).build())));
    when(repository.update(any(), anyLong()))
        .thenThrow(
            new ai.floedb.floecat.service.repo.util.BaseResourceRepository
                .AccountDeletionInProgressException("account"));
    var provider = new AccountAwsExternalIdProvider();
    provider.accounts = repository;

    assertThrows(
        AccountAwsExternalIdProvider.AccountMissingException.class,
        () -> provider.getOrCreate("account"));
  }

  @Test
  void sessionNameIsStableAndAccountSpecific() {
    assertEquals(
        AccountAwsExternalIdProvider.roleSessionName("account"),
        AccountAwsExternalIdProvider.roleSessionName("account"));
    org.junit.jupiter.api.Assertions.assertNotEquals(
        AccountAwsExternalIdProvider.roleSessionName("account-one"),
        AccountAwsExternalIdProvider.roleSessionName("account-two"));
  }
}
