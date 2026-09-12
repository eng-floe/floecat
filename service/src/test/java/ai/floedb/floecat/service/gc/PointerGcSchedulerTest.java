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

package ai.floedb.floecat.service.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.account.AccountAssignment;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.telemetry.TestObservability;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PointerGcSchedulerTest {
  private static final String INCARNATION = "m/inc";

  @Test
  void managedTickCollectsOnlyGcAllowedAccounts() {
    AccountRepository accounts = mock(AccountRepository.class);
    when(accounts.list(anyInt(), anyString(), any()))
        .thenReturn(List.of(account("acct-a"), account("acct-b"), account("acct-c")));
    RecordingPointerGc gc = new RecordingPointerGc();
    TestObservability observability = new TestObservability();
    AccountAssignment assignment =
        AccountAssignment.managedForTesting(
            "m", INCARNATION, new InMemoryPointerStore(), observability);
    // acct-a and acct-b may be collected, acct-c is owned but gated, acct-d is not owned at all.
    assignment.apply(
        1L,
        AccountAssignment.AssignmentMode.SERVING,
        List.of("acct-a", "acct-b", "acct-c"),
        List.of("acct-a", "acct-b"),
        INCARNATION);
    PointerGcScheduler scheduler = new PointerGcScheduler();
    scheduler.accounts = () -> accounts;
    scheduler.pointerGc = () -> gc;
    scheduler.assignment = assignment;
    scheduler.observability = observability;
    scheduler.initMeters();

    System.setProperty("floecat.gc.pointer.enabled", "true");
    try {
      scheduler.tick();
    } finally {
      System.clearProperty("floecat.gc.pointer.enabled");
    }

    assertThat(gc.globalRuns).as("directory GC runs on every process").isEqualTo(1);
    assertThat(gc.accountIds).containsExactlyInAnyOrder("acct-a", "acct-b");
    for (String accountId : List.of("acct-a", "acct-b", "acct-c")) {
      assertThat(assignment.status(accountId).activeGc()).as(accountId).isZero();
    }
  }

  @Test
  void standaloneTickCollectsEveryAccount() {
    AccountRepository accounts = mock(AccountRepository.class);
    when(accounts.list(anyInt(), anyString(), any()))
        .thenReturn(List.of(account("acct-a"), account("acct-b")));
    RecordingPointerGc gc = new RecordingPointerGc();
    TestObservability observability = new TestObservability();
    PointerGcScheduler scheduler = new PointerGcScheduler();
    scheduler.accounts = () -> accounts;
    scheduler.pointerGc = () -> gc;
    scheduler.assignment =
        AccountAssignment.standaloneForTesting(new InMemoryPointerStore(), observability);
    scheduler.observability = observability;
    scheduler.initMeters();

    System.setProperty("floecat.gc.pointer.enabled", "true");
    try {
      scheduler.tick();
    } finally {
      System.clearProperty("floecat.gc.pointer.enabled");
    }

    assertThat(gc.accountIds).containsExactlyInAnyOrder("acct-a", "acct-b");
  }

  private static Account account(String accountId) {
    return Account.newBuilder()
        .setResourceId(
            ResourceId.newBuilder().setId(accountId).setKind(ResourceKind.RK_ACCOUNT).build())
        .setDisplayName(accountId)
        .build();
  }

  private static final class RecordingPointerGc extends PointerGc {
    private final List<String> accountIds = new ArrayList<>();
    private int globalRuns;

    @Override
    public Result runGlobalAccountPointers(long deadlineMs) {
      globalRuns++;
      return new Result(0, 0, 0, 0);
    }

    @Override
    public Result runForAccount(String accountId, long deadlineMs) {
      accountIds.add(accountId);
      return new Result(1, 0, 0, 0);
    }
  }
}
