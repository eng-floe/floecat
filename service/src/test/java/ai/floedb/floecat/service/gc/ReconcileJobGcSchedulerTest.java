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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.telemetry.TestObservability;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ReconcileJobGcSchedulerTest {

  @Test
  void tickAdvancesPastFailingAccountSlice() {
    AccountRepository accounts = mock(AccountRepository.class);
    when(accounts.list(anyInt(), anyString(), any()))
        .thenReturn(List.of(account("acct-a"), account("acct-b")));

    RecordingGc gc = new RecordingGc();
    gc.failAccountId = "acct-a";

    ReconcileJobGcScheduler scheduler = new ReconcileJobGcScheduler();
    scheduler.accounts = () -> accounts;
    scheduler.reconcileJobGc = () -> gc;
    scheduler.observability = new TestObservability();
    scheduler.initMeters();

    scheduler.tick();

    assertEquals(List.of("acct-a", "acct-b"), gc.accountIds);
  }

  private static Account account(String accountId) {
    return Account.newBuilder()
        .setResourceId(
            ResourceId.newBuilder().setId(accountId).setKind(ResourceKind.RK_ACCOUNT).build())
        .setDisplayName(accountId)
        .build();
  }

  private static final class RecordingGc extends ReconcileJobGc {
    private final List<String> accountIds = new ArrayList<>();
    private String failAccountId;

    @Override
    public boolean terminalRetentionBackfillComplete(String accountId) {
      return true;
    }

    @Override
    public AccountResult runAccountSlice(
        String accountId, String pageToken, String canonicalQuarantinePageToken, long deadlineMs) {
      accountIds.add(accountId);
      if (accountId.equals(failAccountId)) {
        throw new RuntimeException("failed account");
      }
      return new AccountResult(0, 0, 0, 0, 0, 0, "", "", 0, 0, 0L, 0L);
    }
  }
}
