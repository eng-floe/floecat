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
import static org.junit.jupiter.api.Assertions.assertTrue;
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

class CasBlobGcSchedulerTest {

  /**
   * One account's sweep throwing (e.g. a transient S3 fault, or 403 AccessDenied when the role
   * lacks the now-required {@code s3:DeleteObjectVersion}) must not starve the rest of the tick.
   * {@code runForAccount} is called inside the per-account loop; without a guard, one exception
   * would end the tick and skip every remaining shuffled account.
   */
  @Test
  void tickAdvancesPastAnAccountWhoseSweepThrows() {
    AccountRepository accounts = mock(AccountRepository.class);
    when(accounts.list(anyInt(), anyString(), any()))
        .thenReturn(List.of(account("acct-a"), account("acct-b")));

    RecordingGc gc = new RecordingGc();
    gc.failAccountId = "acct-a";

    CasBlobGcScheduler scheduler = new CasBlobGcScheduler();
    scheduler.accounts = () -> accounts;
    scheduler.casBlobGc = () -> gc;
    scheduler.observability = new TestObservability();
    scheduler.initMeters();

    // Test config disables CAS GC; a system property (higher config ordinal) re-enables it for the
    // direct tick() invocation.
    System.setProperty("floecat.gc.cas.enabled", "true");
    try {
      scheduler.tick();
    } finally {
      System.clearProperty("floecat.gc.cas.enabled");
    }

    // Both accounts were attempted; the throw on acct-a did not abort the tick before acct-b.
    assertEquals(2, gc.accountIds.size());
    assertTrue(gc.accountIds.contains("acct-a"));
    assertTrue(gc.accountIds.contains("acct-b"));
  }

  private static Account account(String accountId) {
    return Account.newBuilder()
        .setResourceId(
            ResourceId.newBuilder().setId(accountId).setKind(ResourceKind.RK_ACCOUNT).build())
        .setDisplayName(accountId)
        .build();
  }

  private static final class RecordingGc extends CasBlobGc {
    private final List<String> accountIds = new ArrayList<>();
    private String failAccountId;

    @Override
    public Result runForAccount(String accountId) {
      accountIds.add(accountId);
      if (accountId.equals(failAccountId)) {
        throw new RuntimeException("simulated storage fault");
      }
      return new Result(0, 0, 0, 0, 0, 0, false, false);
    }
  }
}
