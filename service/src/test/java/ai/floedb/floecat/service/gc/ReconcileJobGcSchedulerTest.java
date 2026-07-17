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
import java.util.ArrayDeque;
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

  @Test
  void completedLegacyLookupMigrationRunsOnlyOnce() {
    AccountRepository accounts = mock(AccountRepository.class);
    when(accounts.list(anyInt(), anyString(), any())).thenReturn(List.of());
    RecordingGc gc = new RecordingGc();
    ReconcileJobGcScheduler scheduler = new ReconcileJobGcScheduler();
    scheduler.accounts = () -> accounts;
    scheduler.reconcileJobGc = () -> gc;
    scheduler.observability = new TestObservability();
    scheduler.initMeters();

    scheduler.tick();
    scheduler.tick();

    assertEquals(1, gc.lookupMigrationCalls);
  }

  @Test
  void legacyLookupMigrationStopsAfterFullPassMakesNoProgress() {
    AccountRepository accounts = mock(AccountRepository.class);
    when(accounts.list(anyInt(), anyString(), any())).thenReturn(List.of());
    RecordingGc gc = new RecordingGc();
    gc.lookupMigrationResults.add(new ReconcileJobGc.LookupMigrationResult(2, 1, 1, 0, ""));
    gc.lookupMigrationResults.add(new ReconcileJobGc.LookupMigrationResult(1, 0, 1, 0, ""));
    ReconcileJobGcScheduler scheduler = new ReconcileJobGcScheduler();
    scheduler.accounts = () -> accounts;
    scheduler.reconcileJobGc = () -> gc;
    scheduler.observability = new TestObservability();
    scheduler.initMeters();

    scheduler.tick();
    scheduler.tick();
    scheduler.tick();

    assertEquals(2, gc.lookupMigrationCalls);
  }

  @Test
  void legacyLookupMigrationRetriesAfterRetryableFullPass() {
    AccountRepository accounts = mock(AccountRepository.class);
    when(accounts.list(anyInt(), anyString(), any())).thenReturn(List.of());
    RecordingGc gc = new RecordingGc();
    gc.lookupMigrationResults.add(new ReconcileJobGc.LookupMigrationResult(1, 0, 0, 1, ""));
    gc.lookupMigrationResults.add(new ReconcileJobGc.LookupMigrationResult(1, 1, 0, 0, ""));
    gc.lookupMigrationResults.add(new ReconcileJobGc.LookupMigrationResult(0, 0, 0, 0, ""));
    ReconcileJobGcScheduler scheduler = new ReconcileJobGcScheduler();
    scheduler.accounts = () -> accounts;
    scheduler.reconcileJobGc = () -> gc;
    scheduler.observability = new TestObservability();
    scheduler.initMeters();

    scheduler.tick();
    scheduler.tick();
    scheduler.tick();
    scheduler.tick();

    assertEquals(3, gc.lookupMigrationCalls);
  }

  @Test
  void legacyCleanupMigrationRetriesBeforeWritingCompletionMarker() {
    AccountRepository accounts = mock(AccountRepository.class);
    when(accounts.list(anyInt(), anyString(), any())).thenReturn(List.of());
    RecordingGc gc = new RecordingGc();
    gc.cleanupMigrationResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 2, 0, 1, ""));
    gc.cleanupMigrationResults.add(new ReconcileJobGc.CleanupMigrationResult(10, 3, 0, 0, ""));
    ReconcileJobGcScheduler scheduler = new ReconcileJobGcScheduler();
    scheduler.accounts = () -> accounts;
    scheduler.reconcileJobGc = () -> gc;
    scheduler.observability = new TestObservability();
    scheduler.initMeters();

    scheduler.tick();
    scheduler.tick();
    scheduler.tick();

    assertEquals(2, gc.cleanupMigrationCalls);
    assertEquals(1, gc.cleanupMigrationCompletionCalls);
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
    private final ArrayDeque<CleanupMigrationResult> cleanupMigrationResults = new ArrayDeque<>();
    private final ArrayDeque<LookupMigrationResult> lookupMigrationResults = new ArrayDeque<>();
    private String failAccountId;
    private int cleanupMigrationCalls;
    private int cleanupMigrationCompletionCalls;
    private int lookupMigrationCalls;

    @Override
    public CleanupMigrationResult runLegacyCleanupMigrationSlice(String pageToken) {
      cleanupMigrationCalls++;
      return cleanupMigrationResults.isEmpty()
          ? new CleanupMigrationResult(0, 0, 0, 0, "")
          : cleanupMigrationResults.removeFirst();
    }

    @Override
    public boolean completeLegacyCleanupMigration() {
      cleanupMigrationCompletionCalls++;
      return true;
    }

    @Override
    public LookupMigrationResult runLegacyLookupMigrationSlice(String pageToken) {
      lookupMigrationCalls++;
      return lookupMigrationResults.isEmpty()
          ? new LookupMigrationResult(0, 0, 0, 0, "")
          : lookupMigrationResults.removeFirst();
    }

    @Override
    public GlobalResult runReadySlice(String pageToken) {
      return new GlobalResult(0, 0, 0, "");
    }

    @Override
    public AccountResult runAccountSlice(
        String accountId,
        String pageToken,
        String canonicalQuarantinePageToken,
        String dedupePageToken,
        String rootSummaryPageToken,
        String connectorRootSummaryPageToken) {
      accountIds.add(accountId);
      if (accountId.equals(failAccountId)) {
        throw new RuntimeException("failed account");
      }
      return new AccountResult(0, 0, 0, 0, 0, 0, 0, 0, 0, "", "", "", "", "");
    }
  }
}
