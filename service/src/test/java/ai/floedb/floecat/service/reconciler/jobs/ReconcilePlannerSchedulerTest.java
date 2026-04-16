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

package ai.floedb.floecat.service.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.ReconcileMode;
import ai.floedb.floecat.connector.rpc.ReconcilePolicy;
import ai.floedb.floecat.reconciler.impl.ReconcileExecutorRegistry;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ReconcilePlannerSchedulerTest {

  @Test
  void runPlannerPassUsesOpaqueAccountTokensBetweenPages() {
    TestScheduler scheduler = new TestScheduler();
    scheduler.accounts = mock(AccountRepository.class);
    scheduler.connectors = mock(ConnectorRepository.class);
    scheduler.jobs = mock(ReconcileJobStore.class);
    scheduler.executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(scheduler.executorRegistry.hasExecutorForJobKind(any())).thenReturn(true);

    List<String> enqueued = new ArrayList<>();
    when(scheduler.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenAnswer(
            invocation -> {
              enqueued.add(invocation.getArgument(1, String.class));
              return "job-" + enqueued.size();
            });

    Account accountA = account("acct-a", "alpha");
    Account accountB = account("acct-b", "bravo");
    when(scheduler.accounts.list(anyInt(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              String token = invocation.getArgument(1, String.class);
              StringBuilder next = invocation.getArgument(2, StringBuilder.class);
              next.setLength(0);
              if (token == null || token.isBlank()) {
                next.append("opaque-account-token-2");
                return List.of(accountA);
              }
              if ("opaque-account-token-2".equals(token)) {
                return List.of(accountB);
              }
              return List.of();
            });

    when(scheduler.connectors.list(anyString(), anyInt(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              String accountId = invocation.getArgument(0, String.class);
              if ("acct-a".equals(accountId)) {
                return List.of(connector("acct-a", "conn-a1", "alpha-1"));
              }
              if ("acct-b".equals(accountId)) {
                return List.of(connector("acct-b", "conn-b1", "bravo-1"));
              }
              return List.of();
            });

    scheduler.runPlannerPass(5L, 1, 10, 1L, ReconcileMode.RM_INCREMENTAL);

    assertEquals(List.of("conn-a1"), enqueued);
    assertEquals("opaque-account-token-2", scheduler.plannerCursor().accountToken());

    scheduler.runPlannerPass(100L, 1, 10, 1L, ReconcileMode.RM_INCREMENTAL);

    assertEquals(List.of("conn-a1", "conn-b1"), enqueued);
    assertEquals("", scheduler.plannerCursor().accountToken());
  }

  @Test
  void runPlannerPassUsesOpaqueConnectorTokensWithinAccount() {
    TestScheduler scheduler = new TestScheduler();
    scheduler.accounts = mock(AccountRepository.class);
    scheduler.connectors = mock(ConnectorRepository.class);
    scheduler.jobs = mock(ReconcileJobStore.class);
    scheduler.executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(scheduler.executorRegistry.hasExecutorForJobKind(any())).thenReturn(true);

    List<String> enqueued = new ArrayList<>();
    List<String> connectorTokens = new ArrayList<>();
    when(scheduler.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenAnswer(
            invocation -> {
              enqueued.add(invocation.getArgument(1, String.class));
              return "job-" + enqueued.size();
            });

    when(scheduler.accounts.list(anyInt(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              String token = invocation.getArgument(1, String.class);
              if (token == null || token.isBlank()) {
                return List.of(account("acct-a", "alpha"));
              }
              return List.of();
            });

    when(scheduler.connectors.list(anyString(), anyInt(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              String token = invocation.getArgument(2, String.class);
              StringBuilder next = invocation.getArgument(3, StringBuilder.class);
              connectorTokens.add(token);
              next.setLength(0);
              if (token == null || token.isBlank()) {
                next.append("opaque-connector-token-2");
                return List.of(connector("acct-a", "conn-a1", "alpha-1"));
              }
              if ("opaque-connector-token-2".equals(token)) {
                return List.of(connector("acct-a", "conn-a2", "alpha-2"));
              }
              return List.of();
            });

    scheduler.runPlannerPass(100L, 10, 1, 1L, ReconcileMode.RM_INCREMENTAL);

    assertEquals(List.of("", "opaque-connector-token-2"), connectorTokens);
    assertEquals(List.of("conn-a1", "conn-a2"), enqueued);
  }

  @Test
  void runPlannerPassStopsWhenConnectorPageTokenStagnates() {
    TestScheduler scheduler = new TestScheduler();
    scheduler.accounts = mock(AccountRepository.class);
    scheduler.connectors = mock(ConnectorRepository.class);
    scheduler.jobs = mock(ReconcileJobStore.class);
    scheduler.executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(scheduler.executorRegistry.hasExecutorForJobKind(any())).thenReturn(true);

    List<String> enqueued = new ArrayList<>();
    when(scheduler.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenAnswer(
            invocation -> {
              enqueued.add(invocation.getArgument(1, String.class));
              return "job-" + enqueued.size();
            });

    when(scheduler.accounts.list(anyInt(), anyString(), any()))
        .thenReturn(List.of(account("acct-a", "alpha")));
    when(scheduler.connectors.list(anyString(), anyInt(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              String token = invocation.getArgument(2, String.class);
              StringBuilder next = invocation.getArgument(3, StringBuilder.class);
              next.setLength(0);
              next.append(token == null ? "" : token);
              return List.of(connector("acct-a", "conn-a1", "alpha-1"));
            });

    scheduler.runPlannerPass(100L, 10, 1, 1L, ReconcileMode.RM_INCREMENTAL);

    assertEquals(List.of("conn-a1"), enqueued);
  }

  @Test
  void runPlannerPassHonorsDeadlineWithinLargeAccount() {
    TestScheduler scheduler = new TestScheduler();
    scheduler.accounts = mock(AccountRepository.class);
    scheduler.connectors = mock(ConnectorRepository.class);
    scheduler.jobs = mock(ReconcileJobStore.class);
    scheduler.executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(scheduler.executorRegistry.hasExecutorForJobKind(any())).thenReturn(true);

    List<String> enqueued = new ArrayList<>();
    when(scheduler.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenAnswer(
            invocation -> {
              enqueued.add(invocation.getArgument(1, String.class));
              return "job-" + enqueued.size();
            });

    when(scheduler.accounts.list(anyInt(), anyString(), any()))
        .thenReturn(List.of(account("acct-a", "alpha"), account("acct-b", "bravo")));
    when(scheduler.connectors.list(anyString(), anyInt(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              String accountId = invocation.getArgument(0, String.class);
              if ("acct-a".equals(accountId)) {
                return List.of(
                    connector("acct-a", "conn-a1", "alpha-1"),
                    connector("acct-a", "conn-a2", "alpha-2"));
              }
              return List.of(connector("acct-b", "conn-b1", "bravo-1"));
            });

    scheduler.runPlannerPass(5L, 10, 10, 1L, ReconcileMode.RM_INCREMENTAL);

    assertEquals(List.of("conn-a1"), enqueued);
    assertEquals("", scheduler.plannerCursor().accountToken());
  }

  @Test
  void runPlannerPassSkipsConnectorsWithDisabledPolicy() {
    TestScheduler scheduler = new TestScheduler();
    scheduler.accounts = mock(AccountRepository.class);
    scheduler.connectors = mock(ConnectorRepository.class);
    scheduler.jobs = mock(ReconcileJobStore.class);
    scheduler.executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(scheduler.executorRegistry.hasExecutorForJobKind(any())).thenReturn(true);

    List<String> enqueued = new ArrayList<>();
    when(scheduler.jobs.enqueuePlan(
            anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString()))
        .thenAnswer(
            invocation -> {
              enqueued.add(invocation.getArgument(1, String.class));
              return "job-" + enqueued.size();
            });

    when(scheduler.accounts.list(anyInt(), anyString(), any()))
        .thenReturn(List.of(account("acct-a", "alpha")));
    when(scheduler.connectors.list(anyString(), anyInt(), anyString(), any()))
        .thenReturn(
            List.of(
                connector("acct-a", "conn-disabled", "alpha-1", false),
                connector("acct-a", "conn-enabled", "alpha-2", true)));

    scheduler.runPlannerPass(100L, 10, 10, 1L, ReconcileMode.RM_INCREMENTAL);

    assertEquals(List.of("conn-enabled"), enqueued);
  }

  @Test
  void runPlannerPassUsesConfiguredExecutionPolicyForPlanJobs() {
    TestScheduler scheduler = new TestScheduler();
    scheduler.accounts = mock(AccountRepository.class);
    scheduler.connectors = mock(ConnectorRepository.class);
    scheduler.jobs = mock(ReconcileJobStore.class);
    scheduler.executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(scheduler.executorRegistry.hasExecutorForJobKind(any())).thenReturn(true);

    String priorClass = System.getProperty("floecat.reconciler.auto.execution-class");
    String priorLane = System.getProperty("floecat.reconciler.auto.execution-lane");
    System.setProperty("floecat.reconciler.auto.execution-class", "HEAVY");
    System.setProperty("floecat.reconciler.auto.execution-lane", "planner-lane");
    try {
      when(scheduler.accounts.list(anyInt(), anyString(), any()))
          .thenReturn(List.of(account("acct-a", "alpha")));
      when(scheduler.connectors.list(anyString(), anyInt(), anyString(), any()))
          .thenReturn(List.of(connector("acct-a", "conn-a1", "alpha-1")));
      when(scheduler.jobs.enqueuePlan(
              anyString(),
              anyString(),
              anyBoolean(),
              any(),
              any(),
              eq(
                  ReconcileExecutionPolicy.of(
                      ReconcileExecutionClass.HEAVY, "planner-lane", java.util.Map.of())),
              anyString()))
          .thenReturn("job-1");

      scheduler.runPlannerPass(100L, 10, 10, 1L, ReconcileMode.RM_INCREMENTAL);
    } finally {
      restoreProperty("floecat.reconciler.auto.execution-class", priorClass);
      restoreProperty("floecat.reconciler.auto.execution-lane", priorLane);
    }
  }

  @Test
  void runPlannerPassDoesNotEnqueueWhenPlannerExecutorIsUnavailable() {
    TestScheduler scheduler = new TestScheduler();
    scheduler.accounts = mock(AccountRepository.class);
    scheduler.connectors = mock(ConnectorRepository.class);
    scheduler.jobs = mock(ReconcileJobStore.class);
    scheduler.executorRegistry = mock(ReconcileExecutorRegistry.class);

    when(scheduler.executorRegistry.hasExecutorForJobKind(any())).thenReturn(false);
    when(scheduler.accounts.list(anyInt(), anyString(), any()))
        .thenReturn(List.of(account("acct-a", "alpha")));
    when(scheduler.connectors.list(anyString(), anyInt(), anyString(), any()))
        .thenReturn(List.of(connector("acct-a", "conn-a1", "alpha-1")));

    scheduler.runPlannerPass(100L, 10, 10, 1L, ReconcileMode.RM_INCREMENTAL);

    org.mockito.Mockito.verify(scheduler.jobs, never())
        .enqueuePlan(anyString(), anyString(), anyBoolean(), any(), any(), any(), anyString());
  }

  private static Account account(String accountId, String displayName) {
    return Account.newBuilder()
        .setResourceId(
            ResourceId.newBuilder().setId(accountId).setKind(ResourceKind.RK_ACCOUNT).build())
        .setDisplayName(displayName)
        .build();
  }

  private static Connector connector(String accountId, String id, String displayName) {
    return connector(accountId, id, displayName, true);
  }

  private static Connector connector(
      String accountId, String id, String displayName, boolean policyEnabled) {
    return Connector.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(id)
                .setKind(ResourceKind.RK_CONNECTOR)
                .build())
        .setDisplayName(displayName)
        .setState(ConnectorState.CS_ACTIVE)
        .setPolicy(ReconcilePolicy.newBuilder().setEnabled(policyEnabled).build())
        .build();
  }

  private static final class TestScheduler extends ReconcilePlannerScheduler {
    private long nowMs;

    @Override
    long nowMs() {
      return nowMs++;
    }
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }
}
