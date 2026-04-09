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

package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.rpc.CompleteLeasedReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcileCancellationRequest;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.ReconcileCompletionState;
import ai.floedb.floecat.reconciler.rpc.RenewReconcileLeaseRequest;
import ai.floedb.floecat.reconciler.rpc.ReportReconcileProgressRequest;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconcileExecutorControlImplTest {
  private ReconcileExecutorControlImpl service;

  @BeforeEach
  void setUp() {
    service = new ReconcileExecutorControlImpl();
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.jobs = mock(ReconcileJobStore.class);

    PrincipalContext principalContext = mock(PrincipalContext.class);
    when(service.principalProvider.get()).thenReturn(principalContext);
    when(principalContext.getCorrelationId()).thenReturn("corr");
    doNothing().when(service.authz).require(any(), eq("connector.manage"));
  }

  @Test
  void leaseReconcileJobUsesExecutorAwareLeaseFilterAndMapsLease() {
    when(service.jobs.leaseNext(any()))
        .thenReturn(
            Optional.of(
                new ReconcileJobStore.LeasedJob(
                    "job-1",
                    "acct",
                    "connector-1",
                    false,
                    CaptureMode.METADATA_AND_STATS,
                    ReconcileScope.of(
                        java.util.List.of(java.util.List.of("db")),
                        "orders",
                        java.util.List.of("id")),
                    ReconcileExecutionPolicy.of(
                        ReconcileExecutionClass.HEAVY, "remote", Map.of("tier", "gold")),
                    "lease-1",
                    "remote-executor",
                    "")));

    var response =
        service
            .leaseReconcileJob(
                LeaseReconcileJobRequest.newBuilder()
                    .setExecutorId("remote-executor")
                    .addExecutionClasses(ai.floedb.floecat.reconciler.rpc.ExecutionClass.EC_HEAVY)
                    .addLanes("remote")
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getFound());
    assertEquals("job-1", response.getJob().getJobId());
    assertEquals("connector-1", response.getJob().getConnectorId().getId());
    assertEquals("orders", response.getJob().getScope().getDestinationTableDisplayName());
    assertEquals("remote-executor", response.getJob().getPinnedExecutorId());
    verify(service.jobs)
        .leaseNext(
            argThat(
                request ->
                    request != null
                        && request.executionClasses.contains(ReconcileExecutionClass.HEAVY)
                        && request.lanes.contains("remote")
                        && request.executorIds.contains("remote-executor")));
  }

  @Test
  void renewReconcileLeaseReturnsCancellationSignal() {
    when(service.jobs.renewLease("job-1", "lease-1")).thenReturn(true);
    when(service.jobs.isCancellationRequested("job-1")).thenReturn(true);

    var response =
        service
            .renewReconcileLease(
                RenewReconcileLeaseRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getRenewed());
    assertTrue(response.getCancellationRequested());
  }

  @Test
  void reportReconcileProgressActsAsHeartbeat() {
    when(service.jobs.renewLease("job-1", "lease-1")).thenReturn(true);

    var response =
        service
            .reportReconcileProgress(
                ReportReconcileProgressRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setTablesScanned(4)
                    .setTablesChanged(2)
                    .setErrors(1)
                    .setSnapshotsProcessed(3)
                    .setStatsProcessed(5)
                    .setMessage("working")
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getLeaseValid());
    verify(service.jobs).markProgress("job-1", "lease-1", 4, 2, 1, 3, 5, "working");
  }

  @Test
  void completeLeasedReconcileJobMarksSucceeded() {
    when(service.jobs.renewLease("job-1", "lease-1")).thenReturn(true);

    var response =
        service
            .completeLeasedReconcileJob(
                CompleteLeasedReconcileJobRequest.newBuilder()
                    .setJobId("job-1")
                    .setLeaseEpoch("lease-1")
                    .setState(ReconcileCompletionState.RCS_SUCCEEDED)
                    .setTablesScanned(7)
                    .setTablesChanged(3)
                    .setSnapshotsProcessed(2)
                    .setStatsProcessed(9)
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getAccepted());
    verify(service.jobs)
        .markSucceeded(eq("job-1"), eq("lease-1"), anyLong(), eq(7L), eq(3L), eq(2L), eq(9L));
  }

  @Test
  void getReconcileCancellationReadsQueueState() {
    when(service.jobs.isCancellationRequested("job-1")).thenReturn(true);

    var response =
        service
            .getReconcileCancellation(
                GetReconcileCancellationRequest.newBuilder().setJobId("job-1").build())
            .await()
            .indefinitely();

    assertTrue(response.getCancellationRequested());
  }
}
