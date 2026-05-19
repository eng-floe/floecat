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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.StandaloneFileGroupExecutionPayload;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeasedFileGroupExecutionServiceTest {
  private static final String ACCOUNT_ID = "acct";
  private static final String CONNECTOR_ID = "conn";
  private static final String PARENT_JOB_ID = "parent-job";
  private static final String CHILD_JOB_ID = "child-job";
  private static final String LEASE_EPOCH = "lease-1";
  private static final String TABLE_ID = "table-1";
  private static final long SNAPSHOT_ID = 55L;

  private LeasedFileGroupExecutionService service;
  private ReconcileJobStore jobs;
  private TableRepository tableRepo;
  private ConnectorRepository connectorRepo;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedFileGroupExecutionService();
    jobs = mock(ReconcileJobStore.class);
    tableRepo = mock(TableRepository.class);
    connectorRepo = mock(ConnectorRepository.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.tableRepo = tableRepo;
    service.connectorRepo = connectorRepo;
    when(principal.getCorrelationId()).thenReturn("corr");
  }

  @Test
  void resolveUsesParentSnapshotTaskFileGroupsFromDurableJobView() {
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of("s3://bucket/data/file-1.parquet"));

    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.get(CHILD_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    CHILD_JOB_ID,
                    ReconcileJobKind.EXEC_FILE_GROUP,
                    ReconcileSnapshotTask.empty(),
                    group.asReference(),
                    PARENT_JOB_ID)));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    PARENT_JOB_ID,
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ReconcileSnapshotTask.of(
                        TABLE_ID,
                        SNAPSHOT_ID,
                        "db",
                        "events",
                        List.of(group),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(tableRepo.getById(tableId())).thenReturn(Optional.of(table()));
    when(connectorRepo.getById(connectorId())).thenReturn(Optional.of(connector()));

    StandaloneFileGroupExecutionPayload payload =
        service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH);

    assertEquals("plan-1", payload.planId());
    assertEquals("group-1", payload.groupId());
    assertEquals(List.of("s3://bucket/data/file-1.parquet"), payload.plannedFilePaths());
  }

  @Test
  void resolveFailsWhenParentSnapshotTaskDoesNotContainPlannedGroup() {
    ReconcileFileGroupTask childRef =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, 1, List.of(), List.of());

    when(jobs.renewLease(CHILD_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.get(CHILD_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    CHILD_JOB_ID,
                    ReconcileJobKind.EXEC_FILE_GROUP,
                    ReconcileSnapshotTask.empty(),
                    childRef,
                    PARENT_JOB_ID)));
    when(jobs.get(ACCOUNT_ID, PARENT_JOB_ID))
        .thenReturn(
            Optional.of(
                job(
                    PARENT_JOB_ID,
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ReconcileSnapshotTask.of(
                        TABLE_ID,
                        SNAPSHOT_ID,
                        "db",
                        "events",
                        List.of(),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        "/accounts/acct/reconcile/jobs/parent-job/snapshot-plan/blob.json",
                        1),
                    ReconcileFileGroupTask.empty(),
                    "")));

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.resolve(principal, CHILD_JOB_ID, LEASE_EPOCH));

    assertEquals(
        "FAILED_PRECONDITION: planned file group could not be resolved from parent snapshot plan",
        error.getMessage());
  }

  private static ReconcileJobStore.ReconcileJob job(
      String jobId,
      ReconcileJobKind kind,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      String parentJobId) {
    return new ReconcileJobStore.ReconcileJob(
        jobId,
        ACCOUNT_ID,
        CONNECTOR_ID,
        "JS_RUNNING",
        "Running",
        1L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        false,
        CaptureMode.METADATA_ONLY,
        0L,
        0L,
        0L,
        false,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "",
        "remote_file_group_worker",
        kind,
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        snapshotTask,
        fileGroupTask,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        parentJobId);
  }

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId(ACCOUNT_ID)
        .setKind(ResourceKind.RK_TABLE)
        .setId(TABLE_ID)
        .build();
  }

  private static ResourceId connectorId() {
    return ResourceId.newBuilder()
        .setAccountId(ACCOUNT_ID)
        .setKind(ResourceKind.RK_CONNECTOR)
        .setId(CONNECTOR_ID)
        .build();
  }

  private static Table table() {
    return Table.newBuilder()
        .setResourceId(tableId())
        .setUpstream(
            UpstreamRef.newBuilder()
                .setConnectorId(connectorId())
                .setTableDisplayName("events")
                .addNamespacePath("db")
                .build())
        .build();
  }

  private static Connector connector() {
    return Connector.newBuilder()
        .setResourceId(connectorId())
        .setKind(ConnectorKind.CK_ICEBERG)
        .setAuth(AuthConfig.getDefaultInstance())
        .build();
  }
}
