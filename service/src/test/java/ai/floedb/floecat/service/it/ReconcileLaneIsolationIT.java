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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.ExecutionClass;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.LeaseReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.ReconcileExecutorControlGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.ReconcileLaneIsolationProfile;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ReconcileLaneIsolationProfile.class)
class ReconcileLaneIsolationIT {
  private static final String EXPECTED_LANE = "ci-run-a";
  private static final String OTHER_LANE = "ci-run-b";
  private static final String WORKER_AFFINITY = "reconciler-v1";

  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @GrpcClient("floecat")
  ReconcileExecutorControlGrpc.ReconcileExecutorControlBlockingStub executorControl;

  @Inject ReconcileJobStore jobs;
  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private String accountId;
  private String connectorId;

  @BeforeEach
  void setUp() {
    resetter.wipeAll();
    seeder.seedData();

    TestSupport.createCatalog(catalogs, "lane-isolation", "");
    var connector =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("lane-isolation")
                .setKind(ConnectorKind.CK_GLUE)
                .setUri("dummy://lane-isolation")
                .setSource(
                    SourceSelector.newBuilder()
                        .setNamespace(
                            NamespacePath.newBuilder().addSegments("lane-isolation").build())
                        .build())
                .setDestination(
                    DestinationTarget.newBuilder().setCatalogDisplayName("lane-isolation").build())
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .build());
    accountId = connector.getResourceId().getAccountId();
    connectorId = connector.getResourceId().getId();
  }

  @Test
  void fileGroupAndFinalizerJobsAreLeasedOnlyByTheirExecutionLane() {
    ReconcileExecutionPolicy policy =
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, EXPECTED_LANE, Map.of());
    String fileGroupJobId =
        jobs.enqueueFileGroupExecution(
            accountId,
            connectorId,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileFileGroupTask.of(
                "plan-1",
                "snapshot-55-group-0",
                "table-1",
                55L,
                List.of("s3://bucket/data.parquet")),
            policy,
            "",
            "");
    String finalizerJobId =
        jobs.enqueueSnapshotFinalization(
            accountId,
            connectorId,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 56L, "db", "events", List.of(), true),
            policy,
            "",
            "");

    assertFalse(lease(OTHER_LANE).getFound());

    Map<String, ai.floedb.floecat.reconciler.rpc.ReconcileJobKind> leasedJobs =
        new LinkedHashMap<>();
    for (int i = 0; i < 2; i++) {
      LeaseReconcileJobResponse response = lease(EXPECTED_LANE);
      assertTrue(response.getFound());
      leasedJobs.put(response.getJob().getJobId(), response.getJob().getKind());
    }

    assertEquals(
        Map.of(
            fileGroupJobId,
            ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_EXEC_FILE_GROUP,
            finalizerJobId,
            ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_FINALIZE_SNAPSHOT_CAPTURE),
        leasedJobs);
    assertFalse(lease(EXPECTED_LANE).getFound());
  }

  private LeaseReconcileJobResponse lease(String lane) {
    return executorControl.leaseReconcileJob(
        LeaseReconcileJobRequest.newBuilder()
            .setExecutorId("lane-isolation-worker")
            .setWorkerAffinity(WORKER_AFFINITY)
            .addExecutionClasses(ExecutionClass.EC_DEFAULT)
            .addLanes(lane)
            .addJobKinds(ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_EXEC_FILE_GROUP)
            .addJobKinds(
                ai.floedb.floecat.reconciler.rpc.ReconcileJobKind.RJK_FINALIZE_SNAPSHOT_CAPTURE)
            .build());
  }
}
